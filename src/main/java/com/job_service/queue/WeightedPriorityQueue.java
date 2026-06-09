package com.job_service.queue;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Weighted priority queue implementation (Option B with Round-robin).
 *
 * This maintains separate queues for each priority level (0-9).
 * Uses round-robin weighted fairness to prevent starvation of low-priority jobs:
 * - Always try to dequeue from highest priority first
 * - After STARVATION_WINDOW consecutive high-priority selections, rotate to allow lower-priority jobs
 *
 * Thread-safe using ReadWriteLock.
 */
public class WeightedPriorityQueue {
    // Separate queue per priority level (0-9)
    private final Map<Integer, PriorityQueue<JobReference>> queuesByPriority;

    // Round-robin counter for starvation prevention
    private int consecutiveHighPrioritySelections;
    private final int starvationWindow;

    // Last priority used (for tracking fairness)
    private int lastUsedPriority;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public WeightedPriorityQueue(int starvationWindow) {
        this.queuesByPriority = new HashMap<>();
        for (int i = 0; i < 10; i++) {
            queuesByPriority.put(i, new PriorityQueue<>());
        }
        this.starvationWindow = starvationWindow;
        this.consecutiveHighPrioritySelections = 0;
        this.lastUsedPriority = 9;  // Start from highest
    }

    /**
     * Add a job reference to the appropriate priority queue.
     */
    public void enqueue(JobReference jobRef) {
        lock.writeLock().lock();
        try {
            queuesByPriority.get(jobRef.priority).offer(jobRef);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Dequeue a job with round-robin weighted fairness.
     *
     * Returns the next job respecting priority with starvation prevention:
     * - If we've had STARVATION_WINDOW consecutive high-priority dequeues, rotate to lower priorities
     * - Otherwise, always pick from the highest non-empty queue
     */
    public Optional<JobReference> dequeue() {
        lock.writeLock().lock();
        try {
            // If we've been picking high priority too much, allow lower priorities a chance
            if (consecutiveHighPrioritySelections >= starvationWindow && lastUsedPriority > 0) {
                // Rotate to next lower priority
                lastUsedPriority--;
                consecutiveHighPrioritySelections = 0;
            }

            // Try to find a job from lastUsedPriority downwards
            for (int p = Math.min(9, lastUsedPriority); p >= 0; p--) {
                JobReference job = queuesByPriority.get(p).poll();
                if (job != null) {
                    lastUsedPriority = p;
                    if (p == 9) {
                        consecutiveHighPrioritySelections++;
                    } else {
                        consecutiveHighPrioritySelections = 0;
                    }
                    return Optional.of(job);
                }
            }

            // If nothing found at current level, reset and try from top
            lastUsedPriority = 9;
            consecutiveHighPrioritySelections = 0;

            // Find highest non-empty queue
            for (int p = 9; p >= 0; p--) {
                JobReference job = queuesByPriority.get(p).poll();
                if (job != null) {
                    lastUsedPriority = p;
                    if (p == 9) {
                        consecutiveHighPrioritySelections++;
                    } else {
                        consecutiveHighPrioritySelections = 0;
                    }
                    return Optional.of(job);
                }
            }

            return Optional.empty();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Get count of pending jobs at a specific priority level.
     */
    public int sizeAtPriority(int priority) {
        lock.readLock().lock();
        try {
            return queuesByPriority.get(priority).size();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Get total pending job count across all priorities.
     */
    public int totalSize() {
        lock.readLock().lock();
        try {
            return queuesByPriority.values().stream()
                    .mapToInt(java.util.Collection::size)
                    .sum();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Check if queue is empty.
     */
    public boolean isEmpty() {
        lock.readLock().lock();
        try {
            return totalSize() == 0;
        } finally {
            lock.readLock().unlock();
        }
    }
}

