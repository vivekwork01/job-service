package com.job_service.queue;

import java.time.Instant;

/**
 * Small reference object to identify a pending job in the queue.
 * Held in priority queues to avoid storing full JobRecord in the queue.
 */
public class JobReference implements Comparable<JobReference> {
    public final String jobId;
    public final int priority;
    public final Instant enqueuedAt;  // For FIFO ordering within same priority

    public JobReference(String jobId, int priority, Instant enqueuedAt) {
        this.jobId = jobId;
        this.priority = priority;
        this.enqueuedAt = enqueuedAt;
    }

    /**
     * Compare for sorting: higher priority first, then FIFO (earlier enqueued first).
     * This gives us FIFO ordering within same priority.
     */
    @Override
    public int compareTo(JobReference other) {
        // Higher priority first
        if (this.priority != other.priority) {
            return Integer.compare(other.priority, this.priority);
        }
        // FIFO: earlier enqueued comes first
        return this.enqueuedAt.compareTo(other.enqueuedAt);
    }

    @Override
    public String toString() {
        return "JobReference{" +
                "jobId='" + jobId + '\'' +
                ", priority=" + priority +
                '}';
    }
}

