package com.job_service;

import com.job_service.config.JobServiceConfig;
import com.job_service.handler.JobHandler;
import com.job_service.model.*;
import com.job_service.queue.JobReference;
import com.job_service.queue.WeightedPriorityQueue;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * In-process Background Job Service.
 *
 * Guarantees:
 * - At-least-once execution (jobs may be executed multiple times if worker crashes)
 * - Lease-based visibility timeout (claimed jobs expire and return to pending if not renewed)
 * - Lease token validation for completion (Option A: requires matching token)
 * - Per-type lease duration configuration (Option B)
 * - job_id serves as idempotency key for deduplication
 */
public class JobService {

    // Configuration
    private final JobServiceConfig config;

    // Job storage: jobId -> JobRecord (canonical state)
    private final ConcurrentHashMap<String, JobRecord> jobsById = new ConcurrentHashMap<>();

    // Queues by job type, each with weighted priority
    private final Map<String, WeightedPriorityQueue> queuesByType = new ConcurrentHashMap<>();

    // Dead Letter Queue: type -> list of failed jobs
    private final Map<String, List<JobRecord>> dlqByType = new ConcurrentHashMap<>();

    // Handler registry: type -> handler
    private final ConcurrentHashMap<String, JobHandler> handlers = new ConcurrentHashMap<>();

    // Job ID counter for generating unique IDs
    private long jobIdCounter = 0;
    private final ReadWriteLock jobIdLock = new ReentrantReadWriteLock();

    // Track executed jobs for idempotency (job_id -> result)
    private final ConcurrentHashMap<String, JobResult> executedJobs = new ConcurrentHashMap<>();

    public JobService(JobServiceConfig config) {
        this.config = config != null ? config : new JobServiceConfig();
    }

    public JobService() {
        this(new JobServiceConfig());
    }

    /**
     * Submit a job to the service.
     *
     * @return The unique job ID
     */
    public String submit(JobSpec spec) {
        String jobId = generateJobId();
        JobRecord record = new JobRecord(jobId, spec);

        // Store the job record
        jobsById.put(jobId, record);

        // Add to pending queue for its type
        queuesByType.computeIfAbsent(spec.type, k -> new WeightedPriorityQueue(JobServiceConfig.STARVATION_WINDOW))
                .enqueue(new JobReference(jobId, spec.priority, Instant.now()));

        return jobId;
    }

    /**
     * Register a handler for a specific job type.
     */
    public void registerHandler(String jobType, JobHandler handler) {
        handlers.put(jobType, handler);
    }

    /**
     * Poll for a job of the specified types.
     * Atomically claims the job with a lease token.
     *
     * @param workerId Worker ID claiming the job
     * @param jobTypes Set of job types this worker can handle
     * @return Claimed job if available, empty otherwise
     */
    public Optional<ClaimedJob> pollForJob(String workerId, Set<String> jobTypes) {
        // Try to find a pending job in the requested types
        for (String jobType : jobTypes) {
            WeightedPriorityQueue queue = queuesByType.get(jobType);
            if (queue == null) continue;

            Optional<JobReference> jobRefOpt = queue.dequeue();
            if (jobRefOpt.isEmpty()) continue;

            JobReference jobRef = jobRefOpt.get();
            JobRecord record = jobsById.get(jobRef.jobId);

            if (record == null) continue;

            // Try to claim the job atomically
            synchronized (record) {
                if (record.status != JobStatus.PENDING) {
                    // Status changed, job was already claimed - skip
                    continue;
                }

                // Claim the job with lease
                String leaseToken = UUID.randomUUID().toString();
                long leaseDuration = config.getLeaseDurationSeconds(jobType);
                // Check if job spec has override
                if (record.spec.leaseDurationSeconds != null) {
                    leaseDuration = record.spec.leaseDurationSeconds;
                }
                Instant leaseExpiresAt = Instant.now().plusSeconds(leaseDuration);

                record.claimJob(workerId, leaseToken, leaseExpiresAt);

                return Optional.of(new ClaimedJob(
                        record.jobId,
                        record.spec.type,
                        record.spec.payload,
                        leaseToken,
                        leaseExpiresAt
                ));
            }
        }

        return Optional.empty();
    }

    /**
     * Renew the lease for a job being processed.
     *
     * @param jobId Job ID
     * @param leaseToken Current lease token
     * @param additionalDurationSeconds Additional time to grant
     * @return true if renewed, false if token mismatch or expired
     */
    public boolean renewLease(String jobId, String leaseToken, long additionalDurationSeconds) {
        JobRecord record = jobsById.get(jobId);
        if (record == null) return false;

        synchronized (record) {
            // Verify lease token matches (Option A: require matching token)
            if (!leaseToken.equals(record.currentLeaseToken)) {
                return false;
            }

            // Check if lease already expired
            if (record.leaseExpiresAt != null && Instant.now().isAfter(record.leaseExpiresAt)) {
                return false;
            }

            // Extend the lease
            record.leaseExpiresAt = Instant.now().plusSeconds(additionalDurationSeconds);
            record.lastModifiedAt = Instant.now();
            return true;
        }
    }

    /**
     * Complete a job (success or failure).
     *
     * @param jobId Job ID
     * @param leaseToken Lease token (must match for Option A semantics)
     * @param result Result of job execution
     * @return true if completed successfully, false if token mismatch or lease expired
     */
    public boolean completeJob(String jobId, String leaseToken, JobResult result) {
        JobRecord record = jobsById.get(jobId);
        if (record == null) return false;

        return handleJobCompletion(record, leaseToken, result);
    }

    /**
     * Internal method to handle job completion with proper synchronization.
     */
    private boolean handleJobCompletion(JobRecord record, String leaseToken, JobResult result) {
        synchronized (record) {
            // Option A: require lease token match for correctness
            if (!leaseToken.equals(record.currentLeaseToken)) {
                return false;
            }

            // Check if lease expired
            if (record.leaseExpiresAt != null && Instant.now().isAfter(record.leaseExpiresAt)) {
                return false;
            }

            record.attempts++;

            if (result instanceof JobResult.Success) {
                // Job succeeded
                record.status = JobStatus.SUCCEEDED;
                record.releaseLease();
                record.lastModifiedAt = Instant.now();

                // Track as executed for idempotency (job_id as key)
                executedJobs.put(record.jobId, result);
                return true;

            } else if (result instanceof JobResult.TransientFailure) {
                // Transient failure - retry if retries remain
                JobResult.TransientFailure tf = (JobResult.TransientFailure) result;
                record.lastError = tf.error;

                if (record.attempts < record.spec.maxRetries) {
                    // Schedule for retry
                    record.status = JobStatus.RETRY_SCHEDULED;
                    record.releaseLease();
                    record.lastModifiedAt = Instant.now();

                    // Re-enqueue to pending (retry logic will be added in Phase 3)
                    queuesByType.computeIfAbsent(record.spec.type,
                            k -> new WeightedPriorityQueue(JobServiceConfig.STARVATION_WINDOW))
                            .enqueue(new JobReference(record.jobId, record.spec.priority, Instant.now()));

                    record.status = JobStatus.PENDING;
                    return true;
                } else {
                    // Max retries exhausted - move to DLQ
                    moveToDLQ(record);
                    return true;
                }

            } else if (result instanceof JobResult.PermanentFailure) {
                // Permanent failure - skip retries and go to DLQ
                JobResult.PermanentFailure pf = (JobResult.PermanentFailure) result;
                record.lastError = pf.error;
                moveToDLQ(record);
                return true;
            }

            return false;
        }
    }

    /**
     * Move a job to the Dead Letter Queue.
     */
    private void moveToDLQ(JobRecord record) {
        record.status = JobStatus.FAILED;
        record.releaseLease();
        record.lastModifiedAt = Instant.now();

        dlqByType.computeIfAbsent(record.spec.type, k -> Collections.synchronizedList(new ArrayList<>()))
                .add(record);
    }

    /**
     * Get pending job count by type.
     */
    public Map<String, Integer> getPendingCountByType() {
        Map<String, Integer> counts = new HashMap<>();
        for (Map.Entry<String, WeightedPriorityQueue> entry : queuesByType.entrySet()) {
            int size = entry.getValue().totalSize();
            if (size > 0) {
                counts.put(entry.getKey(), size);
            }
        }
        return counts;
    }

    /**
     * Get pending count for a specific type and priority (0 if not found).
     */
    public int getPendingCount(String jobType, int priority) {
        WeightedPriorityQueue queue = queuesByType.get(jobType);
        if (queue == null) return 0;
        return queue.sizeAtPriority(priority);
    }

    /**
     * Get total pending count across all types.
     */
    public int getTotalPendingCount() {
        return queuesByType.values().stream()
                .mapToInt(WeightedPriorityQueue::totalSize)
                .sum();
    }

    /**
     * Get in-flight (claimed) job count.
     */
    public int getInFlightCount() {
        return (int) jobsById.values().stream()
                .filter(r -> r.status == JobStatus.CLAIMED)
                .count();
    }

    /**
     * Get DLQ size for a type.
     */
    public int getDLQSize(String jobType) {
        List<JobRecord> dlq = dlqByType.get(jobType);
        return dlq == null ? 0 : dlq.size();
    }

    /**
     * List DLQ entries for a type.
     */
    public List<JobRecord> listDLQ(String jobType, int limit, int offset) {
        List<JobRecord> dlq = dlqByType.get(jobType);
        if (dlq == null || dlq.isEmpty()) return Collections.emptyList();

        int end = Math.min(offset + limit, dlq.size());
        if (offset >= dlq.size()) return Collections.emptyList();

        return new ArrayList<>(dlq.subList(offset, end));
    }

    /**
     * Scan for expired leases and return jobs to pending.
     * Should be called periodically or before polling.
     */
    public void scanExpiredLeases() {
        for (JobRecord record : jobsById.values()) {
            synchronized (record) {
                if (record.status == JobStatus.CLAIMED && record.isLeaseExpired()) {
                    // Lease expired - return to pending
                    record.status = JobStatus.PENDING;
                    record.releaseLease();

                    // Re-enqueue to pending
                    queuesByType.computeIfAbsent(record.spec.type,
                            k -> new WeightedPriorityQueue(JobServiceConfig.STARVATION_WINDOW))
                            .enqueue(new JobReference(record.jobId, record.spec.priority, Instant.now()));
                }
            }
        }
    }

    /**
     * Get a job record (for testing).
     */
    protected JobRecord getJobRecord(String jobId) {
        return jobsById.get(jobId);
    }

    /**
     * Clear all state (for testing).
     */
    public void clear() {
        jobsById.clear();
        queuesByType.clear();
        dlqByType.clear();
        handlers.clear();
        executedJobs.clear();
    }

    /**
     * Generate a unique job ID.
     */
    private String generateJobId() {
        jobIdLock.writeLock().lock();
        try {
            return "job-" + (++jobIdCounter) + "-" + UUID.randomUUID();
        } finally {
            jobIdLock.writeLock().unlock();
        }
    }
}

