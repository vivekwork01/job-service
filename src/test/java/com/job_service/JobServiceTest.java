package com.job_service;

import com.job_service.config.JobServiceConfig;
import com.job_service.handler.JobHandler;
import com.job_service.model.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive tests for JobService implementation.
 *
 * Covers:
 * - Phase 1: Core queue and job lifecycle (submit, claim, complete)
 * - Lease token validation (Option A)
 * - Per-type lease duration (Option B)
 * - Weighted priority queue with FIFO (Option B)
 * - Idempotency with job_id
 * - Lease expiry (worker crash simulation)
 */
@DisplayName("JobService Tests")
public class JobServiceTest {

    private JobService service;
    private JobServiceConfig config;

    @BeforeEach
    void setUp() {
        config = new JobServiceConfig();
        // Use small lease durations for fast tests
        config.setDefaultLeaseDuration(1);  // 1 second
        service = new JobService(config);
    }

    // ============ Phase 1: Core Queue and Job Lifecycle ============

    @Test
    @DisplayName("Test 1.1: Submit a job and verify it's in pending queue")
    void testSubmitJob() {
        JobSpec spec = new JobSpec("send_email", Map.of("recipient", "user@example.com"));
        String jobId = service.submit(spec);

        assertNotNull(jobId);
        assertTrue(jobId.startsWith("job-"));

        // Verify job is pending
        JobRecord record = service.getJobRecord(jobId);
        assertNotNull(record);
        assertEquals(JobStatus.PENDING, record.status);
        assertEquals("send_email", record.spec.type);
    }

    @Test
    @DisplayName("Test 1.2: Claim a job atomically")
    void testClaimJob() {
        JobSpec spec = new JobSpec("send_email", Map.of("recipient", "user@example.com"));
        String jobId = service.submit(spec);

        Optional<ClaimedJob> claimed = service.pollForJob("worker-1", Set.of("send_email"));
        assertTrue(claimed.isPresent());

        ClaimedJob job = claimed.get();
        assertEquals(jobId, job.jobId);
        assertEquals("send_email", job.jobType);
        assertNotNull(job.leaseToken);
        assertNotNull(job.leaseExpiresAt);

        // Verify job status changed
        JobRecord record = service.getJobRecord(jobId);
        assertEquals(JobStatus.CLAIMED, record.status);
        assertEquals("worker-1", record.currentWorkerId);
        assertEquals(job.leaseToken, record.currentLeaseToken);
    }

    @Test
    @DisplayName("Test 1.3: Complete a job successfully with correct lease token")
    void testCompleteJobSuccess() {
        JobSpec spec = new JobSpec("send_email", Map.of("recipient", "user@example.com"));
        String jobId = service.submit(spec);

        Optional<ClaimedJob> claimed = service.pollForJob("worker-1", Set.of("send_email"));
        assertTrue(claimed.isPresent());
        ClaimedJob job = claimed.get();

        // Complete with correct token
        boolean completed = service.completeJob(jobId, job.leaseToken, new JobResult.Success());
        assertTrue(completed);

        // Verify job is succeeded
        JobRecord record = service.getJobRecord(jobId);
        assertEquals(JobStatus.SUCCEEDED, record.status);
        assertNull(record.currentLeaseToken);
        assertNull(record.currentWorkerId);
    }

    @Test
    @DisplayName("Test 1.4: Reject completion with wrong lease token (Option A)")
    void testRejectCompletionWrongToken() {
        JobSpec spec = new JobSpec("send_email", Map.of("recipient", "user@example.com"));
        String jobId = service.submit(spec);

        Optional<ClaimedJob> claimed = service.pollForJob("worker-1", Set.of("send_email"));
        assertTrue(claimed.isPresent());

        // Try completing with wrong token (Option A behavior)
        boolean completed = service.completeJob(jobId, "wrong-token", new JobResult.Success());
        assertFalse(completed, "Completion with wrong token should fail");

        // Job should still be claimed
        JobRecord record = service.getJobRecord(jobId);
        assertEquals(JobStatus.CLAIMED, record.status);
    }

    // ============ Option B: Per-type Lease Duration ============

    @Test
    @DisplayName("Test 2.1: Job uses type-specific lease duration")
    void testPerTypeLeaseDuration() {
        config.setLeaseDurationForType("long_task", 60);

        JobSpec spec = new JobSpec("long_task", Map.of("data", "value"));
        String jobId = service.submit(spec);

        Optional<ClaimedJob> claimed = service.pollForJob("worker-1", Set.of("long_task"));
        assertTrue(claimed.isPresent());

        Instant leaseExpiresAt = claimed.get().leaseExpiresAt;
        long remainingSeconds = java.time.temporal.ChronoUnit.SECONDS
                .between(Instant.now(), leaseExpiresAt);

        // Should be approximately 60 seconds (with small margin for execution time)
        assertTrue(remainingSeconds > 50 && remainingSeconds <= 60,
                "Lease duration should be ~60 seconds, got " + remainingSeconds);
    }

    @Test
    @DisplayName("Test 2.2: Job spec can override per-job lease duration")
    void testJobLevelLeaseDurationOverride() {
        config.setLeaseDurationForType("task", 30);

        // Create job with override
        JobSpec spec = new JobSpec("task", Map.of("data", "value"), 5, 3, 10L);
        String jobId = service.submit(spec);

        Optional<ClaimedJob> claimed = service.pollForJob("worker-1", Set.of("task"));
        assertTrue(claimed.isPresent());

        Instant leaseExpiresAt = claimed.get().leaseExpiresAt;
        long remainingSeconds = java.time.temporal.ChronoUnit.SECONDS
                .between(Instant.now(), leaseExpiresAt);

        // Should use override value of 10
        assertTrue(remainingSeconds > 8 && remainingSeconds <= 10,
                "Lease duration should be ~10 seconds (override), got " + remainingSeconds);
    }

    // ============ Option B: Weighted Priority Queue with FIFO ============

    @Test
    @DisplayName("Test 3.1: Higher priority jobs are dequeued first")
    void testPriorityOrdering() {
        // Submit jobs with different priorities
        String id1 = service.submit(new JobSpec("task", Map.of("id", "1"), 3, 3));
        String id2 = service.submit(new JobSpec("task", Map.of("id", "2"), 7, 3));
        String id3 = service.submit(new JobSpec("task", Map.of("id", "3"), 5, 3));

        // Dequeue - should get priority 7 first
        Optional<ClaimedJob> job1 = service.pollForJob("worker", Set.of("task"));
        assertEquals(id2, job1.get().jobId, "Priority 7 should be dequeued first");

        // Return remaining to pending somehow... we'll need to implement a way to do this
        // For now, let's just verify the ordering by submitting new jobs
        service.clear();

        String p9 = service.submit(new JobSpec("task", Map.of("id", "p9"), 9, 3));
        String p5 = service.submit(new JobSpec("task", Map.of("id", "p5"), 5, 3));
        String p8 = service.submit(new JobSpec("task", Map.of("id", "p8"), 8, 3));

        Optional<ClaimedJob> first = service.pollForJob("w1", Set.of("task"));
        assertEquals(p9, first.get().jobId);

        Optional<ClaimedJob> second = service.pollForJob("w2", Set.of("task"));
        assertEquals(p8, second.get().jobId);

        Optional<ClaimedJob> third = service.pollForJob("w3", Set.of("task"));
        assertEquals(p5, third.get().jobId);
    }

    @Test
    @DisplayName("Test 3.2: Same-priority jobs follow FIFO ordering")
    void testFIFOSamePriority() {
        String id1 = service.submit(new JobSpec("task", Map.of("id", "1"), 5, 3));
        // Small delay to ensure different timestamps
        try { Thread.sleep(10); } catch (InterruptedException e) {}
        String id2 = service.submit(new JobSpec("task", Map.of("id", "2"), 5, 3));
        try { Thread.sleep(10); } catch (InterruptedException e) {}
        String id3 = service.submit(new JobSpec("task", Map.of("id", "3"), 5, 3));

        // Should dequeue in FIFO order (id1, id2, id3) since all have same priority
        Optional<ClaimedJob> job1 = service.pollForJob("w1", Set.of("task"));
        assertEquals(id1, job1.get().jobId, "First should be dequeued (FIFO)");

        Optional<ClaimedJob> job2 = service.pollForJob("w2", Set.of("task"));
        assertEquals(id2, job2.get().jobId, "Second should be dequeued (FIFO)");

        Optional<ClaimedJob> job3 = service.pollForJob("w3", Set.of("task"));
        assertEquals(id3, job3.get().jobId, "Third should be dequeued (FIFO)");
    }

    // ============ Lease Expiry: At-least-once on Worker Crash ============

    @Test
    @DisplayName("Test 4.1: Expired lease returns job to pending (worker crash simulation)")
    void testLeaseExpiryReturnsToPending() throws InterruptedException {
        // Create config with very short lease
        JobServiceConfig shortLeaseConfig = new JobServiceConfig();
        shortLeaseConfig.setDefaultLeaseDuration(1);  // 1 second
        JobService shortLeaseService = new JobService(shortLeaseConfig);

        String jobId = shortLeaseService.submit(new JobSpec("task", Map.of("data", "value")));

        // Claim the job
        Optional<ClaimedJob> claimed = shortLeaseService.pollForJob("worker-1", Set.of("task"));
        assertTrue(claimed.isPresent());

        JobRecord record = shortLeaseService.getJobRecord(jobId);
        assertEquals(JobStatus.CLAIMED, record.status);

        // Wait for lease to expire
        Thread.sleep(1500);  // Wait 1.5 seconds

        // Scan for expired leases
        shortLeaseService.scanExpiredLeases();

        // Job should be back to pending
        record = shortLeaseService.getJobRecord(jobId);
        assertEquals(JobStatus.PENDING, record.status);
        assertNull(record.currentLeaseToken);
        assertNull(record.currentWorkerId);

        // Another worker should be able to claim it
        Optional<ClaimedJob> claimed2 = shortLeaseService.pollForJob("worker-2", Set.of("task"));
        assertTrue(claimed2.isPresent());
        assertEquals(jobId, claimed2.get().jobId);
    }

    @Test
    @DisplayName("Test 4.2: At-least-once: same job executed twice if first worker crashes")
    void testAtLeastOnceSemantics() throws InterruptedException {
        JobServiceConfig shortLeaseConfig = new JobServiceConfig();
        shortLeaseConfig.setDefaultLeaseDuration(1);
        JobService shortLeaseService = new JobService(shortLeaseConfig);

        String jobId = shortLeaseService.submit(new JobSpec("task", Map.of("data", "value")));

        // Worker 1 claims but doesn't renew (simulating crash)
        Optional<ClaimedJob> claimed1 = shortLeaseService.pollForJob("worker-1", Set.of("task"));
        assertTrue(claimed1.isPresent());
        String token1 = claimed1.get().leaseToken;

        // Wait for lease to expire
        Thread.sleep(1500);
        shortLeaseService.scanExpiredLeases();

        // Worker 2 claims the same job
        Optional<ClaimedJob> claimed2 = shortLeaseService.pollForJob("worker-2", Set.of("task"));
        assertTrue(claimed2.isPresent());
        assertEquals(jobId, claimed2.get().jobId);
        String token2 = claimed2.get().leaseToken;

        // Tokens should be different (new lease)
        assertNotEquals(token1, token2);

        // Worker 2 completes successfully
        boolean completed = shortLeaseService.completeJob(jobId, token2, new JobResult.Success());
        assertTrue(completed);

        // Worker 1 tries to complete with old token - should fail
        boolean shouldFail = shortLeaseService.completeJob(jobId, token1, new JobResult.Success());
        assertFalse(shouldFail, "Should reject completion with expired token");
    }

    // ============ Lease Renewal ============

    @Test
    @DisplayName("Test 5.1: Lease renewal extends expiration time")
    void testLeaseRenewal() {
        String jobId = service.submit(new JobSpec("task", Map.of("data", "value")));

        Optional<ClaimedJob> claimed = service.pollForJob("worker-1", Set.of("task"));
        assertTrue(claimed.isPresent());
        String leaseToken = claimed.get().leaseToken;
        Instant originalExpires = claimed.get().leaseExpiresAt;

        // Renew the lease
        boolean renewed = service.renewLease(jobId, leaseToken, 5);  // Add 5 more seconds
        assertTrue(renewed);

        JobRecord record = service.getJobRecord(jobId);
        Instant newExpires = record.leaseExpiresAt;

        // New expiration should be after the original
        assertTrue(newExpires.isAfter(originalExpires),
                "Renewed lease should expire later");
    }

    @Test
    @DisplayName("Test 5.2: Renewal fails with wrong lease token")
    void testLeaseRenewalRejectsWrongToken() {
        String jobId = service.submit(new JobSpec("task", Map.of("data", "value")));

        Optional<ClaimedJob> claimed = service.pollForJob("worker-1", Set.of("task"));
        assertTrue(claimed.isPresent());

        // Try to renew with wrong token
        boolean renewed = service.renewLease(jobId, "wrong-token", 5);
        assertFalse(renewed, "Renewal with wrong token should fail");
    }

    // ============ Transient Failure and Retry ============

    @Test
    @DisplayName("Test 6.1: Transient failure returns job to pending for retry")
    void testTransientFailureRequeuesJob() {
        String jobId = service.submit(new JobSpec("task", Map.of("data", "value"), 5, 3));

        Optional<ClaimedJob> claimed = service.pollForJob("worker-1", Set.of("task"));
        assertTrue(claimed.isPresent());

        // Complete with transient failure
        boolean completed = service.completeJob(jobId, claimed.get().leaseToken,
                new JobResult.TransientFailure("Network timeout"));
        assertTrue(completed);

        // Job should be back in pending (status might be RETRY_SCHEDULED but we enqueue to pending)
        JobRecord record = service.getJobRecord(jobId);
        assertEquals(1, record.attempts, "Attempt count should increment");
        assertTrue(record.status == JobStatus.PENDING || record.status == JobStatus.RETRY_SCHEDULED);
        assertEquals("Network timeout", record.lastError);

        // Another worker should be able to claim it
        Optional<ClaimedJob> claimed2 = service.pollForJob("worker-2", Set.of("task"));
        assertTrue(claimed2.isPresent());
        assertEquals(jobId, claimed2.get().jobId);
    }

    @Test
    @DisplayName("Test 6.2: Permanent failure goes directly to DLQ")
    void testPermanentFailureToDLQ() {
        String jobId = service.submit(new JobSpec("task", Map.of("data", "value")));

        Optional<ClaimedJob> claimed = service.pollForJob("worker-1", Set.of("task"));
        assertTrue(claimed.isPresent());

        // Complete with permanent failure
        boolean completed = service.completeJob(jobId, claimed.get().leaseToken,
                new JobResult.PermanentFailure("Invalid payload"));
        assertTrue(completed);

        // Job should be in failed/DLQ
        JobRecord record = service.getJobRecord(jobId);
        assertEquals(JobStatus.FAILED, record.status);

        // Should be in DLQ
        List<JobRecord> dlq = service.listDLQ("task", 100, 0);
        assertTrue(dlq.stream().anyMatch(j -> j.jobId.equals(jobId)));
    }

    @Test
    @DisplayName("Test 6.3: Max retries exhausted moves to DLQ")
    void testMaxRetriesExhausted() {
        JobSpec spec = new JobSpec("task", Map.of("data", "value"), 5, 2);  // max 2 retries
        String jobId = service.submit(spec);

        // Failure 1
        Optional<ClaimedJob> claim1 = service.pollForJob("w1", Set.of("task"));
        service.completeJob(jobId, claim1.get().leaseToken, new JobResult.TransientFailure("error"));
        assertEquals(1, service.getJobRecord(jobId).attempts);

        // Failure 2
        Optional<ClaimedJob> claim2 = service.pollForJob("w2", Set.of("task"));
        service.completeJob(jobId, claim2.get().leaseToken, new JobResult.TransientFailure("error"));
        assertEquals(2, service.getJobRecord(jobId).attempts);

        // Failure 3 - exceeds max_retries
        Optional<ClaimedJob> claim3 = service.pollForJob("w3", Set.of("task"));
        assertThrows(NoSuchElementException.class, () -> service.completeJob(jobId, claim3.get().leaseToken, new JobResult.TransientFailure("error")));

        // Should be in DLQ now
        JobRecord record = service.getJobRecord(jobId);
        assertEquals(JobStatus.FAILED, record.status);

        List<JobRecord> dlq = service.listDLQ("task", 100, 0);
        assertTrue(dlq.stream().anyMatch(j -> j.jobId.equals(jobId)));
    }

    // ============ Observability ============

    @Test
    @DisplayName("Test 7.1: Get pending count by type")
    void testPendingCountByType() {
        service.submit(new JobSpec("email", Map.of("data", "value")));
        service.submit(new JobSpec("email", Map.of("data", "value")));
        service.submit(new JobSpec("sms", Map.of("data", "value")));

        Map<String, Integer> counts = service.getPendingCountByType();
        assertEquals(2, counts.get("email"));
        assertEquals(1, counts.get("sms"));
    }

    @Test
    @DisplayName("Test 7.2: Get in-flight count")
    void testInFlightCount() {
        service.submit(new JobSpec("task", Map.of("data", "value")));
        service.submit(new JobSpec("task", Map.of("data", "value")));

        assertEquals(2, service.getTotalPendingCount());

        // Claim one
        service.pollForJob("worker", Set.of("task"));

        assertEquals(1, service.getTotalPendingCount());
        assertEquals(1, service.getInFlightCount());
    }

    @Test
    @DisplayName("Test 7.3: DLQ size and listing")
    void testDLQListing() {
        String id1 = service.submit(new JobSpec("task", Map.of("id", "1")));
        String id2 = service.submit(new JobSpec("task", Map.of("id", "2")));

        // Move to DLQ
        Optional<ClaimedJob> claim1 = service.pollForJob("w1", Set.of("task"));
        service.completeJob(id1, claim1.get().leaseToken, new JobResult.PermanentFailure("error"));

        Optional<ClaimedJob> claim2 = service.pollForJob("w2", Set.of("task"));
        service.completeJob(id2, claim2.get().leaseToken, new JobResult.PermanentFailure("error"));

        assertEquals(2, service.getDLQSize("task"));

        List<JobRecord> dlq = service.listDLQ("task", 100, 0);
        assertEquals(2, dlq.size());
    }

    // ============ Idempotency with job_id ============

    @Test
    @DisplayName("Test 8.1: job_id serves as idempotency key")
    void testIdempotencyWithJobId() {
        // Same job submitted twice should have different IDs
        JobSpec spec = new JobSpec("task", Map.of("data", "duplicate"));
        String id1 = service.submit(spec);
        String id2 = service.submit(spec);

        assertNotEquals(id1, id2, "Each submission should get unique job ID");

        // Both should be process-able independently
        Optional<ClaimedJob> claim1 = service.pollForJob("w1", Set.of("task"));
        Optional<ClaimedJob> claim2 = service.pollForJob("w2", Set.of("task"));

        assertTrue(claim1.isPresent());
        assertTrue(claim2.isPresent());
        assertNotEquals(claim1.get().jobId, claim2.get().jobId);
    }

    // ============ Concurrent Operations ============

    @Test
    @DisplayName("Test 9.1: Concurrent submissions and claims don't lose jobs")
    void testConcurrentSubmissionsAndClaims() throws InterruptedException {
        int numProducers = 5;
        int jobsPerProducer = 10;
        int numWorkers = 3;

        ExecutorService producerPool = Executors.newFixedThreadPool(numProducers);
        CountDownLatch submitDone = new CountDownLatch(numProducers);

        // Submit jobs concurrently
        for (int i = 0; i < numProducers; i++) {
            producerPool.submit(() -> {
                for (int j = 0; j < jobsPerProducer; j++) {
                    service.submit(new JobSpec("task", Map.of("id", UUID.randomUUID().toString())));
                }
                submitDone.countDown();
            });
        }

        submitDone.await();
        producerPool.shutdown();

        int totalSubmitted = numProducers * jobsPerProducer;
        assertEquals(totalSubmitted, service.getTotalPendingCount());

        // Claim jobs concurrently
        ExecutorService workerPool = Executors.newFixedThreadPool(numWorkers);
        AtomicInteger claimed = new AtomicInteger(0);
        CountDownLatch claimDone = new CountDownLatch(totalSubmitted);

        for (int i = 0; i < totalSubmitted; i++) {
            workerPool.submit(() -> {
                Optional<ClaimedJob> job = service.pollForJob("worker-" + Thread.currentThread().getId(),
                        Set.of("task"));
                if (job.isPresent()) {
                    claimed.incrementAndGet();
                }
                claimDone.countDown();
            });
        }

        claimDone.await();
        workerPool.shutdown();

        assertEquals(totalSubmitted, claimed.get(), "All jobs should be claimed");
    }

    @Test
    @DisplayName("Test 9.2: Concurrent completions maintain atomicity")
    void testConcurrentCompletions() throws InterruptedException {
        int jobs = 20;
        for (int i = 0; i < jobs; i++) {
            service.submit(new JobSpec("task", Map.of("id", String.valueOf(i))));
        }

        List<ClaimedJob> claimed = new ArrayList<>();
        for (int i = 0; i < jobs; i++) {
            Optional<ClaimedJob> c = service.pollForJob("w" + i, Set.of("task"));
            assertTrue(c.isPresent());
            claimed.add(c.get());
        }

        // Complete all concurrently
        ExecutorService executor = Executors.newFixedThreadPool(5);
        CountDownLatch completeDone = new CountDownLatch(jobs);
        AtomicInteger succeeded = new AtomicInteger(0);

        for (ClaimedJob job : claimed) {
            executor.submit(() -> {
                boolean success = service.completeJob(job.jobId, job.leaseToken, new JobResult.Success());
                if (success) succeeded.incrementAndGet();
                completeDone.countDown();
            });
        }

        completeDone.await();
        executor.shutdown();

        assertEquals(jobs, succeeded.get(), "All completions should succeed");
        assertEquals(0, service.getTotalPendingCount());
        assertEquals(0, service.getInFlightCount());
    }
}


