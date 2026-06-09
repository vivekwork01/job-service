# Job Service - PLAN

This document is the implementation and design plan for the in-process Background Job Service described in `problem_statement.md`.

Checklist (what I'll deliver):
- PLAN and decisions for every ambiguous item in the problem statement
- Design overview and data structures
- API surface and worker contract
- Backoff/retry formula and justification
- Observability and test plan (concurrency tests)
- Incremental implementation and commit plan

## 1. Understanding the problem

We must implement an in-process job queue where producers submit jobs and workers running in the same process pull and execute them. The service must guarantee at-least-once execution, lease semantics (visibility timeout), retry with exponential backoff, DLQ for exhausted retries, and observability APIs for queue and worker state.

Workers register the job types they handle and must be able to renew leases for long-running jobs. The system is in-memory for this exercise, but must be designed so persistence (e.g., relational DB, Redis) could be added later without changing the core APIs.

## 2. Functional requirements (summary)

- submit(job_spec) -> job_id (type, payload, priority 0-9 default 5, max_retries default 3)
- Workers register for a list of types and pull jobs by acquiring a lease (visibility timeout)
- Job state machine: pending -> claimed -> succeeded | failed (DLQ) | retry_scheduled -> pending
- Exponential backoff for retries; permanent failure bypasses retries and goes to DLQ
- Observability: pending counts by type/priority, in-flight count, DLQ size/list, per-worker stats
- Tests must cover lease expiry (worker crash), retry/backoff, lease renewal, priority ordering, DLQ, permanent-failure short-circuit

## 3. Ambiguous decisions and recommended choices

For each ambiguous behavior listed in the problem statement I evaluate 1-2 sensible options and pick the recommended solution with justification.

### 3.1 Lease duration

**Options:**
- A: Fixed global default lease duration (e.g., 30s) with ability to override per-job at submission time.
- B: Per-job-type default configured in the service; submission may still override.

**Recommendation:** Option A (global default, override per submission).

**Rationale:** A global default keeps the system simple and predictable; allowing an override per-job covers long-running or short-running special cases without complicating worker semantics. A per-type default can be added later if profiling shows systematic differences by type. Suggested default: 30 seconds.

**Default selection reasoning:** 30s is a commonly-used visibility timeout that balances worker crash detection and typical handler runtime for short to medium tasks. We'll also provide a lease renewal API so long-running handlers can extend beyond the lease safely.

### 3.2 Lease semantics on overrun (late completion)

**Options:**
- A: Accept completion only if the finishing worker still holds the active lease token. If the lease expired and another worker re-claimed, completion is rejected and treated as a no-op (worker should detect and discard result).
- B: Allow late completion to succeed (last writer wins); i.e., if worker A finishes after leasing expired and worker B also executed, final state is whatever comes last.

**Recommendation:** Option A (require matching lease token to commit completion).

**Rationale:** Option A reduces correctness surprises and helps to avoid silently overwriting a newer successful result with an older result. It enforces an ownership model: when lease expires ownership transfers. It still provides at-least-once semantics because if a worker completes after expiry, the job may have been retried or completed by another worker; rejecting late commits avoids double-committing and is safer for correctness. To handle possible rejected successful work, we will document that handlers must be idempotent; we also provide an optional idempotency key in JobSpec for stronger semantics.

**Behavior details:** Each lease operation returns a unique lease token (UUID) and expiration timestamp. To mark a job done or failed the worker must present the same lease token. If the lease token does not match current lease on the job, the completion request will be rejected with a conflict error.

### 3.3 Priority semantics

**Options:**
- A: Strict priority ordering: jobs with higher numeric priority always dequeued before lower priority jobs.
- B: Weighted/soft priority: high priority jobs have preference but lower-priority jobs still get a share to prevent starvation.

**Recommendation:** Option B (weighted/soft priority with strong bias towards higher priorities but bounded starvation prevention).

**Rationale:** Strict priority can starve low-priority jobs indefinitely if producers keep producing high-priority work. For a long-running system, starvation is problematic. A simple and practical approach: always attempt to dequeue from the highest-priority non-empty bucket first, but after N consecutive high-priority dequeues give a proportional chance to lower priorities (or more simply use round-robin across priorities weighted by priority+1). For this exercise we implement a priority queue that normally favors higher priority but rotates to lower priorities occasionally (configurable fairness window). This preserves responsiveness for high-priority tasks while avoiding starvation.

**Note:** For strict per-type SLAs a user could set a high priority and dedicated worker pool.

### 3.4 Same-priority ordering

**Options:**
- A: FIFO ordering within the same priority
- B: LIFO or unspecified ordering

**Recommendation:** FIFO within the same priority.

**Rationale:** FIFO is the most intuitive and fair choice for same-priority jobs. It is easy to implement with enqueue timestamps and ensures predictable behavior for tests.

### 3.5 At-least-once vs at-most-once and idempotency guidance

The spec requires at-least-once. We will implement the system to provide at-least-once delivery semantics. However, duplicates can still occur when leases expire and jobs are retried or if workers crash after successfully processing but before acknowledging.

**Guidance for handlers:**
- Require handlers to be idempotent or supply an idempotency key in the JobSpec so handlers can deduplicate. The service will expose job_id and job metadata to handlers.
- Document the possibility of duplicate execution and recommend using external idempotent storage or a deduplication table when side effects are involved.

## 4. System design and data structures

### 4.1 Core concepts

- **Job:** immutable metadata (id, type, payload, priority, max_retries, created_at, last_error, attempts)
- **Queue:** in-memory store of pending jobs organized by type and priority
- **Lease:** object containing lease owner (worker id), lease token (UUID), expires_at timestamp
- **DLQ:** per-type list of dead-lettered jobs for inspection

### 4.2 In-memory structures (Java-friendly choices)

- `jobsById`: ConcurrentHashMap<JobId, JobRecord>
- `pendingQueues`: Map<String type, PriorityBlockingQueue<JobReference>> where comparator is (priority desc, enqueueTime asc)
- `claimedIndex`: ConcurrentHashMap<JobId, LeaseRecord>
- `retryScheduler`: DelayedQueue / scheduled executor for jobs in retry_scheduled state (job becomes available after backoff delay)
- `dlq`: ConcurrentLinkedQueue or ConcurrentLinkedDeque per type

JobRecord contains the canonical job state and immutable spec; JobReference is a small struct with jobId, priority, enqueueTime.

### 4.3 Atomicity and concurrency

All operations that move a job between states must be atomic w.r.t. the in-memory structures. We'll use careful synchronization using compare-and-set semantics on the `jobsById` record (e.g., updating a status field under a lock per job or using ConcurrentHashMap.compute to atomically check and set). Claiming a job must atomically remove it from the pending queue and add a lease to claimedIndex.

**Lease acquisition algorithm (simplified):**
- Worker requests a job of a set of types. The service examines pending queues for those types in priority order and attempts to poll a candidate job.
- When a candidate job is polled, an atomic compute on `jobsById` will confirm it's still pending and then set state to claimed with a new lease token and expiration timestamp, and record the claiming worker ID.

**Lease renewal:** worker presents jobId and leaseToken; the service checks the leaseToken equals current lease token and extends expires_at. If mismatch or expired, renewal fails.

**Complete / Fail:** worker presents leaseToken and desired final state (succeeded / failed with error). If token matches current lease token, transition is performed. On transient failure, schedule retry with backoff.

### 4.4 Retry/backoff formula

We will implement exponential backoff with full jitter per AWS recommendations.

**Formula:** baseDelay * 2^(attempts-1), capped at maxDelay. Then apply jitter by randomizing uniformly between 0 and that value.

**Parameters (defaults):** baseDelay = 1s, maxDelay = 60s. So attempts:
- 1st retry: up to 1s
- 2nd retry: up to 2s
- 3rd retry: up to 4s
- ... capped at 60s

**Justification:** Exponential backoff reduces load from repeated failures while jitter prevents thundering herds.

### 4.5 Dead Letter Queue (DLQ)

When attempts > max_retries or PermanentFailure is returned, the job is marked failed and appended to a per-type DLQ with metadata (last_error, attempts, timestamps). DLQ entries are queryable and listable.

## 5. APIs and worker contract

### Producer API
- `submit(JobSpec spec) -> jobId`
- `queryPendingCount(type?, priority?) -> counts`
- `listDLQ(type?, limit?, offset?) -> DLQ entries`

### Worker APIs (in-process calls / interfaces)
- `registerWorker(workerId, Set<String> types)`
- `pollForJob(workerId, Set<String> types, leaseDuration?) -> Optional<ClaimedJob>` (includes leaseToken)
- `renewLease(workerId, jobId, leaseToken, additionalDuration?) -> success/failure`
- `completeJob(workerId, jobId, leaseToken, Result) -> success/failure`
- `reportHeartbeat(workerId) -> updates worker lastSeen for observability`

### Interfaces
- `JobHandler { Result run(Map<String,Object> payload); }`
- `Result enum: Success, TransientFailure(error), PermanentFailure(error)`

### Observability APIs
- `pendingCountByType()` and `pendingCountByPriority(type?)`
- `inFlightCount()` and `inFlightByWorker()`
- `dlqSize(type?)` and `listDLQ(type?, limit?, offset?)`
- `perWorkerStats(workerId) -> { currentLeases, successCount, failureCount, lastSeen }`

## 6. Tests (required and extra)

All tests will run in-memory and use multithreaded execution to exercise concurrency.

### Required tests
- **at-least-once on worker crash:** a worker claims a job and never renews; after lease expiry another worker picks it up and succeeds. Assert job succeeded once and was not stuck in claimed forever.
- **retry behavior with backoff:** handler returns TransientFailure. Assert retries happen with increasing delays and DLQ after max_retries.
- **lease renewal happy path:** long-running handler renews lease repeatedly and completes successfully (no other worker can claim during renewable period).
- **priority ordering:** high priority tasks are favored; same-priority FIFO ordering is preserved.
- **DLQ on max retries:** job moves to DLQ and is queryable.
- **permanent-failure short-circuit:** handler returns PermanentFailure and job is moved to DLQ immediately.

### Extra tests
- Concurrent producers & workers under load to validate atomicity and absence of lost updates or double-claims.
- Observability counters reflect actual state under concurrent operations.

### Testing approach details
- Use JUnit for unit tests and java.util.concurrent Executors to run multiple worker threads.
- Use controllable clock or small sleeps to simulate lease expiry deterministically where possible; tests should avoid flaky timing by using small configured lease durations and polling intervals.

## 7. Implementation phases and commit plan

### Phase 1 (core queue and job lifecycle)
- implement JobRecord, job submission, pending queues, jobsById
- implement claim/poll, lease creation, and basic complete path (no retries)
- unit tests: submit + claim + complete

### Phase 2 (leases and lease renewal)
- implement lease store, lease expiry logic, worker registration
- implement lease renewal and expiry scanning (background sweeper or timestamp checks on poll)
- tests: lease expiry (simulate worker crash), lease renewal happy path

### Phase 3 (retries and backoff)
- implement retry scheduler using DelayQueue or ScheduledExecutor
- backoff formula with jitter and cap
- implement DLQ and PermanentFailure handling
- tests: retry backoff, DLQ after max_retries, permanent failure

### Phase 4 (priority, FIFO, fairness)
- implement priority-aware dequeuing with weighted fairness to avoid starvation
- tests: priority ordering, same-priority FIFO

### Phase 5 (observability & worker stats)
- implement counters and per-worker stats
- implement DLQ listing APIs
- tests: observability under concurrency

### Phase 6 (polish & documentation)
- docs, README, problem_statement.md cross-references, comments, and final test run

**Commit cadence:** make small, focused commits for each phase and include tests in the same commit. Keep commit messages descriptive and frequent.

## 8. Production considerations (how to extend beyond in-memory)

- **Persistence:** shift job store and queues to a durable backend (e.g., relational DB, Redis streams, or Kafka). Replace in-memory queue operations with transactional operations in the persistence layer.
- **Distributed workers:** move worker registration over a network; lease tokens still important for correctness.
- **High availability:** coordinate leader election to manage scheduled retry queue and lease sweeper in a multi-process deployment.

## 9. Observability & instrumentation

- Expose metrics counters (pending_by_type, pending_by_priority, in_flight, dlq_size, per_worker_success/failure) via a simple in-process metrics registry (or exportable endpoint) for future Prometheus integration.
- Logging: structured logs with job_id, attempt, worker_id, lease_token for important transitions.

## 10. Safety, edge-cases and notes

- Job payloads are opaque to the service; large payloads should be avoided in production (store payload references externally).
- To avoid time drift issues, use a single time source in the service (System.nanoTime for elapsed or Clock abstraction) and prefer monotonic clock for lease expiry where available.
- Tests should keep timings conservative but deterministic; where possible use injectable clock to make tests deterministic.

## Appendix: Default configuration values (tunable)

- defaultLeaseDuration = 30s
- leaseRenewWindow = 10s (workers should renew before this remaining)
- baseBackoff = 1s
- maxBackoff = 60s
- defaultPriority = 5
- defaultMaxRetries = 3
- starvationWindow = 50 (number of consecutive selections favoring higher priority before rotating)

## Next steps

If you approve this plan, I'll begin by creating the