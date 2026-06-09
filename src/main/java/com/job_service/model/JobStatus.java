package com.job_service.model;

/**
 * Represents the lifecycle state of a job.
 *
 * State machine:
 * pending -> claimed -> succeeded | failed | retry_scheduled -> pending
 */
public enum JobStatus {
    PENDING,           // Job is waiting to be claimed
    CLAIMED,           // Job is claimed by a worker and being processed
    SUCCEEDED,         // Job completed successfully
    FAILED,            // Job failed and moved to DLQ (max retries exhausted or permanent failure)
    RETRY_SCHEDULED    // Job scheduled for retry after backoff
}

