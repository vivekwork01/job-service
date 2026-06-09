package com.job_service.model;

import java.time.Instant;
import java.util.Map;

/**
 * Internal representation of a job with state tracking.
 * This is the canonical record stored in jobsById.
 */
public class JobRecord {
    public final String jobId;
    public final JobSpec spec;
    public JobStatus status;
    public final Instant createdAt;
    public Instant lastModifiedAt;
    public int attempts;
    public String lastError;
    public String currentWorkerId;  // Worker currently holding the lease
    public String currentLeaseToken;  // Unique token for current lease
    public Instant leaseExpiresAt;

    public JobRecord(String jobId, JobSpec spec) {
        this.jobId = jobId;
        this.spec = spec;
        this.status = JobStatus.PENDING;
        this.createdAt = Instant.now();
        this.lastModifiedAt = Instant.now();
        this.attempts = 0;
        this.lastError = null;
    }

    public synchronized void claimJob(String workerId, String leaseToken, Instant expiresAt) {
        this.status = JobStatus.CLAIMED;
        this.currentWorkerId = workerId;
        this.currentLeaseToken = leaseToken;
        this.leaseExpiresAt = expiresAt;
        this.lastModifiedAt = Instant.now();
    }

    public synchronized void releaseLease() {
        this.currentWorkerId = null;
        this.currentLeaseToken = null;
        this.leaseExpiresAt = null;
    }

    public synchronized boolean isLeaseExpired() {
        return leaseExpiresAt != null && Instant.now().isAfter(leaseExpiresAt);
    }

    @Override
    public String toString() {
        return "JobRecord{" +
                "jobId='" + jobId + '\'' +
                ", status=" + status +
                ", attempts=" + attempts +
                ", spec=" + spec +
                '}';
    }
}

