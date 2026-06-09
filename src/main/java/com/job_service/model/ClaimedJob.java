package com.job_service.model;

import java.time.Instant;
import java.util.Map;

/**
 * Job returned to a worker when claiming a job from the queue.
 * Contains the job ID, spec, and lease information needed for processing.
 */
public class ClaimedJob {
    public final String jobId;
    public final String jobType;
    public final Map<String, Object> payload;
    public final String leaseToken;
    public final Instant leaseExpiresAt;

    public ClaimedJob(String jobId, String jobType, Map<String, Object> payload,
                      String leaseToken, Instant leaseExpiresAt) {
        this.jobId = jobId;
        this.jobType = jobType;
        this.payload = payload;
        this.leaseToken = leaseToken;
        this.leaseExpiresAt = leaseExpiresAt;
    }

    @Override
    public String toString() {
        return "ClaimedJob{" +
                "jobId='" + jobId + '\'' +
                ", jobType='" + jobType + '\'' +
                ", leaseExpiresAt=" + leaseExpiresAt +
                '}';
    }
}

