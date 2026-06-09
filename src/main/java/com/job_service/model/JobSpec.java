package com.job_service.model;

import java.util.Map;
import java.util.Objects;

/**
 * Specification for a job to be submitted to the service.
 *
 * Parameters:
 * - type: job type (e.g., "send_email", "resize_image")
 * - payload: opaque data passed to handler
 * - priority: priority level (0-9, default 5, higher executes earlier)
 * - max_retries: max number of retries before DLQ (default 3)
 * - leaseDurationSeconds: override per-job lease duration (optional, uses type default if not provided)
 */
public class JobSpec {
    public final String type;
    public final Map<String, Object> payload;
    public final int priority;
    public final int maxRetries;
    public final Long leaseDurationSeconds;  // Optional override

    public JobSpec(String type, Map<String, Object> payload) {
        this(type, payload, 5, 3, null);
    }

    public JobSpec(String type, Map<String, Object> payload, int priority, int maxRetries) {
        this(type, payload, priority, maxRetries, null);
    }

    public JobSpec(String type, Map<String, Object> payload, int priority, int maxRetries, Long leaseDurationSeconds) {
        this.type = Objects.requireNonNull(type, "type cannot be null");
        this.payload = Objects.requireNonNull(payload, "payload cannot be null");
        this.priority = priority;
        this.maxRetries = maxRetries;
        this.leaseDurationSeconds = leaseDurationSeconds;
    }

    @Override
    public String toString() {
        return "JobSpec{" +
                "type='" + type + '\'' +
                ", priority=" + priority +
                ", maxRetries=" + maxRetries +
                ", leaseDurationSeconds=" + leaseDurationSeconds +
                '}';
    }
}

