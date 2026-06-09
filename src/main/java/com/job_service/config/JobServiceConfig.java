package com.job_service.config;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration for JobService.
 * Defines per-type lease durations and other settings.
 *
 * Uses Option B: Per-job-type lease duration with global default.
 */
public class JobServiceConfig {
    // Default lease duration for all job types (in seconds)
    public static final long DEFAULT_LEASE_DURATION_SECONDS = 30;

    // Base delay for exponential backoff (in milliseconds)
    public static final long BASE_BACKOFF_MILLIS = 1000;

    // Max delay for exponential backoff (in milliseconds)
    public static final long MAX_BACKOFF_MILLIS = 60000;

    // Starvation window: number of high-priority selections before fairness rotation
    public static final int STARVATION_WINDOW = 50;

    // Per-type lease duration overrides (type -> duration in seconds)
    private final Map<String, Long> perTypeLeaseDurations;

    private long defaultLeaseDuration;

    public JobServiceConfig() {
        this.perTypeLeaseDurations = new HashMap<>();
        this.defaultLeaseDuration = DEFAULT_LEASE_DURATION_SECONDS;
    }

    /**
     * Get lease duration for a job type (or default if not configured).
     */
    public long getLeaseDurationSeconds(String jobType) {
        return perTypeLeaseDurations.getOrDefault(jobType, defaultLeaseDuration);
    }

    /**
     * Set per-type lease duration override.
     */
    public void setLeaseDurationForType(String jobType, long durationSeconds) {
        perTypeLeaseDurations.put(jobType, durationSeconds);
    }

    /**
     * Set global default lease duration.
     */
    public void setDefaultLeaseDuration(long durationSeconds) {
        this.defaultLeaseDuration = durationSeconds;
    }

    public long getDefaultLeaseDuration() {
        return defaultLeaseDuration;
    }
}

