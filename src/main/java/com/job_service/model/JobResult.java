package com.job_service.model;

/**
 * Result of job execution returned by JobHandler.
 */
public abstract class JobResult {

    public static class Success extends JobResult {
        @Override
        public String toString() {
            return "Success()";
        }
    }

    public static class TransientFailure extends JobResult {
        public final String error;

        public TransientFailure(String error) {
            this.error = error;
        }

        @Override
        public String toString() {
            return "TransientFailure(error=" + error + ")";
        }
    }

    public static class PermanentFailure extends JobResult {
        public final String error;

        public PermanentFailure(String error) {
            this.error = error;
        }

        @Override
        public String toString() {
            return "PermanentFailure(error=" + error + ")";
        }
    }
}

