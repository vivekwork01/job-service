package com.job_service.handler;

import com.job_service.model.JobResult;
import java.util.Map;

/**
 * Interface that job handlers implement to execute job payloads.
 */
public interface JobHandler {
    /**
     * Execute the job with the given payload.
     *
     * @param payload The opaque job payload
     * @return JobResult indicating success or failure type
     */
    JobResult run(Map<String, Object> payload);
}

