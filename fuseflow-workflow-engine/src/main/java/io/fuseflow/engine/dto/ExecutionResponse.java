package io.fuseflow.engine.dto;

import tools.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** Full representation of a workflow execution returned by the API. */
public record ExecutionResponse(
        UUID id,
        UUID workflowId,
        String workflowName,
        JsonNode input,
        JsonNode output,
        String status,
        long version,
        Instant createdAt,
        Instant updatedAt,
        Instant startedAt,
        Instant completedAt,
        List<ActivityResponse> activities) {

    /** A single task within the execution with its current state. */
    public record ActivityResponse(
            String taskId,
            String activityName,
            String status,
            int attempt,
            JsonNode output,
            String error,
            Instant updatedAt) {
    }
}
