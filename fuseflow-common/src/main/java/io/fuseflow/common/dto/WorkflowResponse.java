package io.fuseflow.common.dto;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Full representation of a workflow definition returned by the API. Shared wire contract
 * (Phase 6): the SDK's {@code DefinitionClient} reads it back to make registration idempotent
 * (same DAG → no-op, different DAG → replace) without drifting from the definition service.
 */
public record WorkflowResponse(
        UUID id,
        String name,
        String description,
        List<Task> tasks,
        long version,
        Instant createdAt,
        Instant updatedAt) {

    public record Task(String id, String activity, List<String> dependsOn) {
    }
}
