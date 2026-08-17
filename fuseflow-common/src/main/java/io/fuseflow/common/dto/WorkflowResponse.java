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
        RetryPolicy retryPolicy,
        List<Task> tasks,
        long version,
        Instant createdAt,
        Instant updatedAt) {

    /** Convenience constructor for callers without a retry policy. */
    public WorkflowResponse(UUID id, String name, String description, List<Task> tasks,
                            long version, Instant createdAt, Instant updatedAt) {
        this(id, name, description, null, tasks, version, createdAt, updatedAt);
    }

    public record Task(String id, String activity, List<String> dependsOn, RetryPolicy retryPolicy) {

        /** Convenience constructor for callers without a per-task retry policy. */
        public Task(String id, String activity, List<String> dependsOn) {
            this(id, activity, dependsOn, null);
        }
    }
}
