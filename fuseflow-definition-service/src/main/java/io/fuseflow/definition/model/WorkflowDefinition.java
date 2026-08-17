package io.fuseflow.definition.model;

import java.time.Instant;
import java.util.UUID;

/**
 * A registered workflow definition (aggregate root of the {@code definition} schema).
 *
 * @param retryPolicyJson raw JSON of the workflow-level retry policy (Phase 7), null when unset
 */
public record WorkflowDefinition(
        UUID id,
        String name,
        String description,
        String retryPolicyJson,
        long version,
        Instant createdAt,
        Instant updatedAt) {

    /** Convenience constructor for definitions without a retry policy. */
    public WorkflowDefinition(UUID id, String name, String description, long version,
                              Instant createdAt, Instant updatedAt) {
        this(id, name, description, null, version, createdAt, updatedAt);
    }
}
