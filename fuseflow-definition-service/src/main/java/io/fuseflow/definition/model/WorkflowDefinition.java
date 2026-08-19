package io.fuseflow.definition.model;

import java.time.Instant;
import java.util.UUID;

/**
 * A registered workflow definition (aggregate root of the {@code definition} schema).
 *
 * <p>Phase 8: {@code semanticVersion} is the version label; {@code (name, semanticVersion)}
 * is the unique key. Each row is an immutable snapshot — a new DAG is a new version row.
 *
 * @param semanticVersion version label (default "1")
 * @param retryPolicyJson raw JSON of the workflow-level retry policy (Phase 7), null when unset
 */
public record WorkflowDefinition(
        UUID id,
        String name,
        String semanticVersion,
        String description,
        String retryPolicyJson,
        long version,
        Instant createdAt,
        Instant updatedAt) {

    /** Convenience constructor for definitions without a semantic version (defaults to "1"). */
    public WorkflowDefinition(UUID id, String name, String description, String retryPolicyJson,
                              long version, Instant createdAt, Instant updatedAt) {
        this(id, name, null, description, retryPolicyJson, version, createdAt, updatedAt);
    }

    /** Convenience constructor for definitions without a retry policy or semantic version. */
    public WorkflowDefinition(UUID id, String name, String description, long version,
                              Instant createdAt, Instant updatedAt) {
        this(id, name, null, description, null, version, createdAt, updatedAt);
    }
}
