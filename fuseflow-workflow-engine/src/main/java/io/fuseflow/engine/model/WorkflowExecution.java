package io.fuseflow.engine.model;

import java.time.Instant;
import java.util.UUID;

/**
 * A runnable instance of a workflow definition (aggregate root of the {@code engine} schema).
 *
 * @param shard the deterministic execution shard (Phase 5, engine HA): computed once at start
 *              via {@code floorMod(executionId.hashCode(), shardCount)} and stored so boot-time
 *              recovery can be scoped per engine instance.
 */
public record WorkflowExecution(
        UUID id,
        UUID workflowId,
        String workflowName,
        long definitionVersion,
        String input,          // raw JSON, may be null
        String output,         // raw JSON, may be null
        WorkflowStatus status,
        long version,
        Instant createdAt,
        Instant updatedAt,
        Instant startedAt,
        Instant completedAt,
        int shard) {

    /** Backward-compatible constructor (shard defaults to 0, for tests and legacy callers). */
    public WorkflowExecution(UUID id, UUID workflowId, String workflowName, long definitionVersion,
                             String input, String output, WorkflowStatus status, long version,
                             Instant createdAt, Instant updatedAt, Instant startedAt, Instant completedAt) {
        this(id, workflowId, workflowName, definitionVersion, input, output, status, version,
                createdAt, updatedAt, startedAt, completedAt, 0);
    }
}
