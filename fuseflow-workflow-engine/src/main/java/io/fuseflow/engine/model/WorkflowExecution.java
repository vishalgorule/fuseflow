package io.fuseflow.engine.model;

import java.time.Instant;
import java.util.UUID;

/** A runnable instance of a workflow definition (aggregate root of the {@code engine} schema). */
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
        Instant completedAt) {
}
