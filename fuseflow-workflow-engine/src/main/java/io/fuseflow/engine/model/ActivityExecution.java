package io.fuseflow.engine.model;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * A single DAG task within an execution. The dependency graph is materialized here:
 * {@code remainingDependencies} is the count of uncompleted upstream tasks; {@code dependents}
 * lists the task ids that must be decremented when this activity completes. The scheduler never
 * re-scans the definition DAG (dependency-counting model, architecture §6.2).
 */
public record ActivityExecution(
        UUID workflowExecutionId,
        String taskId,
        String activityName,
        ActivityStatus status,
        int remainingDependencies,
        List<String> dependents,
        int attempt,
        String output,   // raw JSON, may be null
        String error,    // may be null
        long version,
        Instant createdAt,
        Instant updatedAt) {
}
