package io.fuseflow.engine.model;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * A single DAG task within an execution. The dependency graph is materialized here:
 * {@code remainingDependencies} is the count of uncompleted upstream tasks; {@code dependents}
 * lists the task ids that must be decremented when this activity completes. The scheduler never
 * re-scans the definition DAG (dependency-counting model, architecture §6.2).
 *
 * @param retryDueAt Phase 7 due-time queue: when a failed-but-retryable attempt is waiting to be
 *                   re-dispatched (the row stays SCHEDULED until the retry poller picks it up).
 *                   Null = on the normal dispatch path.
 * @param errorType  Phase 7: the failing exception class name, matched against the policy's
 *                   nonRetryableExceptions and reported on ActivityFailed/dead-letter messages.
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
        Instant retryDueAt,
        String errorType,
        long version,
        Instant createdAt,
        Instant updatedAt) {

    /** Convenience constructor for rows without retry state (pre-Phase 7 callers/tests). */
    public ActivityExecution(UUID workflowExecutionId, String taskId, String activityName,
                             ActivityStatus status, int remainingDependencies, List<String> dependents,
                             int attempt, String output, String error, long version,
                             Instant createdAt, Instant updatedAt) {
        this(workflowExecutionId, taskId, activityName, status, remainingDependencies, dependents,
                attempt, output, error, null, null, version, createdAt, updatedAt);
    }
}
