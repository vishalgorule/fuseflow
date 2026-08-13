package io.fuseflow.engine.dispatch;

import io.fuseflow.common.messaging.ActivityTask;

import java.util.UUID;

/**
 * Outcome of executing an {@link ActivityTask}. Carries {@code (executionId, taskId, attempt)}
 * so the engine can implement at-least-once idempotency (duplicate results are ignored).
 */
public record ActivityResult(
        UUID executionId,
        String taskId,
        int attempt,
        boolean success,
        String output,   // raw JSON, may be null
        String error) {  // may be null

    public static ActivityResult success(ActivityTask task, String output) {
        return new ActivityResult(task.executionId(), task.taskId(), task.attempt(), true, output, null);
    }

    public static ActivityResult failure(ActivityTask task, String error) {
        return new ActivityResult(task.executionId(), task.taskId(), task.attempt(), false, null, error);
    }
}
