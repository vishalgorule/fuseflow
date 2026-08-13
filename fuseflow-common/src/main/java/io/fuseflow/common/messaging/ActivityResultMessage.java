package io.fuseflow.common.messaging;

import java.util.UUID;

/**
 * A worker-to-engine signal about an activity on the {@code activity-results} topic.
 * Carries {@code (executionId, taskId, attempt)} so the engine can implement at-least-once
 * idempotency — duplicate or stale results are ignored by the state machine.
 *
 * @param type   STARTED (progress) or COMPLETED/FAILED (terminal outcome)
 * @param output raw JSON output, present for COMPLETED
 * @param error  failure message, present for FAILED
 */
public record ActivityResultMessage(
        UUID executionId,
        String taskId,
        int attempt,
        ActivityResultType type,
        String output,
        String error) {

    public static ActivityResultMessage started(ActivityTask task) {
        return new ActivityResultMessage(task.executionId(), task.taskId(), task.attempt(),
                ActivityResultType.STARTED, null, null);
    }

    public static ActivityResultMessage completed(ActivityTask task, String output) {
        return new ActivityResultMessage(task.executionId(), task.taskId(), task.attempt(),
                ActivityResultType.COMPLETED, output, null);
    }

    public static ActivityResultMessage failed(ActivityTask task, String error) {
        return new ActivityResultMessage(task.executionId(), task.taskId(), task.attempt(),
                ActivityResultType.FAILED, null, error);
    }
}
