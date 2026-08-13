package io.fuseflow.sdk.core;

import java.util.UUID;

/**
 * Context passed to every {@link io.fuseflow.sdk.annotation.Activity} execution: identifies the
 * execution/task and carries the execution input (raw JSON, may be null) plus the attempt
 * number echoed back in the result for engine-side idempotency.
 */
public record ActivityContext(
        UUID executionId,
        String taskId,
        String activityName,
        int attempt,
        String input) {
}
