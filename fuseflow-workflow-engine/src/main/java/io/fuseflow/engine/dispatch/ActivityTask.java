package io.fuseflow.engine.dispatch;

import java.util.UUID;

/** A unit of work handed to a {@link TaskDispatcher}: execute {@code activityName} for an execution. */
public record ActivityTask(
        UUID executionId,
        String taskId,
        String activityName,
        String input,   // raw JSON input of the execution, may be null
        int attempt) {
}
