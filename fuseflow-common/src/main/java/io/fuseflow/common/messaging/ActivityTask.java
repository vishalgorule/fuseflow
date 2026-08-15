package io.fuseflow.common.messaging;

import java.util.UUID;

/**
 * A unit of work handed to a worker (Phase 4): execute {@code activityName} for an execution.
 * This is the wire contract carried on the pool dispatch topics ({@code fuseflow-pool.<poolName>},
 * Phase 5) — produced by the engine's dispatcher, consumed by the worker SDK. Serialized as JSON
 * with the project's Jackson 3 {@code ObjectMapper} (engine and SDK share this record, so the
 * contract cannot drift).
 *
 * @param executionId  the workflow execution this task belongs to
 * @param taskId       the task id within the execution's DAG
 * @param activityName the activity to execute
 * @param input        raw JSON input of the execution, may be null
 * @param attempt      retry attempt number (echoed back in the result for idempotency)
 */
public record ActivityTask(
        UUID executionId,
        String taskId,
        String activityName,
        String input,
        int attempt) {
}
