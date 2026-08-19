package io.fuseflow.common.messaging;

import java.time.Instant;
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
 * @param dispatchAt   when the engine dispatched this message (stamped by the Kafka dispatcher);
 *                     null on messages produced before the field existed (worker executes them)
 * @param expiresAt    dispatch-time + the engine's start timeout — a worker must skip a message
 *                     past this: the engine has already timed it out and (likely) retried, so
 *                     executing it would be wasted work whose result is dropped anyway. Stamped
 *                     by the engine, so worker and engine can never drift on the timeout
 */
public record ActivityTask(
        UUID executionId,
        String taskId,
        String activityName,
        String input,
        int attempt,
        Instant dispatchAt,
        Instant expiresAt) {

    /**
     * 5-arg convenience form (tests, in-memory dispatch): stamps the dispatch time at
     * construction. The Kafka dispatcher re-stamps both timestamps at publish, so callers
     * never need to pass them explicitly.
     */
    public ActivityTask(UUID executionId, String taskId, String activityName, String input, int attempt) {
        this(executionId, taskId, activityName, input, attempt, Instant.now(), null);
    }
}
