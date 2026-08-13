package io.fuseflow.common.messaging;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * A workflow lifecycle event published by the engine to the {@code workflow-events} topic as an
 * asynchronous mirror of the event-sourced {@code engine.workflow_event} table (architecture §8).
 * The Postgres table remains the source of truth for replay; the topic is for external observers
 * (dashboard, analytics) and is not consumed by any platform service in v1.
 *
 * @param eventType e.g. WorkflowStarted, WorkflowCompleted, WorkflowFailed
 * @param payload   JSON-able key/value detail
 */
public record WorkflowEventMessage(
        UUID executionId,
        String eventType,
        Map<String, Object> payload,
        Instant occurredAt) {
}
