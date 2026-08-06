package io.fuseflow.engine.model;

import java.time.Instant;
import java.util.UUID;

/** An immutable, append-only execution event (event sourcing / FR-9). */
public record WorkflowEvent(
        long id,
        UUID workflowExecutionId,
        String eventType,
        String payload,   // raw JSON
        Instant createdAt) {
}
