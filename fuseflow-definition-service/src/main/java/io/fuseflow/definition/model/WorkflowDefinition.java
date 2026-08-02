package io.fuseflow.definition.model;

import java.time.Instant;
import java.util.UUID;

/** A registered workflow definition (aggregate root of the {@code definition} schema). */
public record WorkflowDefinition(
        UUID id,
        String name,
        String description,
        long version,
        Instant createdAt,
        Instant updatedAt) {
}
