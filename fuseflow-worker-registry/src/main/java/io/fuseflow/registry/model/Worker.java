package io.fuseflow.registry.model;

import java.time.Instant;
import java.util.UUID;

/** A registered worker (aggregate root of the {@code registry} schema). */
public record Worker(
        UUID id,
        String host,
        int capacity,           // max concurrent activities the worker can run
        WorkerStatus status,
        Instant lastHeartbeatAt,
        long version,
        Instant createdAt,
        Instant updatedAt) {
}
