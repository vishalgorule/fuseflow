package io.fuseflow.registry.dto;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** Full representation of a worker returned by the API. */
public record WorkerResponse(
        UUID id,
        String host,
        int capacity,
        String status,
        List<String> activities,
        Instant lastHeartbeatAt,
        long version,
        Instant createdAt,
        Instant updatedAt) {
}
