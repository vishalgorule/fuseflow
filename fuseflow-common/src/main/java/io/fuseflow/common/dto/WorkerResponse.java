package io.fuseflow.common.dto;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** Full representation of a worker returned by the registry API. */
public record WorkerResponse(
        UUID id,
        String host,
        String status,
        List<String> activities,
        String poolName,
        Integer concurrency,
        Instant lastHeartbeatAt,
        long version,
        Instant createdAt,
        Instant updatedAt) {
}
