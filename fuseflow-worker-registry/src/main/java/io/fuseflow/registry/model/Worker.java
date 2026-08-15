package io.fuseflow.registry.model;

import java.time.Instant;
import java.util.UUID;

/** A registered worker (aggregate root of the {@code registry} schema). */
public record Worker(
        UUID id,
        String host,
        WorkerStatus status,
        Instant lastHeartbeatAt,
        long version,
        Instant createdAt,
        Instant updatedAt,
        String poolName,        // the worker pool (capability group) this worker joins
        Integer concurrency) {  // pool-level declared parallelism (drives pool-topic partitions)

    /** Backward-compatible constructor (pool defaults to {@code default}). */
    public Worker(UUID id, String host, WorkerStatus status,
                  Instant lastHeartbeatAt, long version, Instant createdAt, Instant updatedAt) {
        this(id, host, status, lastHeartbeatAt, version, createdAt, updatedAt, "default", null);
    }
}
