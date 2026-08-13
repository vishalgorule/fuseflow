package io.fuseflow.common.messaging;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * A worker state-change event published by the registry to the {@code worker-events} topic
 * (architecture §8). Only transitions are published — heartbeats stay REST (low latency, high
 * frequency).
 *
 * @param eventType e.g. worker_registered, worker_deregistered, worker_offline
 * @param payload   JSON-able key/value detail (host, capacity, activities, …)
 */
public record WorkerEventMessage(
        UUID workerId,
        String eventType,
        Map<String, Object> payload,
        Instant occurredAt) {
}
