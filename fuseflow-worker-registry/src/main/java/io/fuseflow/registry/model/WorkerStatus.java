package io.fuseflow.registry.model;

/**
 * Liveness states of a registered worker (Phase 3, FR-4 / FR-12).
 *
 * <p>Derived from heartbeat freshness by the offline detector:
 * <ul>
 *   <li>{@code ONLINE} — last heartbeat is recent (within {@code degraded-after}).</li>
 *   <li>{@code DEGRADED} — some heartbeats were missed (older than {@code degraded-after},
 *       still within the heartbeat timeout).</li>
 *   <li>{@code OFFLINE} — no heartbeat within the configured timeout (N missed heartbeats).</li>
 * </ul>
 * Heartbeats revive a worker to {@code ONLINE} at any time; the detector only ever downgrades.
 */
public enum WorkerStatus {
    ONLINE,
    DEGRADED,
    OFFLINE
}
