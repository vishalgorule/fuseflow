package io.fuseflow.common.dto;

/**
 * Optional body of a worker heartbeat request (shared SDK ↔ registry contract).
 *
 * @param capacity optional new capacity (must be >= 1 when present)
 */
public record HeartbeatRequest(Integer capacity) {
}
