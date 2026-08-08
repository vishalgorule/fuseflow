package io.fuseflow.registry.dto;

/**
 * Optional body of a heartbeat request. Currently only {@code capacity} may be refreshed.
 *
 * @param capacity optional new capacity (must be >= 1 when present)
 */
public record HeartbeatRequest(Integer capacity) {
}
