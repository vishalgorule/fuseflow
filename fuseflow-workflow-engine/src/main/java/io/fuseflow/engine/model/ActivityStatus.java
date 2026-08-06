package io.fuseflow.engine.model;

/**
 * Lifecycle states of a single activity within an execution.
 *
 * <p>{@code PENDING} → {@code SCHEDULED} → {@code STARTED} → {@code COMPLETED | FAILED}.
 * On recovery, {@code SCHEDULED} and {@code STARTED} activities are re-dispatched: with the
 * in-memory dispatcher (Phase 2) there is no durable in-flight progress, so both states are
 * re-driven from durable state. {@code PENDING} activities with satisfied dependencies are
 * scheduled first.
 */
public enum ActivityStatus {
    PENDING,
    SCHEDULED,
    STARTED,
    COMPLETED,
    FAILED
}
