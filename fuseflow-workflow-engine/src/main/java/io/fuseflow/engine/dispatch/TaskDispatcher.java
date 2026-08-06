package io.fuseflow.engine.dispatch;

/**
 * SPI (Phase 2, plan §4 task 4): hands an {@link ActivityTask} to whatever executes activities.
 * The in-memory implementation ships in Phase 2; a Kafka-backed implementation replaces it in
 * Phase 4 without touching the scheduler.
 */
public interface TaskDispatcher {

    /** Asynchronously dispatch the task. Must be safe to call with no durable side effects. */
    void dispatch(ActivityTask task);
}
