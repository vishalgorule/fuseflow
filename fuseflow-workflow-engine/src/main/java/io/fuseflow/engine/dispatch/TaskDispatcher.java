package io.fuseflow.engine.dispatch;

import io.fuseflow.common.messaging.ActivityTask;

/**
 * SPI (Phase 2): hands an {@link ActivityTask} to whatever executes activities.
 * The sole implementation is {@link KafkaTaskDispatcher}, which routes tasks to worker pools
 * via Kafka topics.
 */
public interface TaskDispatcher {

    /** Asynchronously dispatch the task. Must be safe to call with no durable side effects. */
    void dispatch(ActivityTask task);
}
