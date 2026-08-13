package io.fuseflow.engine.dispatch;

import io.fuseflow.common.messaging.ActivityTask;

/**
 * SPI for actually executing an activity's business logic (Phase 2 in-memory mode).
 * Tests inject deterministic fakes (immediate success, fail-a-task, latch-blocked);
 * the demo app wires {@code DemoActivityExecutor} which auto-completes after a delay.
 */
public interface ActivityExecutor {

    /** Execute the activity. Throwing is treated as a failure by the dispatcher. */
    ActivityResult execute(ActivityTask task) throws Exception;
}
