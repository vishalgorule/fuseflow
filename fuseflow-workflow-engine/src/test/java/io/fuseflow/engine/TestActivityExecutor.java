package io.fuseflow.engine;

import io.fuseflow.engine.dispatch.ActivityExecutor;
import io.fuseflow.engine.dispatch.ActivityResult;
import io.fuseflow.common.messaging.ActivityTask;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Deterministic in-memory worker for engine integration tests: auto-completes every activity
 * with hooks to fail specific tasks or block on them (used to simulate a mid-run crash for the
 * restart-recovery test). A fresh instance is created per Spring context, so blocking configured
 * in one context never leaks into a recovered one.
 */
class TestActivityExecutor implements ActivityExecutor {

    private final Set<String> failTasks = ConcurrentHashMap.newKeySet();
    private final ConcurrentHashMap<String, CountDownLatch> blockLatches = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CountDownLatch> dispatchedLatches = new ConcurrentHashMap<>();

    /** The next executions of the given tasks return failure results. */
    void fail(String... taskIds) {
        failTasks.addAll(Set.of(taskIds));
    }

    /** The next execution of {@code taskId} blocks until {@link #release} is called. */
    void blockOn(String taskId) {
        blockLatches.put(taskId, new CountDownLatch(1));
    }

    /** Clears all hooks. Called between tests; per-context instances are fresh anyway. */
    void reset() {
        failTasks.clear();
        blockLatches.clear();
        dispatchedLatches.clear();
    }

    /** Blocks until the executor has picked up {@code taskId} (10s timeout). */
    void awaitDispatched(String taskId) throws InterruptedException {
        CountDownLatch latch = dispatchedLatches.computeIfAbsent(taskId, k -> new CountDownLatch(1));
        if (!latch.await(10, TimeUnit.SECONDS)) {
            throw new IllegalStateException("task was never dispatched: " + taskId);
        }
    }

    void release(String taskId) {
        CountDownLatch latch = blockLatches.remove(taskId);
        if (latch != null) {
            latch.countDown();
        }
    }

    @Override
    public ActivityResult execute(ActivityTask task) throws Exception {
        // Signal dispatch before potentially blocking so tests can observe the STARTED state.
        dispatchedLatches.computeIfAbsent(task.taskId(), k -> new CountDownLatch(1)).countDown();
        CountDownLatch block = blockLatches.get(task.taskId());
        if (block != null) {
            block.await();
        }
        if (failTasks.contains(task.taskId())) {
            return ActivityResult.failure(task, "simulated failure for " + task.taskId());
        }
        return ActivityResult.success(task, "{\"message\":\"ok\",\"taskId\":\"" + task.taskId() + "\"}");
    }
}
