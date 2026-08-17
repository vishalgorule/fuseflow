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
 *
 * <p>Phase 7: {@link #failNext} lets a task fail a bounded number of attempts and then succeed
 * (flaky activity), and {@link #failWithType} attaches an exception class name so retry-policy
 * non-retryable classification can be exercised.
 */
class TestActivityExecutor implements ActivityExecutor {

    private final Set<String> failTasks = ConcurrentHashMap.newKeySet();
    private final ConcurrentHashMap<String, String> failTypes = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Integer> failNext = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CountDownLatch> blockLatches = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CountDownLatch> dispatchedLatches = new ConcurrentHashMap<>();

    /** The next executions of the given tasks return failure results. */
    void fail(String... taskIds) {
        failTasks.addAll(Set.of(taskIds));
    }

    /** The next executions of {@code taskId} fail with the given exception class name. */
    void failWithType(String taskId, String errorType) {
        failTasks.add(taskId);
        failTypes.put(taskId, errorType);
    }

    /** The next {@code times} executions of {@code taskId} fail, then it succeeds (flaky). */
    void failNext(String taskId, int times) {
        failNext.put(taskId, times);
    }

    /** The next execution of {@code taskId} blocks until {@link #release} is called. */
    void blockOn(String taskId) {
        blockLatches.put(taskId, new CountDownLatch(1));
    }

    /** Clears all hooks. Called between tests; per-context instances are fresh anyway. */
    void reset() {
        failTasks.clear();
        failTypes.clear();
        failNext.clear();
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
        Integer remaining = failNext.get(task.taskId());
        if (remaining != null) {
            if (remaining > 1) {
                failNext.put(task.taskId(), remaining - 1);
            } else {
                failNext.remove(task.taskId());
            }
            return failure(task, "flaky failure for " + task.taskId());
        }
        if (failTasks.contains(task.taskId())) {
            return failure(task, "simulated failure for " + task.taskId());
        }
        return ActivityResult.success(task, "{\"message\":\"ok\",\"taskId\":\"" + task.taskId() + "\"}");
    }

    private ActivityResult failure(ActivityTask task, String error) {
        String errorType = failTypes.get(task.taskId());
        return ActivityResult.failure(task, errorType, error);
    }
}
