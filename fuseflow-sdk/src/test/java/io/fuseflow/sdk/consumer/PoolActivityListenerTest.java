package io.fuseflow.sdk.consumer;

import io.fuseflow.common.messaging.ActivityTask;
import io.fuseflow.sdk.runtime.ActivityRegistry;
import io.fuseflow.sdk.runtime.FuseFlowWorker;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class PoolActivityListenerTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ActivityRegistry registry = new ActivityRegistry();
    private final FuseFlowWorker worker = mock(FuseFlowWorker.class);
    private final WorkflowControlCache controlCache = new WorkflowControlCache(Duration.ofMinutes(10));
    private final ActivityDedupCache dedupCache = new ActivityDedupCache();
    private final PoolActivityListener listener =
            new PoolActivityListener(objectMapper, registry, worker, controlCache, dedupCache);

    private static ConsumerRecord<String, String> record(String json) {
        return new ConsumerRecord<>("fuseflow-pool.default", 0, 0L, "b", json);
    }

    private static ActivityTask task() {
        return new ActivityTask(UUID.randomUUID(), "b", "resizeImage", null, 1);
    }

    @Test
    void routesSupportedActivityToWorker() throws Exception {
        registry.register("resizeImage", ctx -> "{}");
        ActivityTask task = task();

        listener.onDispatch(record(objectMapper.writeValueAsString(task)));

        verify(worker).execute(any(ActivityTask.class), any(Runnable.class));
    }

    @Test
    void skipsUnsupportedActivities() throws Exception {
        ActivityTask task = new ActivityTask(UUID.randomUUID(), "b", "noSuchActivity", null, 1);

        listener.onDispatch(record(objectMapper.writeValueAsString(task)));

        verifyNoInteractions(worker);
    }

    @Test
    void skipsExpiredDispatch() throws Exception {
        registry.register("resizeImage", ctx -> "{}");
        // Option A: a message past its engine-stamped expiry was already timed out + retried.
        Instant past = Instant.now().minusSeconds(120);
        ActivityTask task = new ActivityTask(UUID.randomUUID(), "b", "resizeImage", null, 1,
                past, past.plusSeconds(60));

        listener.onDispatch(record(objectMapper.writeValueAsString(task)));

        verifyNoInteractions(worker);
    }

    @Test
    void executesFreshDispatchWithNullExpiry() throws Exception {
        // Messages produced before the TTL fields existed carry no timestamps — execute them
        // (backward compatibility; the engine's DB guards still protect).
        registry.register("resizeImage", ctx -> "{}");
        ActivityTask task = new ActivityTask(UUID.randomUUID(), "b", "resizeImage", null, 1,
                null, null);

        listener.onDispatch(record(objectMapper.writeValueAsString(task)));

        verify(worker).execute(any(ActivityTask.class), any(Runnable.class));
    }

    @Test
    void skipsDispatchForPausedExecution() throws Exception {
        registry.register("resizeImage", ctx -> "{}");
        ActivityTask task = task();
        controlCache.pause(task.executionId());

        listener.onDispatch(record(objectMapper.writeValueAsString(task)));

        verifyNoInteractions(worker);
    }

    @Test
    void skipsDispatchForCancelledExecution() throws Exception {
        registry.register("resizeImage", ctx -> "{}");
        ActivityTask task = task();
        controlCache.cancel(task.executionId());

        listener.onDispatch(record(objectMapper.writeValueAsString(task)));

        verifyNoInteractions(worker);
    }

    @Test
    void skipsSupersededAttemptButExecutesTheNewOne() throws Exception {
        registry.register("resizeImage", ctx -> "{}");
        UUID executionId = UUID.randomUUID();
        controlCache.supersede(executionId, "b", 1);
        ActivityTask stale = new ActivityTask(executionId, "b", "resizeImage", null, 1);
        ActivityTask current = new ActivityTask(executionId, "b", "resizeImage", null, 2);

        listener.onDispatch(record(objectMapper.writeValueAsString(stale)));
        listener.onDispatch(record(objectMapper.writeValueAsString(current)));

        verify(worker).execute(eq(current), any(Runnable.class));
        verify(worker, never()).execute(eq(stale), any(Runnable.class));
    }

    @Test
    void toleratesMalformedDispatch() {
        listener.onDispatch(record("{not json"));

        verifyNoInteractions(worker);
    }
}
