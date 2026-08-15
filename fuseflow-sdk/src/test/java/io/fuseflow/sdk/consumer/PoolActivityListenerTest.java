package io.fuseflow.sdk.consumer;

import io.fuseflow.common.messaging.ActivityTask;
import io.fuseflow.sdk.runtime.ActivityRegistry;
import io.fuseflow.sdk.runtime.FuseFlowWorker;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

import java.util.UUID;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class PoolActivityListenerTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ActivityRegistry registry = new ActivityRegistry();
    private final FuseFlowWorker worker = mock(FuseFlowWorker.class);
    private final PoolActivityListener listener =
            new PoolActivityListener(objectMapper, registry, worker);

    private static ConsumerRecord<String, String> record(String json) {
        return new ConsumerRecord<>("fuseflow-pool.default", 0, 0L, "b", json);
    }

    @Test
    void routesSupportedActivityToWorker() throws Exception {
        registry.register("resizeImage", ctx -> "{}");
        ActivityTask task = new ActivityTask(UUID.randomUUID(), "b", "resizeImage", null, 1);

        listener.onDispatch(record(objectMapper.writeValueAsString(task)));

        verify(worker).execute(task);
    }

    @Test
    void skipsUnsupportedActivities() throws Exception {
        ActivityTask task = new ActivityTask(UUID.randomUUID(), "b", "noSuchActivity", null, 1);

        listener.onDispatch(record(objectMapper.writeValueAsString(task)));

        verifyNoInteractions(worker);
    }

    @Test
    void toleratesMalformedDispatch() {
        listener.onDispatch(record("{not json"));

        verifyNoInteractions(worker);
    }
}
