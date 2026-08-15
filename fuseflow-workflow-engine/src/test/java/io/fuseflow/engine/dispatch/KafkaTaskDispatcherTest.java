package io.fuseflow.engine.dispatch;

import io.fuseflow.common.dto.WorkerResponse;
import io.fuseflow.common.messaging.ActivityTask;
import io.fuseflow.engine.registry.PoolRoutingTable;
import io.fuseflow.engine.repository.EventStore;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import org.mockito.ArgumentMatchers;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaTaskDispatcherTest {

    private final KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final PoolRoutingTable routingTable = new PoolRoutingTable("fuseflow-pool");
    private final EventStore eventStore = mock(EventStore.class);

    private static WorkerResponse worker(String pool, String... activities) {
        Instant now = Instant.now();
        return new WorkerResponse(UUID.randomUUID(), "host", "ONLINE", List.of(activities),
                pool, 8, now, 0, now, now);
    }

    @Test
    void publishesToTheResolvedPoolTopic() throws Exception {
        routingTable.seed(List.of(worker("media", "resizeImage")));
        KafkaTaskDispatcher dispatcher =
                new KafkaTaskDispatcher(kafkaTemplate, objectMapper, routingTable, eventStore);
        when(kafkaTemplate.send(ArgumentMatchers.<ProducerRecord<String, String>>any()))
                .thenAnswer(inv -> CompletableFuture.completedFuture(null));
        ActivityTask task = new ActivityTask(UUID.randomUUID(), "b", "resizeImage", null, 1);

        dispatcher.dispatch(task);

        ArgumentCaptor<ProducerRecord<String, String>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(captor.capture());
        assertThat(captor.getValue().topic()).isEqualTo("fuseflow-pool.media");
        // The payload is the task itself (wire contract unchanged).
        assertThat(captor.getValue().value()).contains("\"activityName\":\"resizeImage\"");
        verify(eventStore, never()).append(any(), any(), any());
    }

    @Test
    void unroutableTaskStaysScheduledWithDiagnosticEvent() {
        KafkaTaskDispatcher dispatcher =
                new KafkaTaskDispatcher(kafkaTemplate, objectMapper, routingTable, eventStore);
        ActivityTask task = new ActivityTask(UUID.randomUUID(), "b", "noSuchActivity", null, 1);

        dispatcher.dispatch(task);

        verify(kafkaTemplate, never()).send(ArgumentMatchers.<ProducerRecord<String, String>>any());
        verify(eventStore).append(eq(task.executionId()), eq("ActivityUnroutable"), any());
    }
}
