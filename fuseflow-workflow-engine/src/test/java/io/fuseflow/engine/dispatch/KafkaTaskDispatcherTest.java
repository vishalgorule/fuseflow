package io.fuseflow.engine.dispatch;

import io.fuseflow.common.dto.WorkerResponse;
import io.fuseflow.common.messaging.ActivityTask;
import io.fuseflow.engine.config.ReliabilityProperties;
import io.fuseflow.engine.registry.PoolRoutingTable;
import io.fuseflow.engine.repository.EventStore;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
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
    private final ReliabilityProperties properties = new ReliabilityProperties();

    private static WorkerResponse worker(String pool, String... activities) {
        Instant now = Instant.now();
        return new WorkerResponse(UUID.randomUUID(), "host", "ONLINE", List.of(activities),
                pool, 8, now, 0, now, now);
    }

    @Test
    void publishesToTheResolvedPoolTopic() throws Exception {
        routingTable.seed(List.of(worker("media", "resizeImage")));
        KafkaTaskDispatcher dispatcher =
                new KafkaTaskDispatcher(kafkaTemplate, objectMapper, routingTable, eventStore, properties);
        when(kafkaTemplate.send(ArgumentMatchers.<ProducerRecord<String, String>>any()))
                .thenAnswer(inv -> CompletableFuture.completedFuture(null));
        ActivityTask task = new ActivityTask(UUID.randomUUID(), "b", "resizeImage", null, 1);

        dispatcher.dispatch(task);

        ArgumentCaptor<ProducerRecord<String, String>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(captor.capture());
        assertThat(captor.getValue().topic()).isEqualTo("fuseflow-pool.media");
        // Key = executionId:taskId — spreads each execution's tasks across partitions while
        // keeping retries of the same task in the same partition.
        assertThat(captor.getValue().key()).isEqualTo(task.executionId() + ":b");
        // The payload is the task itself (wire contract unchanged), stamped with dispatch time
        // and an expiry = dispatch + the engine's start timeout (Option A stale-task guard).
        ActivityTask sent = objectMapper.readValue(captor.getValue().value(), ActivityTask.class);
        assertThat(sent.activityName()).isEqualTo("resizeImage");
        assertThat(sent.dispatchAt()).isNotNull();
        assertThat(sent.expiresAt()).isNotNull();
        assertThat(Duration.between(sent.dispatchAt(), sent.expiresAt()))
                .isEqualTo(properties.getTimeout().getStart());
        assertThat(sent.expiresAt()).isAfter(Instant.now());
        verify(eventStore, never()).append(any(), any(), any());
    }

    @Test
    void unroutableTaskStaysScheduledWithDiagnosticEvent() {
        KafkaTaskDispatcher dispatcher =
                new KafkaTaskDispatcher(kafkaTemplate, objectMapper, routingTable, eventStore, properties);
        ActivityTask task = new ActivityTask(UUID.randomUUID(), "b", "noSuchActivity", null, 1);

        dispatcher.dispatch(task);

        verify(kafkaTemplate, never()).send(ArgumentMatchers.<ProducerRecord<String, String>>any());
        verify(eventStore).append(eq(task.executionId()), eq("ActivityUnroutable"), any());
    }
}
