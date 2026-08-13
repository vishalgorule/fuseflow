package io.fuseflow.engine.dispatch;

import io.fuseflow.common.correlation.CorrelationId;
import io.fuseflow.common.messaging.ActivityTask;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import tools.jackson.databind.ObjectMapper;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaTaskDispatcherTest {

    private final KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final KafkaTaskDispatcher dispatcher =
            new KafkaTaskDispatcher(kafkaTemplate, objectMapper, "activity-dispatch");

    @BeforeEach
    void setUp() {
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenAnswer(inv -> CompletableFuture.completedFuture(null));
    }

    @Test
    void publishesTaskAsJsonKeyedByTaskIdWithCorrelationHeader() throws Exception {
        ActivityTask task = new ActivityTask(UUID.randomUUID(), "b", "resizeImage", "{\"k\":1}", 1);

        dispatcher.dispatch(task);

        ArgumentCaptor<ProducerRecord<String, String>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(captor.capture());
        ProducerRecord<String, String> record = captor.getValue();
        assertThat(record.topic()).isEqualTo("activity-dispatch");
        assertThat(record.key()).isEqualTo("b");
        assertThat(objectMapper.readValue(record.value(), ActivityTask.class)).isEqualTo(task);
        assertThat(record.headers().lastHeader(CorrelationId.HEADER)).isNotNull();
    }

    @Test
    void neverThrowsWhenBrokerSendFails() {
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenAnswer(inv -> {
            CompletableFuture<Void> failed = new CompletableFuture<>();
            failed.completeExceptionally(new RuntimeException("broker down"));
            return failed;
        });

        assertThatCode(() -> dispatcher.dispatch(
                new ActivityTask(UUID.randomUUID(), "a", "actA", null, 1)))
                .doesNotThrowAnyException();
    }
}
