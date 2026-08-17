package io.fuseflow.engine.retry;

import io.fuseflow.engine.model.ActivityExecution;
import io.fuseflow.engine.model.ActivityStatus;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DeadLetterPublisherTest {

    private static final UUID EXECUTION = UUID.randomUUID();

    private final KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final DeadLetterPublisher publisher = new DeadLetterPublisher(kafkaTemplate, objectMapper, "dead-letter");

    private static ActivityExecution activity() {
        Instant now = Instant.now();
        return new ActivityExecution(EXECUTION, "a", "actA", ActivityStatus.FAILED, 0, List.of(), 3,
                null, "boom", null, null, 9, now, now);
    }

    @Test
    void publishesDeadLetterWithExecutionMetadata() throws Exception {
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        publisher.publish(activity(), "boom", "java.lang.IllegalStateException");

        @SuppressWarnings("unchecked")
        ArgumentCaptor<ProducerRecord<String, String>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(captor.capture());
        ProducerRecord<String, String> record = captor.getValue();
        assertThat(record.topic()).isEqualTo("dead-letter");
        assertThat(record.key()).isEqualTo("a"); // task id as partition key

        Map<String, Object> payload = objectMapper.readValue(record.value(),
                new TypeReference<Map<String, Object>>() {
                });
        assertThat(payload.get("executionId")).isEqualTo(EXECUTION.toString());
        assertThat(payload.get("taskId")).isEqualTo("a");
        assertThat(payload.get("activityName")).isEqualTo("actA");
        assertThat(payload.get("attempt")).isEqualTo(3);
        assertThat(payload.get("error")).isEqualTo("boom");
        assertThat(payload.get("errorType")).isEqualTo("java.lang.IllegalStateException");
        assertThat(payload).containsKey("deadLetteredAt");
    }

    @Test
    void omitsErrorTypeWhenAbsent() throws Exception {
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        publisher.publish(activity(), "boom", null);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<ProducerRecord<String, String>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(captor.capture());
        Map<String, Object> payload = objectMapper.readValue(captor.getValue().value(),
                new TypeReference<Map<String, Object>>() {
                });
        assertThat(payload).doesNotContainKey("errorType");
    }

    @Test
    void swallowsKafkaFailuresSoRetryPathIsNotBlocked() {
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenThrow(new RuntimeException("broker down"));

        // Must not throw — dead-lettering is best-effort.
        publisher.publish(activity(), "boom", null);
    }
}
