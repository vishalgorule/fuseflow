package io.fuseflow.sdk.pub;

import io.fuseflow.common.correlation.CorrelationId;
import io.fuseflow.common.messaging.ActivityResultMessage;
import io.fuseflow.common.messaging.ActivityResultType;
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ActivityResultPublisherTest {

    private final KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ActivityResultPublisher publisher =
            new ActivityResultPublisher(kafkaTemplate, objectMapper, "activity-results");

    @BeforeEach
    void setUp() {
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenAnswer(inv -> CompletableFuture.completedFuture(null));
    }

    @Test
    void publishesResultAsJsonKeyedByTaskIdWithCorrelationHeader() throws Exception {
        ActivityTask task = new ActivityTask(UUID.randomUUID(), "b", "resizeImage", null, 1);
        CorrelationId.set("corr-123");
        try {
            publisher.publish(ActivityResultMessage.completed(task, "{\"x\":1}"));
        } finally {
            CorrelationId.clear();
        }

        ArgumentCaptor<ProducerRecord<String, String>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(captor.capture());
        ProducerRecord<String, String> record = captor.getValue();
        assertThat(record.topic()).isEqualTo("activity-results");
        assertThat(record.key()).isEqualTo("b");
        assertThat(record.headers().lastHeader(CorrelationId.HEADER)).isNotNull();

        ActivityResultMessage parsed = objectMapper.readValue(record.value(), ActivityResultMessage.class);
        assertThat(parsed.type()).isEqualTo(ActivityResultType.COMPLETED);
        assertThat(parsed.output()).isEqualTo("{\"x\":1}");
        assertThat(parsed.attempt()).isEqualTo(1);
    }
}
