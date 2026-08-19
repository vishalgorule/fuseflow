package io.fuseflow.engine.messaging;

import io.fuseflow.common.messaging.WorkflowEventMessage;
import io.fuseflow.engine.dispatch.AfterCommitDispatcher;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import tools.jackson.databind.ObjectMapper;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class WorkflowEventPublisherTest {

    private final KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenAnswer(inv -> CompletableFuture.completedFuture(null));
    }

    @Test
    void publishesLifecycleEventAfterCommit() throws Exception {
        // No transaction in a unit test → the after-commit action runs immediately.
        WorkflowEventPublisher publisher = new WorkflowEventPublisher(kafkaTemplate, objectMapper,
                new AfterCommitDispatcher(), "workflow-events");
        UUID executionId = UUID.randomUUID();

        publisher.publish(executionId, "WorkflowStarted", Map.of("workflowName", "wf"));

        ArgumentCaptor<ProducerRecord<String, String>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(captor.capture());
        ProducerRecord<String, String> record = captor.getValue();
        assertThat(record.topic()).isEqualTo("workflow-events");
        assertThat(record.key()).isEqualTo(executionId.toString());
        WorkflowEventMessage message = objectMapper.readValue(record.value(), WorkflowEventMessage.class);
        assertThat(message.eventType()).isEqualTo("WorkflowStarted");
        assertThat(message.executionId()).isEqualTo(executionId);
        assertThat(message.payload()).containsEntry("workflowName", "wf");
    }

}
