package io.fuseflow.engine.messaging;

import io.fuseflow.common.correlation.CorrelationId;
import io.fuseflow.common.messaging.WorkflowEventMessage;
import io.fuseflow.engine.dispatch.AfterCommitDispatcher;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * Publishes workflow lifecycle events to the {@code workflow-events} topic (Phase 4,
 * architecture §8) as an asynchronous mirror of the event-sourced table. Only lifecycle events
 * are published ({@code WorkflowStarted/Completed/Failed}; {@code Paused/Resumed/Cancelled}
 * arrive with Phase 6); per-activity events stay queryable via the engine's history API.
 *
 * <p>Publishing happens after the surrounding transaction commits (persist → append event
 * → publish, architecture §10.1).
 */
@Component
public class WorkflowEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(WorkflowEventPublisher.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final AfterCommitDispatcher afterCommit;
    private final String topic;

    public WorkflowEventPublisher(KafkaTemplate<String, String> kafkaTemplate,
                                  ObjectMapper objectMapper,
                                  AfterCommitDispatcher afterCommit,
                                  @Value("${fuseflow.kafka.topic.workflow-events}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.afterCommit = afterCommit;
        this.topic = topic;
    }

    /** Mirrors a lifecycle event to the topic after the surrounding transaction commits. */
    public void publish(UUID executionId, String eventType, Map<String, Object> payload) {
        WorkflowEventMessage message = new WorkflowEventMessage(executionId, eventType, payload, Instant.now());
        afterCommit.runAfterCommit(() -> send(message));
    }

    private void send(WorkflowEventMessage message) {
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic,
                    message.executionId().toString(), objectMapper.writeValueAsString(message));
            record.headers().add(CorrelationId.HEADER,
                    CorrelationId.getOrCreate().getBytes(StandardCharsets.UTF_8));
            kafkaTemplate.send(record).whenComplete((sent, ex) -> {
                if (ex != null) {
                    log.error("Failed to publish {} event for execution {} (Postgres remains the " +
                                    "source of truth)",
                            message.eventType(), message.executionId(), ex);
                }
            });
        } catch (Exception ex) {
            log.error("Failed to serialize/publish {} event for execution {}",
                    message.eventType(), message.executionId(), ex);
        }
    }
}
