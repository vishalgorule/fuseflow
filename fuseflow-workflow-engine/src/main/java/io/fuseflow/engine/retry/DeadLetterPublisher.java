package io.fuseflow.engine.retry;

import io.fuseflow.common.correlation.CorrelationId;
import io.fuseflow.engine.model.ActivityExecution;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Publishes terminal activity failures to the {@code dead-letter} topic (Phase 7, plan §9 task
 * 6): every activity whose retries are exhausted or whose failure is non-retryable lands here
 * for inspection via {@code GET /api/v1/dead-letters}. Kafka-only — in the in-memory dispatch
 * mode there is no broker and the bean is absent (the {@link RetryManager} tolerates that via
 * {@code ObjectProvider}).
 */
@Component
@ConditionalOnProperty(name = "fuseflow.engine.dispatch-mode", havingValue = "kafka", matchIfMissing = true)
public class DeadLetterPublisher {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterPublisher.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String topic;

    public DeadLetterPublisher(KafkaTemplate<String, String> kafkaTemplate,
                               ObjectMapper objectMapper,
                               @Value("${fuseflow.kafka.topic.dead-letter}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.topic = topic;
    }

    public void publish(ActivityExecution activity, String error, String errorType) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("executionId", activity.workflowExecutionId().toString());
        payload.put("taskId", activity.taskId());
        payload.put("activityName", activity.activityName());
        payload.put("attempt", activity.attempt());
        payload.put("error", error);
        if (errorType != null) {
            payload.put("errorType", errorType);
        }
        payload.put("deadLetteredAt", Instant.now().toString());
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, activity.taskId(),
                    objectMapper.writeValueAsString(payload));
            record.headers().add(CorrelationId.HEADER,
                    CorrelationId.getOrCreate().getBytes(StandardCharsets.UTF_8));
            kafkaTemplate.send(record);
            log.warn("Dead-lettered activity {} of execution {} (attempt {}, error type {})",
                    activity.taskId(), activity.workflowExecutionId(), activity.attempt(), errorType);
        } catch (Exception ex) {
            log.error("Failed to publish dead-letter for task {} of execution {}",
                    activity.taskId(), activity.workflowExecutionId(), ex);
        }
    }
}
