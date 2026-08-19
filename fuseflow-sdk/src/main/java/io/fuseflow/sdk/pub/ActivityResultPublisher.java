package io.fuseflow.sdk.pub;

import io.fuseflow.common.correlation.CorrelationId;
import io.fuseflow.common.messaging.ActivityResultMessage;
import io.fuseflow.common.messaging.ActivityResultType;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;

/**
 * Publishes the worker's activity signals ({@code STARTED} / {@code COMPLETED} / {@code FAILED})
 * to the {@code activity-results} queue. Keyed by task id (ordering within a partition) and
 * stamped with the correlation ID that travelled with the dispatch message.
 *
 * <p>Post-Phase 7 hardening — two templates, split by signal type:
 * <ul>
 *   <li><b>terminal results</b> (COMPLETED/FAILED) go through the <em>transactional</em>
 *       template: they join the pool listener's container-managed Kafka transaction and commit
 *       atomically with the offset — a worker crash mid-execution leaves the offset
 *       uncommitted, so the task is redelivered instead of stalling until the execution
 *       timeout;</li>
 *   <li><b>the eager STARTED</b> signal goes through a dedicated <em>non-transactional</em>
 *       template: it must reach the engine immediately (it arms the execution-timeout clock and
 *       stops the start timeout), not at commit time.</li>
 * </ul>
 */
public class ActivityResultPublisher {

    private static final Logger log = LoggerFactory.getLogger(ActivityResultPublisher.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final KafkaTemplate<String, String> startedKafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String queue;

    public ActivityResultPublisher(KafkaTemplate<String, String> kafkaTemplate,
                                   KafkaTemplate<String, String> startedKafkaTemplate,
                                   ObjectMapper objectMapper,
                                   String queue) {
        this.kafkaTemplate = kafkaTemplate;
        this.startedKafkaTemplate = startedKafkaTemplate;
        this.objectMapper = objectMapper;
        this.queue = queue;
    }

    public void publish(ActivityResultMessage message) {
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(queue, message.taskId(),
                    objectMapper.writeValueAsString(message));
            String correlationId = CorrelationId.get();
            if (correlationId != null) {
                record.headers().add(CorrelationId.HEADER, correlationId.getBytes(StandardCharsets.UTF_8));
            }
            KafkaTemplate<String, String> template = message.type() == ActivityResultType.STARTED
                    ? startedKafkaTemplate : kafkaTemplate;
            template.send(record).whenComplete((sent, ex) -> {
                if (ex != null) {
                    log.error("Failed to publish {} result for task {} of execution {}",
                            message.type(), message.taskId(), message.executionId(), ex);
                }
            });
        } catch (Exception ex) {
            log.error("Failed to serialize result for task {} of execution {}",
                    message.taskId(), message.executionId(), ex);
        }
    }
}
