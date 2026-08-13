package io.fuseflow.engine.dispatch;

import io.fuseflow.common.correlation.CorrelationId;
import io.fuseflow.common.messaging.ActivityTask;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;

/**
 * Phase 4 {@link TaskDispatcher} (the default): publishes each {@link ActivityTask} to the
 * {@code activity-dispatch} topic (keyed by task id) so SDK workers can execute it. Replaces
 * the Phase 2 in-memory dispatcher; see {@code fuseflow.engine.dispatch-mode}.
 *
 * <p>Delivery is at-least-once and idempotent by design: the activity is durably {@code
 * SCHEDULED} before dispatch, and the worker echoes {@code (executionId, taskId, attempt)} in
 * its result, so a re-published task after a crash or engine restart is harmless. The task id
 * key keeps per-task ordering within a partition; the correlation-ID header keeps end-to-end
 * traceability.
 *
 * <p>Deliberately does NOT query the registry for a capable worker (Phase 4 decision): tasks
 * are always published, and unroutable or never-executed tasks are caught by the Phase 5
 * timeout/dead-letter machinery. If Kafka is unreachable the activity stays {@code SCHEDULED}
 * in the DB and boot-time recovery re-publishes it.
 */
@Component
@ConditionalOnProperty(name = "fuseflow.engine.dispatch-mode", havingValue = "kafka", matchIfMissing = true)
public class KafkaTaskDispatcher implements TaskDispatcher {

    private static final Logger log = LoggerFactory.getLogger(KafkaTaskDispatcher.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String topic;

    public KafkaTaskDispatcher(KafkaTemplate<String, String> kafkaTemplate,
                               ObjectMapper objectMapper,
                               @Value("${fuseflow.kafka.topic.activity-dispatch}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.topic = topic;
    }

    @Override
    public void dispatch(ActivityTask task) {
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, task.taskId(),
                    objectMapper.writeValueAsString(task));
            record.headers().add(CorrelationId.HEADER,
                    CorrelationId.getOrCreate().getBytes(StandardCharsets.UTF_8));
            kafkaTemplate.send(record).whenComplete((sent, ex) -> {
                if (ex != null) {
                    log.error("Kafka dispatch failed for task {} of execution {} (stays SCHEDULED; " +
                                    "boot-time recovery re-publishes)",
                            task.taskId(), task.executionId(), ex);
                }
            });
            log.debug("Dispatched activity {} of execution {} to {}", task.activityName(), task.executionId(), topic);
        } catch (Exception ex) {
            // Serialization failure: the activity remains SCHEDULED; recovery re-publishes it.
            log.error("Failed to dispatch activity {} of execution {}",
                    task.activityName(), task.executionId(), ex);
        }
    }
}
