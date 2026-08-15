package io.fuseflow.engine.dispatch;

import io.fuseflow.common.correlation.CorrelationId;
import io.fuseflow.common.messaging.ActivityTask;
import io.fuseflow.engine.registry.PoolRoutingTable;
import io.fuseflow.engine.repository.EventStore;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;

/**
 * Phase 5 {@link TaskDispatcher} (the default): routes each {@link ActivityTask} to the topic
 * of exactly one capable pool via the {@link PoolRoutingTable} (activity → pool topic, resolved
 * by a deterministic hash over the task id — so overlapping pools never double-execute).
 * Replaces the Phase 4 broadcast dispatcher, which published to a single topic and let every
 * worker group filter.
 *
 * <p>Delivery is at-least-once and idempotent by design: the activity is durably {@code
 * SCHEDULED} before dispatch, and the worker echoes {@code (executionId, taskId, attempt)} in
 * its result, so a re-published task after a crash or engine restart is harmless. The task id
 * key keeps per-task ordering within a partition; the correlation-ID header keeps end-to-end
 * traceability.
 *
 * <p>Unroutable tasks (no ONLINE pool advertises the activity — interim surface before Phase 7
 * retries/timeouts) stay {@code SCHEDULED} and append an {@code ActivityUnroutable} diagnostic
 * event; boot-time recovery and Phase 7 re-drive them once a capable pool appears. If Kafka is
 * unreachable the activity stays {@code SCHEDULED} and boot-time recovery re-publishes it.
 */
@Component
@ConditionalOnProperty(name = "fuseflow.engine.dispatch-mode", havingValue = "kafka", matchIfMissing = true)
public class KafkaTaskDispatcher implements TaskDispatcher {

    private static final Logger log = LoggerFactory.getLogger(KafkaTaskDispatcher.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final PoolRoutingTable routingTable;
    private final EventStore eventStore;

    public KafkaTaskDispatcher(KafkaTemplate<String, String> kafkaTemplate,
                               ObjectMapper objectMapper,
                               PoolRoutingTable routingTable,
                               EventStore eventStore) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.routingTable = routingTable;
        this.eventStore = eventStore;
    }

    @Override
    public void dispatch(ActivityTask task) {
        Optional<String> topic = routingTable.resolveTopic(task.activityName(), task.taskId());
        if (topic.isEmpty()) {
            eventStore.append(task.executionId(), "ActivityUnroutable", Map.of(
                    "taskId", task.taskId(),
                    "activityName", task.activityName(),
                    "reason", "no ONLINE pool advertises activity '" + task.activityName() + "'"));
            log.warn("Activity {} of execution {} has no routable pool — task stays SCHEDULED",
                    task.activityName(), task.executionId());
            return;
        }
        publish(task, topic.get());
    }

    private void publish(ActivityTask task, String topic) {
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
            log.debug("Dispatched activity {} of execution {} to pool topic {}", task.activityName(),
                    task.executionId(), topic);
        } catch (Exception ex) {
            // Serialization failure: the activity remains SCHEDULED; recovery re-publishes it.
            log.error("Failed to dispatch activity {} of execution {}",
                    task.activityName(), task.executionId(), ex);
        }
    }
}
