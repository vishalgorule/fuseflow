package io.fuseflow.registry.messaging;

import io.fuseflow.common.correlation.CorrelationId;
import io.fuseflow.common.messaging.WorkerEventMessage;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * Publishes worker state-change events to the {@code worker-events} topic (Phase 4,
 * architecture §8): {@code worker_registered}, {@code worker_deregistered}, {@code
 * worker_offline}, {@code worker_online}. Heartbeats stay REST by design (low latency, high
 * frequency) — only transitions are published, and a heartbeat that revives a
 * DEGRADED/OFFLINE worker publishes {@code worker_online} so observers (the engine's pool
 * routing table) recover liveness without waiting for a re-registration.
 *
 * <p>Publishing happens only after the surrounding transaction commits (persist → append event
 * → publish), mirroring the engine's after-commit dispatcher; when no transaction is active the
 * event is sent immediately. Gated by {@code fuseflow.registry.events-enabled} so tests and
 * non-messaging deployments can turn it off.
 */
@Component
public class WorkerEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(WorkerEventPublisher.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String topic;
    private final boolean enabled;

    public WorkerEventPublisher(KafkaTemplate<String, String> kafkaTemplate,
                                ObjectMapper objectMapper,
                                @Value("${fuseflow.kafka.topic.worker-events}") String topic,
                                @Value("${fuseflow.registry.events-enabled:true}") boolean enabled) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.topic = topic;
        this.enabled = enabled;
    }

    /** Publishes a worker event after the surrounding transaction commits (no-op if disabled). */
    public void publish(UUID workerId, String eventType, Map<String, Object> payload) {
        if (!enabled) {
            return;
        }
        WorkerEventMessage message = new WorkerEventMessage(workerId, eventType, payload, Instant.now());
        if (TransactionSynchronizationManager.isActualTransactionActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    send(message);
                }
            });
        } else {
            send(message);
        }
    }

    private void send(WorkerEventMessage message) {
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic,
                    message.workerId().toString(), objectMapper.writeValueAsString(message));
            record.headers().add(CorrelationId.HEADER,
                    CorrelationId.getOrCreate().getBytes(StandardCharsets.UTF_8));
            kafkaTemplate.send(record).whenComplete((sent, ex) -> {
                if (ex != null) {
                    log.error("Failed to publish worker event {} for worker {}",
                            message.eventType(), message.workerId(), ex);
                }
            });
        } catch (Exception ex) {
            log.error("Failed to serialize/publish worker event {} for worker {}",
                    message.eventType(), message.workerId(), ex);
        }
    }
}
