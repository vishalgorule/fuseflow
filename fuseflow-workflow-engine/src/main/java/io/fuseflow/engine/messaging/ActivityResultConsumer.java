package io.fuseflow.engine.messaging;

import io.fuseflow.common.correlation.CorrelationId;
import io.fuseflow.common.messaging.ActivityResultMessage;
import io.fuseflow.common.messaging.ActivityResultType;
import io.fuseflow.engine.dispatch.ActivityResult;
import io.fuseflow.engine.service.ActivityStateService;
import io.fuseflow.engine.service.ResultHandler;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;

/**
 * Consumes the {@code activity-results} topic (Phase 4): workers signal {@code STARTED}
 * (progress) and {@code COMPLETED}/{@code FAILED} (terminal) per activity. STARTED is recorded
 * via the durable SCHEDULED/STARTED → STARTED transition (emitting the {@code ActivityStarted}
 * event); terminal outcomes are fed into the {@link ResultHandler}, which ignores stale or
 * duplicate results (idempotency via {@code (executionId, taskId, attempt)}).
 *
 * <p>Error handling preserves at-least-once: malformed payloads (permanent errors) are logged
 * and acknowledged so they cannot stall a partition, while processing failures (e.g. a
 * transient database outage during {@code handleResult}) are rethrown so the container
 * redelivers the message — a result is never acknowledged before it is durably applied.
 */
@Component
@ConditionalOnProperty(name = "fuseflow.engine.dispatch-mode", havingValue = "kafka", matchIfMissing = true)
public class ActivityResultConsumer {

    private static final Logger log = LoggerFactory.getLogger(ActivityResultConsumer.class);

    private final ObjectMapper objectMapper;
    private final ActivityStateService activityState;
    private final ResultHandler resultHandler;

    public ActivityResultConsumer(ObjectMapper objectMapper,
                                  ActivityStateService activityState,
                                  ResultHandler resultHandler) {
        this.objectMapper = objectMapper;
        this.activityState = activityState;
        this.resultHandler = resultHandler;
    }

    @KafkaListener(topics = "${fuseflow.kafka.topic.activity-results}",
            groupId = "${spring.kafka.consumer.group-id:fuseflow-engine}")
    public void onResult(ConsumerRecord<String, String> record) {
        applyCorrelation(record);
        try {
            ActivityResultMessage message = parse(record.value());
            if (message == null || message.type() == null) {
                // Malformed or unusable payload — permanent error, logged and acknowledged.
                return;
            }
            switch (message.type()) {
                case STARTED -> handleStarted(message);
                case COMPLETED -> resultHandler.handleResult(
                        toResult(message, true, message.output(), null, null));
                case FAILED -> resultHandler.handleResult(
                        toResult(message, false, null, message.error(), message.errorType()));
            }
        } finally {
            CorrelationId.clear();
            MDC.remove(CorrelationId.MDC_KEY);
        }
    }

    /** Phase 7: verifies {@code message.attempt()} so a stale redelivery cannot mark a newer attempt. */
    private void handleStarted(ActivityResultMessage message) {
        if (activityState.startActivity(message.executionId(), message.taskId(), message.attempt())) {
            log.debug("Activity {} of execution {} reported STARTED (attempt {})",
                    message.taskId(), message.executionId(), message.attempt());
        }
    }

    private ActivityResultMessage parse(String value) {
        try {
            return objectMapper.readValue(value, ActivityResultMessage.class);
        } catch (Exception ex) {
            log.error("Malformed activity result '{}': {}", value, ex.getMessage());
            return null;
        }
    }

    private static ActivityResult toResult(ActivityResultMessage message, boolean success,
                                           String output, String error, String errorType) {
        return new ActivityResult(message.executionId(), message.taskId(), message.attempt(),
                success, output, error, errorType);
    }

    private void applyCorrelation(ConsumerRecord<String, String> record) {
        var header = record.headers().lastHeader(CorrelationId.HEADER);
        if (header != null) {
            String id = new String(header.value(), StandardCharsets.UTF_8);
            CorrelationId.set(id);
            MDC.put(CorrelationId.MDC_KEY, id);
        }
    }
}
