package io.fuseflow.sdk.consumer;

import io.fuseflow.common.messaging.WorkflowEventMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import tools.jackson.databind.ObjectMapper;

import java.util.Map;
import java.util.UUID;

/**
 * Consumes the engine's {@code workflow-events} topic (Option B) and turns lifecycle signals
 * into {@link WorkflowControlCache} state: {@code WorkflowPaused}/{@code WorkflowCancelled}
 * block the execution (its queued dispatch messages are skipped), {@code WorkflowResumed}
 * unblocks it, {@code WorkflowCompleted}/{@code WorkflowFailed} drop the state, and
 * {@code ActivitySuperseded} marks a retried attempt as stale. The engine publishes all of
 * these today; this consumer is the worker's half of the "no activity executes after
 * pause/cancel/retry" contract.
 *
 * <p>One consumer group <b>per worker instance</b> (unique group id), so every worker sees
 * every event and keeps a full replica of the control state — a shared group would split
 * events across workers and leave some of them executing blocked tasks. Best-effort by design:
 * a worker that was down when the signal was published misses it until the next event, and the
 * engine's DB guards remain the source of truth.
 */
public class ControlEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(ControlEventConsumer.class);

    private final ObjectMapper objectMapper;
    private final WorkflowControlCache cache;

    public ControlEventConsumer(ObjectMapper objectMapper, WorkflowControlCache cache) {
        this.objectMapper = objectMapper;
        this.cache = cache;
    }

    @KafkaListener(topics = "${fuseflow.worker.control-topic:workflow-events}",
            groupId = "fuseflow-worker-control-${random.uuid}")
    public void onEvent(ConsumerRecord<String, String> record) {
        try {
            WorkflowEventMessage event = objectMapper.readValue(record.value(), WorkflowEventMessage.class);
            apply(event);
        } catch (Exception ex) {
            log.error("Failed to process control event '{}': {}", record.value(), ex.getMessage(), ex);
        }
    }

    private void apply(WorkflowEventMessage event) {
        UUID executionId = event.executionId();
        if (executionId == null) {
            return;
        }
        switch (event.eventType()) {
            case "WorkflowPaused" -> {
                cache.pause(executionId);
                log.info("Control: execution {} paused — skipping its queued tasks", executionId);
            }
            case "WorkflowResumed" -> {
                cache.resume(executionId);
                log.info("Control: execution {} resumed — queued tasks may execute again", executionId);
            }
            case "WorkflowCancelled" -> {
                cache.cancel(executionId);
                log.info("Control: execution {} cancelled — skipping its queued tasks", executionId);
            }
            case "WorkflowCompleted", "WorkflowFailed" -> cache.clear(executionId);
            case "ActivitySuperseded" -> {
                Map<String, Object> payload = event.payload() == null ? Map.of() : event.payload();
                Object taskId = payload.get("taskId");
                Object supersededAttempt = payload.get("supersededAttempt");
                if (taskId != null && supersededAttempt instanceof Number attempt) {
                    cache.supersede(executionId, String.valueOf(taskId), attempt.intValue());
                    log.debug("Control: attempt {} of task {} (execution {}) superseded by a retry",
                            attempt.intValue(), taskId, executionId);
                }
            }
            default -> {
                // WorkflowStarted and any future events are irrelevant to execution skipping.
            }
        }
    }
}
