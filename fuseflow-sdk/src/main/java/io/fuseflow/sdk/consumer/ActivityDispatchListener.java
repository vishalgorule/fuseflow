package io.fuseflow.sdk.consumer;

import io.fuseflow.common.correlation.CorrelationId;
import io.fuseflow.common.messaging.ActivityTask;
import io.fuseflow.sdk.runtime.ActivityRegistry;
import io.fuseflow.sdk.runtime.FuseFlowWorker;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;

/**
 * Consumes {@code activity-dispatch} messages (Phase 4). Messages for activities this worker
 * does not support are acknowledged and skipped deliberately: with a single dispatch topic and
 * capability-based consumer groups, every group receives the full stream and filters — the
 * engine has already ensured a capable worker exists somewhere (Phase 4 decision; unroutable
 * tasks are caught by Phase 5 timeouts). The correlation ID from the dispatch header is
 * propagated into MDC and echoed back on the result.
 */
public class ActivityDispatchListener {

    private static final Logger log = LoggerFactory.getLogger(ActivityDispatchListener.class);

    private final ObjectMapper objectMapper;
    private final ActivityRegistry activityRegistry;
    private final FuseFlowWorker worker;

    public ActivityDispatchListener(ObjectMapper objectMapper,
                                    ActivityRegistry activityRegistry,
                                    FuseFlowWorker worker) {
        this.objectMapper = objectMapper;
        this.activityRegistry = activityRegistry;
        this.worker = worker;
    }

    @KafkaListener(topics = "${fuseflow.kafka.topic.activity-dispatch:activity-dispatch}",
            groupId = "${fuseflow.worker.group-id:fuseflow-workers}")
    public void onDispatch(ConsumerRecord<String, String> record) {
        applyCorrelation(record);
        try {
            ActivityTask task = objectMapper.readValue(record.value(), ActivityTask.class);
            if (!activityRegistry.supports(task.activityName())) {
                log.debug("Skipping unsupported activity {} for execution {}",
                        task.activityName(), task.executionId());
                return;
            }
            worker.execute(task);
        } catch (Exception ex) {
            log.error("Failed to process dispatch '{}': {}", record.value(), ex.getMessage(), ex);
        } finally {
            CorrelationId.clear();
            MDC.remove(CorrelationId.MDC_KEY);
        }
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
