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
 * Consumes dispatch messages from the worker's <b>pool queue</b> ({@code fuseflow-pool.<pool>},
 * Phase 5). Phase 4 broadcast-and-filter is replaced by per-pool routing: the engine publishes
 * each task to exactly one pool's queue, so a worker only ever receives activities its pool
 * advertises — the capability check below is a defensive guard, not the routing mechanism.
 * The correlation ID from the dispatch header is propagated into MDC and echoed back on the
 * result.
 */
public class PoolActivityListener {

    private static final Logger log = LoggerFactory.getLogger(PoolActivityListener.class);

    private final ObjectMapper objectMapper;
    private final ActivityRegistry activityRegistry;
    private final FuseFlowWorker worker;

    public PoolActivityListener(ObjectMapper objectMapper,
                                ActivityRegistry activityRegistry,
                                FuseFlowWorker worker) {
        this.objectMapper = objectMapper;
        this.activityRegistry = activityRegistry;
        this.worker = worker;
    }

    @KafkaListener(topics = "${fuseflow.queue.pool-prefix:fuseflow-pool}.${fuseflow.worker.pool:default}",
            groupId = "${fuseflow.worker.pool:default}",
            // Phase 5: the pool's declared concurrency sizes the queue's partitions, so the
            // consumer must match — one listener thread per declared concurrency, otherwise a
            // single worker instance drains its own queue serially (5 tasks/s with the sample
            // 200ms activity delay) and bursts past the engine's start timeout.
            concurrency = "${fuseflow.worker.concurrency:1}")
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
