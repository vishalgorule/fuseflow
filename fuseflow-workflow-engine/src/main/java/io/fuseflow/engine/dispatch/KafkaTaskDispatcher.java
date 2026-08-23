package io.fuseflow.engine.dispatch;

import io.fuseflow.common.correlation.CorrelationId;
import io.fuseflow.common.messaging.ActivityTask;
import io.fuseflow.engine.config.ReliabilityProperties;
import io.fuseflow.engine.metrics.EngineMetrics;
import io.fuseflow.engine.registry.PoolRoutingTable;
import io.fuseflow.engine.repository.EventStore;
import io.micrometer.observation.annotation.Observed;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Phase 5 {@link TaskDispatcher} (the default): routes each {@link ActivityTask} to the topic
 * of exactly one capable pool via the {@link PoolRoutingTable} (activity → pool topic, resolved
 * by a deterministic hash over the task id — so overlapping pools never double-execute).
 * Replaces the Phase 4 broadcast dispatcher, which published to a single topic and let every
 * worker group filter.
 *
 * <p>Delivery is at-least-once and idempotent by design: the activity is durably {@code
 * SCHEDULED} before dispatch, and the worker echoes {@code (executionId, taskId, attempt)} in
 * its result, so a re-published task after a crash or engine restart is harmless. The record
 * key is {@code executionId:taskId}: it keeps retries of the same task in the same partition
 * (per-task ordering) while spreading each execution's tasks across the pool topic's
 * partitions — keying by task id alone would hash the few distinct task ids onto a subset of
 * the partitions (e.g. 5 task ids over 8 partitions leaves 3 idle). The correlation-ID header
 * keeps end-to-end traceability. Each message is stamped with {@code dispatchAt} and
 * {@code expiresAt} (= dispatch + the engine's start timeout): a worker skips a message past
 * its expiry — the engine already timed it out and superseded it with a retry (Option A
 * stale-task guard), so executing it would be wasted work whose result the engine drops.
 *
 * <p>Unroutable tasks (no ONLINE pool advertises the activity — interim surface before Phase 7
 * retries/timeouts) stay {@code SCHEDULED} and append an {@code ActivityUnroutable} diagnostic
 * event; boot-time recovery and Phase 7 re-drive them once a capable pool appears. If Kafka is
 * unreachable the activity stays {@code SCHEDULED} and boot-time recovery re-publishes it.
 */
@Component
public class KafkaTaskDispatcher implements TaskDispatcher {

    private static final Logger log = LoggerFactory.getLogger(KafkaTaskDispatcher.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final PoolRoutingTable routingTable;
    private final EventStore eventStore;
    private final ReliabilityProperties properties;
    private final EngineMetrics metrics;
    private final ObjectProvider<Tracer> tracerProvider;
    private final ObjectProvider<Propagator> propagatorProvider;

    public KafkaTaskDispatcher(KafkaTemplate<String, String> kafkaTemplate,
                               ObjectMapper objectMapper,
                               PoolRoutingTable routingTable,
                               EventStore eventStore,
                               ReliabilityProperties properties,
                               EngineMetrics metrics,
                               ObjectProvider<Tracer> tracerProvider,
                               ObjectProvider<Propagator> propagatorProvider) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.routingTable = routingTable;
        this.eventStore = eventStore;
        this.properties = properties;
        this.metrics = metrics;
        this.tracerProvider = tracerProvider;
        this.propagatorProvider = propagatorProvider;
    }

    @Observed(name = "fuseflow.engine.dispatch")
    @Override
    public void dispatch(ActivityTask task) {
        metrics.dispatchAttempted();
        long start = System.nanoTime();
        Optional<String> topic = routingTable.resolveTopic(task.activityName(), task.taskId());
        if (topic.isEmpty()) {
            eventStore.append(task.executionId(), "ActivityUnroutable", Map.of(
                    "taskId", task.taskId(),
                    "activityName", task.activityName(),
                    "reason", "no ONLINE pool advertises activity '" + task.activityName() + "'"));
            // Defensive fallback (routing changed between outbox insert and dispatch): the task
            // wire carries no workflow name, so the metric falls back to the unknown tag.
            metrics.activityUnroutable(null);
            log.warn("Activity {} of execution {} has no routable pool — task stays SCHEDULED",
                    task.activityName(), task.executionId());
            return;
        }
        publish(task, topic.get());
        metrics.recordDispatchDuration(System.nanoTime() - start, TimeUnit.NANOSECONDS);
    }

    private void publish(ActivityTask task, String topic) {
        try {
            // Option A stale-task guard: stamp dispatch time + expiry (= engine start timeout) so
            // a worker can skip a message the engine already timed out and superseded with a retry.
            Instant now = Instant.now();
            ActivityTask stamped = new ActivityTask(task.executionId(), task.taskId(), task.activityName(),
                    task.input(), task.attempt(), now,
                    now.plus(properties.getTimeout().getStart()));
            ProducerRecord<String, String> record = new ProducerRecord<>(topic,
                    task.executionId() + ":" + task.taskId(),
                    objectMapper.writeValueAsString(stamped));
            record.headers().add(CorrelationId.HEADER,
                    CorrelationId.getOrCreate().getBytes(StandardCharsets.UTF_8));
            // Phase 9: propagate the current trace context (W3C traceparent) so the worker's
            // execution span continues the engine's dispatch trace (at-least-once re-delivery
            // is harmless: the worker's span reuses the incoming trace id).
            injectTrace(record);
            kafkaTemplate.send(record).whenComplete((sent, ex) -> {
                if (ex != null) {
                    metrics.dispatchFailed();
                    log.error("Kafka dispatch failed for task {} of execution {} (stays SCHEDULED; " +
                                    "boot-time recovery re-publishes)",
                            task.taskId(), task.executionId(), ex);
                } else {
                    metrics.dispatchSucceeded();
                }
            });
            log.debug("Dispatched activity {} of execution {} to pool topic {}", task.activityName(),
                    task.executionId(), topic);
        } catch (Exception ex) {
            // Serialization failure: the activity remains SCHEDULED; recovery re-publishes it.
            metrics.dispatchFailed();
            log.error("Failed to dispatch activity {} of execution {}",
                    task.activityName(), task.executionId(), ex);
        }
    }

    /** Injects the W3C {@code traceparent} header from the current span (no-op without tracing). */
    private void injectTrace(ProducerRecord<String, String> record) {
        Tracer tracer = tracerProvider.getIfAvailable();
        Propagator propagator = propagatorProvider.getIfAvailable();
        if (tracer == null || propagator == null) {
            return;
        }
        Span current = tracer.currentSpan();
        if (current == null) {
            return;
        }
        propagator.inject(current.context(), record.headers(),
                (headers, key, value) -> headers.add(key, value.getBytes(StandardCharsets.UTF_8)));
    }
}
