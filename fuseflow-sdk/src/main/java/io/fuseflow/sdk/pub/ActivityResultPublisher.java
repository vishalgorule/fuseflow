package io.fuseflow.sdk.pub;

import io.fuseflow.common.correlation.CorrelationId;
import io.fuseflow.common.messaging.ActivityResultMessage;
import io.fuseflow.common.messaging.ActivityResultType;
import io.fuseflow.sdk.runtime.WorkerMetrics;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.kafka.core.KafkaTemplate;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

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
    private final WorkerMetrics metrics;
    private final ObjectProvider<Tracer> tracerProvider;
    private final ObjectProvider<Propagator> propagatorProvider;

    public ActivityResultPublisher(KafkaTemplate<String, String> kafkaTemplate,
                                   KafkaTemplate<String, String> startedKafkaTemplate,
                                   ObjectMapper objectMapper,
                                   String queue,
                                   WorkerMetrics metrics,
                                   ObjectProvider<Tracer> tracerProvider,
                                   ObjectProvider<Propagator> propagatorProvider) {
        this.kafkaTemplate = kafkaTemplate;
        this.startedKafkaTemplate = startedKafkaTemplate;
        this.objectMapper = objectMapper;
        this.queue = queue;
        this.metrics = metrics;
        this.tracerProvider = tracerProvider;
        this.propagatorProvider = propagatorProvider;
    }

    public void publish(ActivityResultMessage message) {
        long start = System.nanoTime();
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(queue, message.taskId(),
                    objectMapper.writeValueAsString(message));
            String correlationId = CorrelationId.get();
            if (correlationId != null) {
                record.headers().add(CorrelationId.HEADER, correlationId.getBytes(StandardCharsets.UTF_8));
            }
            // Phase 9: continue the trace — inject the current span's W3C traceparent so the
            // engine's result handling is part of the same workflow trace.
            injectTrace(record);
            KafkaTemplate<String, String> template = message.type() == ActivityResultType.STARTED
                    ? startedKafkaTemplate : kafkaTemplate;
            template.send(record).whenComplete((sent, ex) -> {
                metrics.recordPublishDuration(System.nanoTime() - start, TimeUnit.NANOSECONDS);
                metrics.resultPublished();
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
