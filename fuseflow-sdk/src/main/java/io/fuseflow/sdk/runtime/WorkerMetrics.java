package io.fuseflow.sdk.runtime;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.springframework.beans.factory.ObjectProvider;

import java.util.concurrent.TimeUnit;

/**
 * Phase 9 observability: worker-side Micrometer metrics. Registers on whatever
 * {@link MeterRegistry} the worker application provides (e.g.
 * {@code micrometer-registry-prometheus} on the sample workers) and falls back to an
 * in-memory registry that is simply never exported when no registry is present — the SDK stays
 * usable without any metrics configuration.
 *
 * <p>Metric names:
 * <ul>
 *   <li>{@code fuseflow.worker.activity.started/completed/failed} — counters</li>
 *   <li>{@code fuseflow.worker.activity.execution.duration} — timer</li>
 *   <li>{@code fuseflow.worker.result.published} — counter</li>
 *   <li>{@code fuseflow.worker.result.publish.duration} — timer</li>
 * </ul>
 */
public class WorkerMetrics {

    private final Counter activitiesStarted;
    private final Counter activitiesCompleted;
    private final Counter activitiesFailed;
    private final Timer activityExecutionTimer;
    private final Counter resultsPublished;
    private final Timer publishDurationTimer;

    public WorkerMetrics(ObjectProvider<MeterRegistry> meterRegistry) {
        MeterRegistry registry = meterRegistry.getIfAvailable();
        if (registry == null) {
            registry = new SimpleMeterRegistry();
        }
        this.activitiesStarted = Counter.builder("fuseflow.worker.activity.started")
                .description("Total number of activities started by this worker")
                .register(registry);
        this.activitiesCompleted = Counter.builder("fuseflow.worker.activity.completed")
                .description("Total number of activities completed successfully by this worker")
                .register(registry);
        this.activitiesFailed = Counter.builder("fuseflow.worker.activity.failed")
                .description("Total number of activities failed by this worker")
                .register(registry);
        this.activityExecutionTimer = Timer.builder("fuseflow.worker.activity.execution.duration")
                .description("Activity execution duration on this worker")
                .publishPercentiles(0.5, 0.75, 0.95, 0.99)
                .register(registry);
        this.resultsPublished = Counter.builder("fuseflow.worker.result.published")
                .description("Total number of activity result messages published by this worker")
                .register(registry);
        this.publishDurationTimer = Timer.builder("fuseflow.worker.result.publish.duration")
                .description("Time to publish an activity result message")
                .publishPercentiles(0.5, 0.75, 0.95, 0.99)
                .register(registry);
    }

    public void activityStarted() {
        activitiesStarted.increment();
    }

    public void activityCompleted() {
        activitiesCompleted.increment();
    }

    public void activityFailed() {
        activitiesFailed.increment();
    }

    public void recordActivityExecutionDuration(long duration, TimeUnit unit) {
        activityExecutionTimer.record(duration, unit);
    }

    public void resultPublished() {
        resultsPublished.increment();
    }

    public void recordPublishDuration(long duration, TimeUnit unit) {
        publishDurationTimer.record(duration, unit);
    }
}
