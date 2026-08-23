package io.fuseflow.engine.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import io.fuseflow.engine.model.WorkflowStatus;
import io.fuseflow.engine.repository.WorkflowExecutionRepository;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Phase 9 observability: custom Micrometer metrics for the workflow engine.
 *
 * <p>Provides counters, timers, and gauges for:
 * <ul>
 *   <li>Workflow executions (started, completed, failed, cancelled, paused, resumed)</li>
 *   <li>Activity lifecycle (scheduled, completed, failed, retried, timed out, unroutable,
 *       dead-lettered)</li>
 *   <li>Scheduling / dispatch / activity / workflow durations</li>
 *   <li>Dispatch outcomes (attempted, succeeded, failed)</li>
 *   <li>Outbox queue depth + max pending age, and active worker fleet size</li>
 *   <li>DB-backed gauges: running executions and total executions</li>
 * </ul>
 *
 * <p><b>Workflow-name dimension.</b> Workflows are user-defined, so the workflow/activity
 * lifecycle counters, timers and summaries carry a {@code workflowName} tag — the platform's
 * contract to users for monitoring their own workflows. The registry reuses meters by
 * name+tags, so cardinality is bounded by the (small) number of registered workflows.
 * Dispatch counters/timers and the fleet/outbox gauges are platform-internal and stay
 * aggregate.
 *
 * <p><b>Timeout type.</b> {@code fuseflow.activity.timed_out} is additionally tagged with
 * {@code type=start|execution} so the platform operator can tell a dispatch/routing problem
 * from a hung-worker problem.
 *
 * <p>DB connection pool health is intentionally left to Micrometer's built-in
 * {@code hikaricp_*} metrics (auto-registered by Spring Boot when micrometer is on the
 * classpath) — see the dashboards/alerts, which query those names.
 */
@Component
public class EngineMetrics {

    private static final Logger log = LoggerFactory.getLogger(EngineMetrics.class);

    private static final String TAG_WORKFLOW = "workflowName";
    private static final String UNKNOWN_WORKFLOW = "unknown";

    private final MeterRegistry meterRegistry;

    // Dispatch counters (platform-internal — aggregate, no workflowName tag)
    private final Counter dispatchesAttempted;
    private final Counter dispatchesSucceeded;
    private final Counter dispatchesFailed;

    // Dispatch timer (platform-internal)
    private final Timer dispatchTimer;

    // Gauges (use AtomicInteger/AtomicLong for thread-safe set)
    private final AtomicInteger runningWorkflows = new AtomicInteger(0);
    private final AtomicInteger pendingOutboxEntries = new AtomicInteger(0);
    private final AtomicLong maxPendingOutboxAgeSeconds = new AtomicLong(0);
    private final AtomicInteger activeWorkers = new AtomicInteger(0);
    private final AtomicInteger totalExecutions = new AtomicInteger(0);

    // DB-backed running workflow gauge
    private final WorkflowExecutionRepository executionRepository;

    public EngineMetrics(MeterRegistry meterRegistry,
                         WorkflowExecutionRepository executionRepository) {
        this.meterRegistry = meterRegistry;
        this.executionRepository = executionRepository;

        // Dispatch counters (aggregate)
        this.dispatchesAttempted = Counter.builder("fuseflow.dispatch.attempted")
                .description("Total number of dispatch attempts")
                .register(meterRegistry);
        this.dispatchesSucceeded = Counter.builder("fuseflow.dispatch.succeeded")
                .description("Total number of successful dispatches")
                .register(meterRegistry);
        this.dispatchesFailed = Counter.builder("fuseflow.dispatch.failed")
                .description("Total number of failed dispatches")
                .register(meterRegistry);

        this.dispatchTimer = Timer.builder("fuseflow.dispatch.duration")
                .description("Time to dispatch an activity to Kafka")
                .publishPercentiles(0.5, 0.75, 0.95, 0.99)
                .register(meterRegistry);

        // Register core gauges
        Gauge.builder("fuseflow.workflow.running", runningWorkflows, AtomicInteger::doubleValue)
                .description("Number of currently running workflows (DB-backed)")
                .register(meterRegistry);
        Gauge.builder("fuseflow.outbox.pending", pendingOutboxEntries, AtomicInteger::doubleValue)
                .description("Number of pending dispatch outbox entries")
                .register(meterRegistry);
        Gauge.builder("fuseflow.outbox.max_pending_age_seconds", maxPendingOutboxAgeSeconds, AtomicLong::doubleValue)
                .description("Age in seconds of the oldest pending dispatch outbox entry (stuck-dispatch detection)")
                .register(meterRegistry);
        Gauge.builder("fuseflow.workers.active", activeWorkers, AtomicInteger::doubleValue)
                .description("Number of active workers (from the pool routing table)")
                .register(meterRegistry);
        Gauge.builder("fuseflow.workflow.executions", totalExecutions, AtomicInteger::doubleValue)
                .description("Total number of workflow executions in the DB (DB-backed)")
                .register(meterRegistry);

        syncCounts();
    }

    // ---------------------------------------------------------------- per-workflow meter lookup

    /** Tagged counter — meters are reused by the registry on (name, workflowName). */
    private Counter workflowCounter(String name, String workflowName) {
        return meterRegistry.counter(name, TAG_WORKFLOW, wf(workflowName));
    }

    /** Tagged timer — publishes the p50/p75/p95/p99 quantiles the dashboards query. */
    private Timer workflowTimer(String name, String workflowName) {
        return Timer.builder(name)
                .publishPercentiles(0.5, 0.75, 0.95, 0.99)
                .tag(TAG_WORKFLOW, wf(workflowName))
                .register(meterRegistry);
    }

    /** Tagged distribution summary. */
    private DistributionSummary workflowSummary(String name, String workflowName) {
        return meterRegistry.summary(name, TAG_WORKFLOW, wf(workflowName));
    }

    private static String wf(String workflowName) {
        return workflowName == null || workflowName.isBlank() ? UNKNOWN_WORKFLOW : workflowName;
    }

    // ---------------------------------------------------------------- workflow lifecycle

    public void workflowStarted(String workflowName) {
        workflowCounter("fuseflow.workflow.started", workflowName).increment();
        syncCounts();
    }

    public void workflowCompleted(String workflowName) {
        workflowCounter("fuseflow.workflow.completed", workflowName).increment();
        syncCounts();
    }

    public void workflowFailed(String workflowName) {
        workflowCounter("fuseflow.workflow.failed", workflowName).increment();
        syncCounts();
    }

    public void workflowCancelled(String workflowName) {
        workflowCounter("fuseflow.workflow.cancelled", workflowName).increment();
        syncCounts();
    }

    public void workflowPaused(String workflowName) {
        workflowCounter("fuseflow.workflow.paused", workflowName).increment();
    }

    public void workflowResumed(String workflowName) {
        workflowCounter("fuseflow.workflow.resumed", workflowName).increment();
    }

    /**
     * Refreshes the DB-backed gauges from the source of truth. Called on every lifecycle event
     * AND on a 5s schedule for self-correction. Both gauges are read from the shared DB, so in
     * HA (two engine instances) each instance reports the same truth — dashboards/alerts must
     * use {@code max by (job)} rather than sum.
     */
    @Scheduled(fixedDelayString = "5s")
    public void syncCounts() {
        try {
            runningWorkflows.set(executionRepository.countByStatus(WorkflowStatus.RUNNING));
            totalExecutions.set(executionRepository.countAll());
        } catch (Exception e) {
            log.warn("Failed to sync execution gauges from DB: {}", e.getMessage());
        }
    }

    // ---------------------------------------------------------------- activity lifecycle

    public void activityScheduled(String workflowName) {
        workflowCounter("fuseflow.activity.scheduled", workflowName).increment();
    }

    public void activityCompleted(String workflowName) {
        workflowCounter("fuseflow.activity.completed", workflowName).increment();
    }

    public void activityFailed(String workflowName) {
        workflowCounter("fuseflow.activity.failed", workflowName).increment();
    }

    public void activityRetried(String workflowName) {
        workflowCounter("fuseflow.activity.retried", workflowName).increment();
    }

    /** {@code type} is {@code start} (never got a STARTED signal) or {@code execution} (hung). */
    public void activityTimedOut(String workflowName, String type) {
        meterRegistry.counter("fuseflow.activity.timed_out", TAG_WORKFLOW, wf(workflowName), "type", type)
                .increment();
    }

    public void activityUnroutable(String workflowName) {
        workflowCounter("fuseflow.activity.unroutable", workflowName).increment();
    }

    public void activityDeadLettered(String workflowName) {
        workflowCounter("fuseflow.activity.dead_lettered", workflowName).increment();
    }

    // ---------------------------------------------------------------- dispatch (aggregate)

    public void dispatchAttempted() {
        dispatchesAttempted.increment();
    }

    public void dispatchSucceeded() {
        dispatchesSucceeded.increment();
    }

    public void dispatchFailed() {
        dispatchesFailed.increment();
    }

    public void recordDispatchDuration(long duration, TimeUnit unit) {
        dispatchTimer.record(duration, unit);
    }

    // ---------------------------------------------------------------- timers

    public void recordSchedulingDuration(String workflowName, long duration, TimeUnit unit) {
        workflowTimer("fuseflow.scheduling.duration", workflowName).record(duration, unit);
    }

    public void recordActivityExecutionDuration(String workflowName, long duration, TimeUnit unit) {
        workflowTimer("fuseflow.activity.execution.duration", workflowName).record(duration, unit);
    }

    public void recordWorkflowDuration(String workflowName, long duration, TimeUnit unit) {
        workflowTimer("fuseflow.workflow.duration", workflowName).record(duration, unit);
    }

    // ---------------------------------------------------------------- distribution summaries

    public void recordWorkflowActivityCount(String workflowName, int count) {
        workflowSummary("fuseflow.workflow.activity.count", workflowName).record(count);
    }

    public void recordRetryAttempts(String workflowName, int attempts) {
        workflowSummary("fuseflow.activity.retry.attempts", workflowName).record(attempts);
    }

    // ---------------------------------------------------------------- gauges

    public void setPendingOutboxEntries(int count) {
        pendingOutboxEntries.set(count);
    }

    public void setMaxPendingOutboxAgeSeconds(long ageSeconds) {
        maxPendingOutboxAgeSeconds.set(ageSeconds);
    }

    public void setActiveWorkers(int count) {
        activeWorkers.set(count);
    }

    public int getRunningWorkflows() {
        return runningWorkflows.get();
    }
}
