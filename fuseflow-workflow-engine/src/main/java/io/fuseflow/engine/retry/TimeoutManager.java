package io.fuseflow.engine.retry;

import io.fuseflow.engine.config.ReliabilityProperties;
import io.fuseflow.engine.dispatch.ActivityResult;
import io.fuseflow.engine.metrics.EngineMetrics;
import io.fuseflow.engine.model.ActivityExecution;
import io.fuseflow.engine.model.WorkflowExecution;
import io.fuseflow.engine.model.WorkflowStatus;
import io.fuseflow.engine.repository.ActivityExecutionRepository;
import io.fuseflow.engine.repository.WorkflowExecutionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Timeout manager (Phase 7, FR-7): converts activities that never make progress into failed
 * attempts, which the {@link RetryManager} then retries (or fails) per policy.
 *
 * <ul>
 *   <li><b>start timeout</b> — a SCHEDULED activity that never received a STARTED signal
 *       within {@code fuseflow.engine.timeout.start} (covers unroutable tasks and dispatches
 *       nobody picked up);</li>
 *   <li><b>execution timeout</b> — a STARTED activity that produced no result within
 *       {@code fuseflow.engine.timeout.execution} (covers hung or dead workers; the registry's
 *       heartbeat detection already routed their pool OFFLINE, so the retry lands on a live
 *       worker).</li>
 * </ul>
 *
 * <p>Retry-waiting rows (SCHEDULED with a future {@code retry_due_at}) are on the retry clock,
 * not the start clock, and are excluded — the {@link RetryScheduler} owns them.
 */
@Component
public class TimeoutManager {

    private static final Logger log = LoggerFactory.getLogger(TimeoutManager.class);

    private final ActivityExecutionRepository activityRepository;
    private final WorkflowExecutionRepository executionRepository;
    private final RetryManager retryManager;
    private final ReliabilityProperties properties;
    private final EngineMetrics metrics;

    public TimeoutManager(ActivityExecutionRepository activityRepository,
                          WorkflowExecutionRepository executionRepository,
                          RetryManager retryManager,
                          ReliabilityProperties properties,
                          EngineMetrics metrics) {
        this.activityRepository = activityRepository;
        this.executionRepository = executionRepository;
        this.retryManager = retryManager;
        this.properties = properties;
        this.metrics = metrics;
    }

    @Scheduled(fixedDelayString = "${fuseflow.engine.poll-interval:5s}")
    public void checkTimeouts() {
        Duration startTimeout = properties.getTimeout().getStart();
        Instant startCutoff = Instant.now().minus(startTimeout);
        // Phase 8: paused/terminal executions are exempt from timeouts — a paused execution's
        // in-flight activities must not be killed by the clock; resume re-drives them.
        List<Candidate> startTimeouts = runningOnly(activityRepository.findStartTimeouts(startCutoff));
        for (Candidate candidate : startTimeouts) {
            ActivityExecution activity = candidate.activity();
            log.warn("Activity {} of execution {} never started within {} — treating as failed attempt {}",
                    activity.taskId(), activity.workflowExecutionId(), startTimeout, activity.attempt());
            metrics.activityTimedOut(candidate.workflowName(), "start");
            retryManager.onActivityFailed(failure(activity, "start timeout after " + startTimeout.toSeconds() + "s"));
        }

        Duration executionTimeout = properties.getTimeout().getExecution();
        Instant executionCutoff = Instant.now().minus(executionTimeout);
        List<Candidate> executionTimeouts = runningOnly(activityRepository.findExecutionTimeouts(executionCutoff));
        for (Candidate candidate : executionTimeouts) {
            ActivityExecution activity = candidate.activity();
            log.warn("Activity {} of execution {} produced no result within {} — treating as failed attempt {}",
                    activity.taskId(), activity.workflowExecutionId(), executionTimeout, activity.attempt());
            metrics.activityTimedOut(candidate.workflowName(), "execution");
            retryManager.onActivityFailed(failure(activity, "execution timeout after " + executionTimeout.toSeconds() + "s"));
        }
    }

    /** Filters timeout candidates to executions still RUNNING (one batched status read). */
    private List<Candidate> runningOnly(List<ActivityExecution> candidates) {
        if (candidates.isEmpty()) {
            return candidates.stream().map(a -> new Candidate(a, "unknown")).toList();
        }
        List<UUID> executionIds = candidates.stream()
                .map(ActivityExecution::workflowExecutionId)
                .distinct()
                .toList();
        Map<UUID, WorkflowExecution> executions = executionRepository.findByIds(executionIds).stream()
                .collect(Collectors.toMap(WorkflowExecution::id, Function.identity()));
        return candidates.stream()
                .map(activity -> {
                    WorkflowExecution execution = executions.get(activity.workflowExecutionId());
                    if (execution == null || execution.status() != WorkflowStatus.RUNNING) {
                        return null;
                    }
                    return new Candidate(activity, execution.workflowName());
                })
                .filter(java.util.Objects::nonNull)
                .toList();
    }

    /** A timeout candidate carrying the workflow name for the metrics tag. */
    private record Candidate(ActivityExecution activity, String workflowName) {
    }

    private static ActivityResult failure(ActivityExecution activity, String error) {
        return new ActivityResult(activity.workflowExecutionId(), activity.taskId(), activity.attempt(),
                false, null, error, null);
    }
}
