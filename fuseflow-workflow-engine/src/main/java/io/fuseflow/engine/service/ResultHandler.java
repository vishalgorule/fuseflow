package io.fuseflow.engine.service;

import io.fuseflow.engine.dispatch.ActivityResult;
import io.fuseflow.engine.metrics.EngineMetrics;
import io.fuseflow.engine.model.ActivityExecution;
import io.fuseflow.engine.model.ActivityStatus;
import io.fuseflow.engine.model.WorkflowExecution;
import io.fuseflow.engine.model.WorkflowStatus;
import io.fuseflow.engine.repository.ActivityExecutionRepository;
import io.fuseflow.engine.repository.EventStore;
import io.fuseflow.engine.repository.WorkflowExecutionRepository;
import io.fuseflow.engine.retry.RetryManager;
import io.micrometer.observation.annotation.Observed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Result handler (Phase 2 plan §4 task 5): persists activity output, appends the
 * corresponding event, fans out to dependents, and drives the execution to a terminal state.
 *
 * <p>Idempotency: results are accepted only while the activity is in-flight ({@code SCHEDULED}
 * or {@code STARTED}) <b>and</b> the result's {@code attempt} matches the row's attempt — a
 * stale redelivery from a previous retry attempt is ignored. The terminal transition itself is
 * a version-guarded conditional update. Since Phase 4 (Kafka) a worker may complete before its
 * STARTED signal is consumed, so {@code SCHEDULED} is treated as in-flight too.
 *
 * <p>Failures (Phase 7, FR-6) are routed to the {@link RetryManager}, which retries per the
 * resolved policy (task → workflow → engine defaults) or fails the activity + workflow and
 * dead-letters it when attempts are exhausted or the failure is non-retryable.
 */
@Service
public class ResultHandler {

    private static final Logger log = LoggerFactory.getLogger(ResultHandler.class);

    private final ActivityExecutionRepository activityRepository;
    private final WorkflowExecutionRepository executionRepository;
    private final EventStore eventStore;
    private final Scheduler scheduler;
    private final RetryManager retryManager;
    private final WorkflowFinalizer workflowFinalizer;
    private final EngineMetrics metrics;
    private final ObjectMapper objectMapper;

    public ResultHandler(ActivityExecutionRepository activityRepository,
                         WorkflowExecutionRepository executionRepository,
                         EventStore eventStore,
                         Scheduler scheduler,
                         RetryManager retryManager,
                         WorkflowFinalizer workflowFinalizer,
                         EngineMetrics metrics,
                         ObjectMapper objectMapper) {
        this.activityRepository = activityRepository;
        this.executionRepository = executionRepository;
        this.eventStore = eventStore;
        this.scheduler = scheduler;
        this.retryManager = retryManager;
        this.workflowFinalizer = workflowFinalizer;
        this.metrics = metrics;
        this.objectMapper = objectMapper;
    }

    @Observed(name = "fuseflow.engine.result")
    @Transactional
    public void handleResult(ActivityResult result) {
        ActivityExecution activity = activityRepository.findById(result.executionId(), result.taskId()).orElse(null);
        if (activity == null || (activity.status() != ActivityStatus.STARTED
                && activity.status() != ActivityStatus.SCHEDULED) || result.attempt() != activity.attempt()) {
            // Duplicate or stale result (re-delivery after recovery, or from a previous
            // retry attempt) — ignore.
            log.debug("Ignoring stale result for task {} of execution {} (attempt {})",
                    result.taskId(), result.executionId(), result.attempt());
            return;
        }

        // Phase 8: a cancelled (terminal) execution abandons in-flight activities — late
        // worker results must not complete them or move the execution. Paused executions keep
        // processing results (in-flight allowed to finish); only scheduling is suspended.
        WorkflowExecution execution = executionRepository.findById(result.executionId()).orElse(null);
        if (execution == null || execution.status() == WorkflowStatus.CANCELLED
                || execution.status() == WorkflowStatus.COMPLETED
                || execution.status() == WorkflowStatus.FAILED) {
            log.debug("Ignoring result for task {} of terminal execution {} ({})",
                    result.taskId(), result.executionId(), execution == null ? "?" : execution.status());
            return;
        }

        if (result.success()) {
            if (!activityRepository.markCompleted(result.executionId(), result.taskId(),
                    result.output(), activity.version())) {
                return;
            }
            eventStore.append(result.executionId(), "ActivityCompleted", completedPayload(activity, result));
            metrics.activityCompleted(execution.workflowName());
            metrics.recordActivityExecutionDuration(execution.workflowName(),
                    executionDurationMillis(activity), TimeUnit.MILLISECONDS);
            scheduler.onActivityCompleted(result.executionId(), result.taskId(), activity.dependents());
        } else {
            // Phase 7: retry per policy or fail terminally (ActivityFailed + dead-letter + workflow failed).
            // Terminal failures are counted by the RetryManager (failTerminal), so timeouts and
            // result-driven failures are both covered without double-counting retryable ones.
            retryManager.onActivityFailed(result);
            return;
        }

        // Phase 7 scale: per-execution completion counter — decremented transactionally with
        // the completion, so exactly one (the last) observes 0 and completes the execution. This
        // replaces the per-completion COUNT(*) scan with a single guarded O(1) decrement.
        if (executionRepository.decrementRemainingActivities(result.executionId()) == 0) {
            workflowFinalizer.completeWorkflow(result.executionId());
        }
    }

    private Map<String, Object> completedPayload(ActivityExecution activity, ActivityResult result) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("taskId", activity.taskId());
        payload.put("activityName", activity.activityName());
        if (result.output() != null) {
            payload.put("output", parse(result.output()));
        }
        return payload;
    }

    /** Wall-clock execution duration from the activity row's creation (SCHEDULED) to now. */
    private static long executionDurationMillis(ActivityExecution activity) {
        return Duration.between(activity.createdAt(), Instant.now()).toMillis();
    }

    private JsonNode parse(String json) {
        try {
            return objectMapper.readTree(json);
        } catch (Exception ex) {
            return objectMapper.getNodeFactory().textNode(json);
        }
    }
}
