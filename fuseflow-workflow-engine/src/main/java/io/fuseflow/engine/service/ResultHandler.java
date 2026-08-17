package io.fuseflow.engine.service;

import io.fuseflow.engine.dispatch.ActivityResult;
import io.fuseflow.engine.model.ActivityExecution;
import io.fuseflow.engine.model.ActivityStatus;
import io.fuseflow.engine.repository.ActivityExecutionRepository;
import io.fuseflow.engine.repository.EventStore;
import io.fuseflow.engine.repository.WorkflowExecutionRepository;
import io.fuseflow.engine.retry.RetryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

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
    private final ObjectMapper objectMapper;

    public ResultHandler(ActivityExecutionRepository activityRepository,
                         WorkflowExecutionRepository executionRepository,
                         EventStore eventStore,
                         Scheduler scheduler,
                         RetryManager retryManager,
                         WorkflowFinalizer workflowFinalizer,
                         ObjectMapper objectMapper) {
        this.activityRepository = activityRepository;
        this.executionRepository = executionRepository;
        this.eventStore = eventStore;
        this.scheduler = scheduler;
        this.retryManager = retryManager;
        this.workflowFinalizer = workflowFinalizer;
        this.objectMapper = objectMapper;
    }

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

        if (result.success()) {
            if (!activityRepository.markCompleted(result.executionId(), result.taskId(),
                    result.output(), activity.version())) {
                return;
            }
            eventStore.append(result.executionId(), "ActivityCompleted", completedPayload(activity, result));
            scheduler.onActivityCompleted(result.executionId(), result.taskId(), activity.dependents());
        } else {
            // Phase 7: retry per policy or fail terminally (ActivityFailed + dead-letter + workflow failed).
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

    private JsonNode parse(String json) {
        try {
            return objectMapper.readTree(json);
        } catch (Exception ex) {
            return objectMapper.getNodeFactory().textNode(json);
        }
    }
}
