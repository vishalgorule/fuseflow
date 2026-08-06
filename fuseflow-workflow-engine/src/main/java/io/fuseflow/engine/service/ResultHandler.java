package io.fuseflow.engine.service;

import io.fuseflow.engine.dispatch.ActivityResult;
import io.fuseflow.engine.model.ActivityExecution;
import io.fuseflow.engine.model.ActivityStatus;
import io.fuseflow.engine.model.WorkflowExecution;
import io.fuseflow.engine.repository.ActivityExecutionRepository;
import io.fuseflow.engine.repository.EventStore;
import io.fuseflow.engine.repository.WorkflowExecutionRepository;
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
 * Result handler (Phase 2 plan §4 task 5): persists activity output/failure, appends the
 * corresponding event, fans out to dependents, and drives the execution to a terminal state.
 *
 * <p>Idempotency: results are accepted only when the activity is currently STARTED, and the
 * terminal transition itself is a version-guarded conditional update — duplicate or stale
 * results are ignored. Phase 2 failure policy is minimal: one failed activity fails the
 * whole workflow (retries arrive in Phase 5).
 */
@Service
public class ResultHandler {

    private static final Logger log = LoggerFactory.getLogger(ResultHandler.class);

    private final ActivityExecutionRepository activityRepository;
    private final WorkflowExecutionRepository executionRepository;
    private final EventStore eventStore;
    private final Scheduler scheduler;
    private final ObjectMapper objectMapper;

    public ResultHandler(ActivityExecutionRepository activityRepository,
                         WorkflowExecutionRepository executionRepository,
                         EventStore eventStore,
                         Scheduler scheduler,
                         ObjectMapper objectMapper) {
        this.activityRepository = activityRepository;
        this.executionRepository = executionRepository;
        this.eventStore = eventStore;
        this.scheduler = scheduler;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public void handleResult(ActivityResult result) {
        ActivityExecution activity = activityRepository.findById(result.executionId(), result.taskId()).orElse(null);
        if (activity == null || activity.status() != ActivityStatus.STARTED) {
            // Duplicate or stale result (e.g. re-delivery after recovery) — ignore.
            log.debug("Ignoring stale result for task {} of execution {}", result.taskId(), result.executionId());
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
            if (!activityRepository.markFailed(result.executionId(), result.taskId(),
                    result.error(), activity.version())) {
                return;
            }
            eventStore.append(result.executionId(), "ActivityFailed",
                    payload("taskId", result.taskId(), "activityName", activity.activityName(),
                            "error", result.error()));
            failWorkflow(result.executionId(), result.error());
            return;
        }

        if (activityRepository.countNonTerminal(result.executionId()) == 0) {
            completeWorkflow(result.executionId());
        }
    }

    /** RUNNING → COMPLETED (guarded); appends WorkflowCompleted when the transition wins. */
    @Transactional
    public void completeWorkflow(UUID executionId) {
        WorkflowExecution execution = executionRepository.findById(executionId).orElse(null);
        if (execution == null) {
            return;
        }
        if (executionRepository.markCompleted(executionId, execution.version())) {
            eventStore.append(executionId, "WorkflowCompleted", Map.of());
            log.info("Workflow execution {} completed", executionId);
        }
    }

    /** RUNNING → FAILED (guarded); appends WorkflowFailed when the transition wins. */
    @Transactional
    public void failWorkflow(UUID executionId, String error) {
        WorkflowExecution execution = executionRepository.findById(executionId).orElse(null);
        if (execution == null) {
            return;
        }
        if (executionRepository.markFailed(executionId, execution.version())) {
            eventStore.append(executionId, "WorkflowFailed", payload("error", error));
            log.info("Workflow execution {} failed: {}", executionId, error);
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

    private static Map<String, Object> payload(String... kv) {
        Map<String, Object> payload = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            if (kv[i + 1] != null) {
                payload.put(kv[i], kv[i + 1]);
            }
        }
        return payload;
    }
}
