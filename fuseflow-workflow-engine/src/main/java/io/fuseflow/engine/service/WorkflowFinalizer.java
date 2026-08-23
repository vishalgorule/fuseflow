package io.fuseflow.engine.service;

import io.fuseflow.engine.messaging.WorkflowEventPublisher;
import io.fuseflow.engine.metrics.EngineMetrics;
import io.fuseflow.engine.model.WorkflowExecution;
import io.fuseflow.engine.repository.EventStore;
import io.fuseflow.engine.repository.WorkflowExecutionRepository;
import io.micrometer.observation.annotation.Observed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Terminal workflow transitions (RUNNING → COMPLETED | FAILED), extracted so both the
 * {@link ResultHandler} (success path) and the {@link RetryManager} (retries exhausted /
 * non-retryable failure) drive the execution to its end state through one guarded path.
 */
@Service
public class WorkflowFinalizer {

    private static final Logger log = LoggerFactory.getLogger(WorkflowFinalizer.class);

    private final WorkflowExecutionRepository executionRepository;
    private final EventStore eventStore;
    private final WorkflowEventPublisher workflowEventPublisher;
    private final EngineMetrics metrics;

    public WorkflowFinalizer(WorkflowExecutionRepository executionRepository,
                             EventStore eventStore,
                             WorkflowEventPublisher workflowEventPublisher,
                             EngineMetrics metrics) {
        this.executionRepository = executionRepository;
        this.eventStore = eventStore;
        this.workflowEventPublisher = workflowEventPublisher;
        this.metrics = metrics;
    }

    /** RUNNING → COMPLETED (guarded); appends WorkflowCompleted when the transition wins. */
    @Observed(name = "fuseflow.engine.complete")
    @Transactional
    public void completeWorkflow(UUID executionId) {
        WorkflowExecution execution = executionRepository.findById(executionId).orElse(null);
        if (execution == null) {
            return;
        }
        if (executionRepository.markCompleted(executionId, execution.version())) {
            eventStore.append(executionId, "WorkflowCompleted", Map.of());
            workflowEventPublisher.publish(executionId, "WorkflowCompleted", Map.of());
            metrics.workflowCompleted(execution.workflowName());
            recordWorkflowDuration(execution);
            log.info("Workflow execution {} completed", executionId);
        }
    }

    /** RUNNING → FAILED (guarded); appends WorkflowFailed when the transition wins. */
    @Observed(name = "fuseflow.engine.fail")
    @Transactional
    public void failWorkflow(UUID executionId, String error) {
        WorkflowExecution execution = executionRepository.findById(executionId).orElse(null);
        if (execution == null) {
            return;
        }
        if (executionRepository.markFailed(executionId, execution.version())) {
            eventStore.append(executionId, "WorkflowFailed", payload("error", error));
            workflowEventPublisher.publish(executionId, "WorkflowFailed", payload("error", error));
            metrics.workflowFailed(execution.workflowName());
            recordWorkflowDuration(execution);
            log.info("Workflow execution {} failed: {}", executionId, error);
        }
    }

    private void recordWorkflowDuration(WorkflowExecution execution) {
        if (execution.startedAt() != null) {
            metrics.recordWorkflowDuration(execution.workflowName(),
                    Duration.between(execution.startedAt(), Instant.now()).toMillis(), TimeUnit.MILLISECONDS);
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
