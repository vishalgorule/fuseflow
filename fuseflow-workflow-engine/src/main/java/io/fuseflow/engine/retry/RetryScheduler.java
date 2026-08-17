package io.fuseflow.engine.retry;

import io.fuseflow.common.messaging.ActivityTask;
import io.fuseflow.engine.config.ReliabilityProperties;
import io.fuseflow.engine.dispatch.AfterCommitDispatcher;
import io.fuseflow.engine.dispatch.TaskDispatcher;
import io.fuseflow.engine.model.ActivityExecution;
import io.fuseflow.engine.model.WorkflowExecution;
import io.fuseflow.engine.repository.ActivityExecutionRepository;
import io.fuseflow.engine.repository.WorkflowExecutionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * DB-polled due-time queue (Phase 7, FR-6): the {@link RetryManager} parks a retryable failure
 * as SCHEDULED with a future {@code retry_due_at}; this poller re-dispatches it when due, using
 * the row's bumped attempt. Delivery stays at-least-once: the claim (clearing {@code retry_due_at})
 * is version-guarded so a concurrent poller never double-claims, and a crash between claim and
 * publish is covered by boot-time recovery (the row is a plain SCHEDULED activity then).
 */
@Component
public class RetryScheduler {

    private static final Logger log = LoggerFactory.getLogger(RetryScheduler.class);

    private final ActivityExecutionRepository activityRepository;
    private final WorkflowExecutionRepository executionRepository;
    private final AfterCommitDispatcher afterCommitDispatcher;
    private final TaskDispatcher taskDispatcher;
    private final ReliabilityProperties properties;

    public RetryScheduler(ActivityExecutionRepository activityRepository,
                          WorkflowExecutionRepository executionRepository,
                          AfterCommitDispatcher afterCommitDispatcher,
                          TaskDispatcher taskDispatcher,
                          ReliabilityProperties properties) {
        this.activityRepository = activityRepository;
        this.executionRepository = executionRepository;
        this.afterCommitDispatcher = afterCommitDispatcher;
        this.taskDispatcher = taskDispatcher;
        this.properties = properties;
    }

    @Scheduled(fixedDelayString = "${fuseflow.engine.poll-interval:5s}")
    public void dispatchDueRetries() {
        // Phase 7 scale: bounded drain (a burst spreads across cycles, oldest first) + a single
        // batched input fetch for the whole cycle (was one workflow SELECT per due retry).
        var due = activityRepository.findDueRetries(properties.getPollBatchSize());
        if (due.isEmpty()) {
            return;
        }
        Map<UUID, WorkflowExecution> executions = executionRepository
                .findByIds(due.stream().map(ActivityExecution::workflowExecutionId).distinct().toList())
                .stream()
                .collect(Collectors.toMap(WorkflowExecution::id, Function.identity()));
        for (ActivityExecution activity : due) {
            WorkflowExecution execution = executions.get(activity.workflowExecutionId());
            if (execution == null) {
                continue;
            }
            if (activityRepository.clearRetryDue(execution.id(), activity.taskId(), activity.version())) {
                ActivityTask task = new ActivityTask(execution.id(), activity.taskId(), activity.activityName(),
                        execution.input(), activity.attempt());
                afterCommitDispatcher.runAfterCommit(() -> taskDispatcher.dispatch(task));
                log.info("Re-dispatching retry attempt {} of activity {} (execution {})",
                        activity.attempt(), activity.taskId(), execution.id());
            }
        }
    }
}
