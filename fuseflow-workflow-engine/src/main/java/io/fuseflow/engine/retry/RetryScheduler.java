package io.fuseflow.engine.retry;

import io.fuseflow.engine.config.ReliabilityProperties;
import io.fuseflow.engine.model.ActivityExecution;
import io.fuseflow.engine.model.WorkflowExecution;
import io.fuseflow.engine.model.WorkflowStatus;
import io.fuseflow.engine.repository.ActivityExecutionRepository;
import io.fuseflow.engine.repository.DispatchOutboxRepository;
import io.fuseflow.engine.repository.WorkflowExecutionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * DB-polled due-time queue (Phase 7, FR-6): the {@link RetryManager} parks a retryable failure
 * as SCHEDULED with a future {@code retry_due_at}; this poller re-dispatches it when due, using
 * the row's bumped attempt. Delivery stays at-least-once: the claim (clearing {@code retry_due_at})
 * is version-guarded so a concurrent poller never double-claims, and — post-Phase 7 hardening —
 * the claim and the dispatch outbox row commit in one transaction, so a crash between claim and
 * publish can never lose the retry either (the outbox poller publishes it, waiting for a capable
 * pool if necessary).
 */
@Component
public class RetryScheduler {

    private static final Logger log = LoggerFactory.getLogger(RetryScheduler.class);

    private final ActivityExecutionRepository activityRepository;
    private final WorkflowExecutionRepository executionRepository;
    private final DispatchOutboxRepository outboxRepository;
    private final ReliabilityProperties properties;

    public RetryScheduler(ActivityExecutionRepository activityRepository,
                          WorkflowExecutionRepository executionRepository,
                          DispatchOutboxRepository outboxRepository,
                          ReliabilityProperties properties) {
        this.activityRepository = activityRepository;
        this.executionRepository = executionRepository;
        this.outboxRepository = outboxRepository;
        this.properties = properties;
    }

    @Scheduled(fixedDelayString = "${fuseflow.engine.poll-interval:5s}")
    @Transactional
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
            // Phase 8: paused/terminal executions keep their retries parked (the row stays due;
            // resume picks it up on the next cycle) — no new dispatch while paused.
            if (execution == null || execution.status() != WorkflowStatus.RUNNING) {
                continue;
            }
            if (activityRepository.clearRetryDue(execution.id(), activity.taskId(), activity.version())) {
                // Claim + outbox row commit together; the outbox poller publishes when a
                // capable pool is available (waiting in the outbox costs no attempts).
                outboxRepository.insert(execution.id(), activity.taskId(), activity.activityName(),
                        execution.input(), activity.attempt());
                log.info("Re-dispatching retry attempt {} of activity {} (execution {})",
                        activity.attempt(), activity.taskId(), execution.id());
            }
        }
    }
}
