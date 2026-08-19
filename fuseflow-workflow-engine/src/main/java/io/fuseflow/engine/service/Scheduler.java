package io.fuseflow.engine.service;

import io.fuseflow.engine.model.ActivityExecution;
import io.fuseflow.engine.model.WorkflowExecution;
import io.fuseflow.engine.model.WorkflowStatus;
import io.fuseflow.engine.registry.PoolRoutingTable;
import io.fuseflow.engine.repository.ActivityExecutionRepository;
import io.fuseflow.engine.repository.DispatchOutboxRepository;
import io.fuseflow.engine.repository.EventStore;
import io.fuseflow.engine.repository.WorkflowExecutionRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Dependency-counting scheduler (architecture §6.2): marks activities SCHEDULED the moment
 * their remaining-dependency count reaches 0, appends {@code ActivityScheduled}, and hands
 * them to the <b>dispatch outbox</b> (a row written in this transaction, published by
 * {@code DispatchOutboxPublisher}) — so a crash between commit and Kafka publish can never
 * lose a task.
 *
 * <p>Post-Phase 7 hardening: pool availability is checked <b>before</b> scheduling. An activity
 * no ONLINE pool can route stays PENDING with an {@code ActivityUnroutable} event and is
 * re-driven when the routing table gains the capability (see {@code PoolRoutingService}) — it
 * never burns the start-timeout/retry clock just because no worker is up yet.
 */
@Service
public class Scheduler {

    private final ActivityExecutionRepository activityRepository;
    private final WorkflowExecutionRepository executionRepository;
    private final EventStore eventStore;
    private final PoolRoutingTable routingTable;
    private final DispatchOutboxRepository outboxRepository;

    public Scheduler(ActivityExecutionRepository activityRepository,
                     WorkflowExecutionRepository executionRepository,
                     EventStore eventStore,
                     PoolRoutingTable routingTable,
                     DispatchOutboxRepository outboxRepository) {
        this.activityRepository = activityRepository;
        this.executionRepository = executionRepository;
        this.eventStore = eventStore;
        this.routingTable = routingTable;
        this.outboxRepository = outboxRepository;
    }

    /**
     * Marks the given activities SCHEDULED (guarded, so already-scheduled/terminal ones are
     * skipped) and writes dispatch outbox rows. Called on start for root tasks and from the
     * result handler for dependents whose counter reached 0.
     *
     * <p>The execution input is passed in explicitly (Phase 7 scale): it is identical for every
     * task of an execution, so callers that already hold it (start, recovery) avoid the
     * per-schedule re-read, and the result path resolves it lazily at most once per completion.
     */
    @Transactional
    public void schedule(UUID executionId, List<ActivityExecution> activities, String input) {
        if (activities == null || activities.isEmpty()) {
            return;
        }
        // Phase 8: a PAUSED execution schedules nothing new (in-flight activities may finish;
        // dependents stay PENDING and are scheduled by resume's re-drive). Terminal executions
        // schedule nothing either.
        WorkflowExecution execution = executionRepository.findById(executionId).orElse(null);
        if (execution == null || execution.status() != WorkflowStatus.RUNNING) {
            return;
        }
        for (ActivityExecution activity : activities) {
            // Post-Phase 7 hardening: verify pool availability BEFORE scheduling. An activity no
            // ONLINE pool can route stays PENDING (ActivityUnroutable) and is re-driven when the
            // routing table gains the capability — it never burns the start-timeout / retry clock.
            if (routingTable.resolveTopic(activity.activityName(), activity.taskId()).isEmpty()) {
                eventStore.append(executionId, "ActivityUnroutable", Map.of(
                        "taskId", activity.taskId(),
                        "activityName", activity.activityName(),
                        "reason", "no ONLINE pool advertises activity '" + activity.activityName() + "'"));
                continue;
            }
            if (activityRepository.markScheduled(executionId, activity.taskId(), activity.version())) {
                eventStore.append(executionId, "ActivityScheduled",
                        Map.of("taskId", activity.taskId(), "activityName", activity.activityName()));
                // Dispatch outbox: the publish happens via the outbox poller, so a crash
                // between this transaction and the Kafka publish can never lose the task.
                outboxRepository.insert(executionId, activity.taskId(), activity.activityName(),
                        input, activity.attempt());
            }
        }
    }

    /**
     * Fan-out after a completion: decrements each dependent's counter and schedules any whose
     * counter reached 0 (they become runnable). Fan-in joins fall out naturally — a task with
     * multiple upstreams only becomes runnable when the last one completes.
     */
    @Transactional
    public void onActivityCompleted(UUID executionId, String completedTaskId, List<String> dependents) {
        // Input is identical for every task of an execution: resolve it lazily, once per
        // completion, only when a dependent actually becomes runnable (Phase 7 scale — the
        // per-runnable-dependent re-read is gone; the decrement itself returns the updated row).
        String input = null;
        for (String dependent : dependents) {
            Optional<ActivityExecution> updated = activityRepository.decrement(executionId, dependent);
            if (updated.isPresent() && updated.get().remainingDependencies() == 0) {
                if (input == null) {
                    input = executionRepository.findById(executionId)
                            .map(WorkflowExecution::input)
                            .orElse(null);
                }
                schedule(executionId, List.of(updated.get()), input);
            }
        }
    }
}
