package io.fuseflow.engine.service;

import io.fuseflow.engine.dispatch.AfterCommitDispatcher;
import io.fuseflow.engine.dispatch.ActivityTask;
import io.fuseflow.engine.dispatch.TaskDispatcher;
import io.fuseflow.engine.model.ActivityExecution;
import io.fuseflow.engine.model.WorkflowExecution;
import io.fuseflow.engine.repository.ActivityExecutionRepository;
import io.fuseflow.engine.repository.EventStore;
import io.fuseflow.engine.repository.WorkflowExecutionRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Dependency-counting scheduler (architecture §6.2): marks activities SCHEDULED the moment
 * their remaining-dependency count reaches 0, appends {@code ActivityScheduled}, and hands
 * them to the {@link TaskDispatcher} — which is only invoked after the surrounding transaction
 * commits (persist → append event → publish). The engine never re-scans the definition DAG.
 */
@Service
public class Scheduler {

    private final ActivityExecutionRepository activityRepository;
    private final WorkflowExecutionRepository executionRepository;
    private final EventStore eventStore;
    private final AfterCommitDispatcher afterCommitDispatcher;
    private final TaskDispatcher taskDispatcher;

    public Scheduler(ActivityExecutionRepository activityRepository,
                     WorkflowExecutionRepository executionRepository,
                     EventStore eventStore,
                     AfterCommitDispatcher afterCommitDispatcher,
                     TaskDispatcher taskDispatcher) {
        this.activityRepository = activityRepository;
        this.executionRepository = executionRepository;
        this.eventStore = eventStore;
        this.afterCommitDispatcher = afterCommitDispatcher;
        this.taskDispatcher = taskDispatcher;
    }

    /**
     * Marks the given activities SCHEDULED (guarded, so already-scheduled/terminal ones are
     * skipped) and dispatches them after commit. Called on start for root tasks and from the
     * result handler for dependents whose counter reached 0.
     */
    @Transactional
    public void schedule(UUID executionId, List<ActivityExecution> activities) {
        if (activities == null || activities.isEmpty()) {
            return;
        }
        String input = executionRepository.findById(executionId)
                .map(WorkflowExecution::input)
                .orElse(null);
        for (ActivityExecution activity : activities) {
            if (activityRepository.markScheduled(executionId, activity.taskId(), activity.version())) {
                eventStore.append(executionId, "ActivityScheduled",
                        Map.of("taskId", activity.taskId(), "activityName", activity.activityName()));
                ActivityTask task = new ActivityTask(executionId, activity.taskId(), activity.activityName(),
                        input, activity.attempt());
                afterCommitDispatcher.runAfterCommit(() -> taskDispatcher.dispatch(task));
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
        for (String dependent : dependents) {
            if (activityRepository.decrement(executionId, dependent)) {
                ActivityExecution updated = activityRepository.findById(executionId, dependent).orElse(null);
                if (updated != null && updated.remainingDependencies() == 0) {
                    schedule(executionId, List.of(updated));
                }
            }
        }
    }
}
