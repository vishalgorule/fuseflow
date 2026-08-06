package io.fuseflow.engine.service;

import io.fuseflow.engine.dispatch.AfterCommitDispatcher;
import io.fuseflow.engine.dispatch.ActivityTask;
import io.fuseflow.engine.dispatch.TaskDispatcher;
import io.fuseflow.engine.model.ActivityExecution;
import io.fuseflow.engine.model.WorkflowExecution;
import io.fuseflow.engine.model.WorkflowStatus;
import io.fuseflow.engine.repository.ActivityExecutionRepository;
import io.fuseflow.engine.repository.WorkflowExecutionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;

/**
 * Boot-time recovery (architecture §6.5): scans for RUNNING executions and re-drives their
 * pending work from durable state — activities left SCHEDULED/STARTED are re-dispatched (with
 * the in-memory dispatcher there is no durable in-flight progress), and PENDING activities
 * whose dependencies are already satisfied are scheduled afterwards. The stale re-dispatch runs
 * first deliberately: {@link Scheduler#schedule} commits PENDING → SCHEDULED, so a scan in the
 * other order would re-find (and double-dispatch) the freshly scheduled tasks. No work is lost
 * or duplicated: the version-guarded transitions make re-dispatch idempotent.
 */
@Component
public class ExecutionRecovery implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(ExecutionRecovery.class);

    private final WorkflowExecutionRepository executionRepository;
    private final ActivityExecutionRepository activityRepository;
    private final Scheduler scheduler;
    private final AfterCommitDispatcher afterCommitDispatcher;
    private final TaskDispatcher taskDispatcher;

    public ExecutionRecovery(WorkflowExecutionRepository executionRepository,
                             ActivityExecutionRepository activityRepository,
                             Scheduler scheduler,
                             AfterCommitDispatcher afterCommitDispatcher,
                             TaskDispatcher taskDispatcher) {
        this.executionRepository = executionRepository;
        this.activityRepository = activityRepository;
        this.scheduler = scheduler;
        this.afterCommitDispatcher = afterCommitDispatcher;
        this.taskDispatcher = taskDispatcher;
    }

    @Override
    public void run(ApplicationArguments args) {
        List<WorkflowExecution> running = executionRepository.findByStatus(WorkflowStatus.RUNNING);
        if (running.isEmpty()) {
            return;
        }
        log.info("Recovering {} RUNNING execution(s)", running.size());
        for (WorkflowExecution execution : running) {
            recover(execution);
        }
    }

    private void recover(WorkflowExecution execution) {
        UUID executionId = execution.id();

        // Re-handle activities that were dispatched but never finished. Runs FIRST: the scan
        // below only ever sees the durable pre-crash state, so nothing it re-dispatches was
        // just scheduled by this recovery pass.
        for (ActivityExecution stale : activityRepository.findStale(executionId)) {
            ActivityTask task = new ActivityTask(executionId, stale.taskId(), stale.activityName(),
                    execution.input(), stale.attempt());
            afterCommitDispatcher.runAfterCommit(() -> taskDispatcher.dispatch(task));
            log.info("Re-dispatching activity {} of execution {}", stale.taskId(), executionId);
        }

        // Safety net: PENDING activities whose dependencies are all satisfied (should not
        // normally persist, since scheduling is atomic with the satisfying completion). Runs
        // AFTER the stale scan on purpose — schedule() commits PENDING → SCHEDULED, so an
        // earlier scan would re-find these rows below and dispatch them a second time.
        List<String> runnableTaskIds = activityRepository.findRunnableTaskIds(executionId);
        if (!runnableTaskIds.isEmpty()) {
            List<ActivityExecution> runnable = runnableTaskIds.stream()
                    .flatMap(taskId -> activityRepository.findById(executionId, taskId).stream())
                    .toList();
            scheduler.schedule(executionId, runnable);
        }
    }
}
