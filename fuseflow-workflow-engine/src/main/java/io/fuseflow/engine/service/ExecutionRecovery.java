package io.fuseflow.engine.service;

import io.fuseflow.engine.dispatch.AfterCommitDispatcher;
import io.fuseflow.common.messaging.ActivityTask;
import io.fuseflow.engine.dispatch.TaskDispatcher;
import io.fuseflow.engine.ha.EngineShards;
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
 * pending work from durable state — activities left SCHEDULED/STARTED are re-dispatched, and
 * PENDING activities whose dependencies are already satisfied are scheduled afterwards. In the
 * in-memory mode the engine is the only executor, so re-dispatch is a pure retry; in the Kafka
 * mode the task may still be genuinely in-flight on a worker, and re-publishing is safe only
 * because results are idempotent by {@code (executionId, taskId, attempt)}. The stale
 * re-dispatch runs first deliberately: {@link Scheduler#schedule} commits PENDING → SCHEDULED,
 * so a scan in the other order would re-find (and double-dispatch) the freshly scheduled tasks.
 * No work is lost or duplicated: the version-guarded transitions make re-dispatch idempotent.
 */
@Component
public class ExecutionRecovery implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(ExecutionRecovery.class);

    private final WorkflowExecutionRepository executionRepository;
    private final ActivityExecutionRepository activityRepository;
    private final Scheduler scheduler;
    private final AfterCommitDispatcher afterCommitDispatcher;
    private final TaskDispatcher taskDispatcher;
    private final EngineShards engineShards;

    public ExecutionRecovery(WorkflowExecutionRepository executionRepository,
                             ActivityExecutionRepository activityRepository,
                             Scheduler scheduler,
                             AfterCommitDispatcher afterCommitDispatcher,
                             TaskDispatcher taskDispatcher,
                             EngineShards engineShards) {
        this.executionRepository = executionRepository;
        this.activityRepository = activityRepository;
        this.scheduler = scheduler;
        this.afterCommitDispatcher = afterCommitDispatcher;
        this.taskDispatcher = taskDispatcher;
        this.engineShards = engineShards;
    }

    @Override
    public void run(ApplicationArguments args) {
        // Phase 5 engine HA: recover only the shards this instance owns, so multiple engine
        // instances in the fuseflow-engine group never re-dispatch the same execution.
        List<WorkflowExecution> running = engineShards.ownsAll()
                ? executionRepository.findByStatus(WorkflowStatus.RUNNING)
                : executionRepository.findByStatusInShards(WorkflowStatus.RUNNING, engineShards.ownedShards());
        if (running.isEmpty()) {
            return;
        }
        log.info("Recovering {} RUNNING execution(s) (shards {} of {})", running.size(),
                engineShards.ownedShards(), engineShards.shardCount());
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
