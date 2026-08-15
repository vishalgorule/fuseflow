package io.fuseflow.engine.service;

import io.fuseflow.engine.dispatch.AfterCommitDispatcher;
import io.fuseflow.common.messaging.ActivityTask;
import io.fuseflow.engine.dispatch.TaskDispatcher;
import io.fuseflow.engine.ha.EngineShards;
import io.fuseflow.engine.model.ActivityExecution;
import io.fuseflow.engine.model.ActivityStatus;
import io.fuseflow.engine.model.WorkflowExecution;
import io.fuseflow.engine.model.WorkflowStatus;
import io.fuseflow.engine.repository.ActivityExecutionRepository;
import io.fuseflow.engine.repository.WorkflowExecutionRepository;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Boot-time recovery ordering: the stale re-dispatch scan must run BEFORE the PENDING safety
 * net, because {@link Scheduler#schedule} commits PENDING → SCHEDULED — a scan in the other
 * order would re-find the freshly scheduled rows and dispatch them a second time.
 */
class ExecutionRecoveryTest {

    private static final UUID EXECUTION = UUID.randomUUID();

    private final WorkflowExecutionRepository executionRepository = mock(WorkflowExecutionRepository.class);
    private final ActivityExecutionRepository activityRepository = mock(ActivityExecutionRepository.class);
    private final Scheduler scheduler = mock(Scheduler.class);
    private final AfterCommitDispatcher afterCommitDispatcher = mock(AfterCommitDispatcher.class);
    private final TaskDispatcher taskDispatcher = mock(TaskDispatcher.class);
    private final ExecutionRecovery recovery = new ExecutionRecovery(executionRepository, activityRepository,
            scheduler, afterCommitDispatcher, taskDispatcher, new EngineShards(8, "all"));

    private static WorkflowExecution running(UUID id) {
        Instant now = Instant.now();
        return new WorkflowExecution(id, UUID.randomUUID(), "wf", 1, "{}", null,
                WorkflowStatus.RUNNING, 0, now, now, now, null);
    }

    @Test
    void reDispatchesStaleTasksBeforeSchedulingRunnablePendingOnes() {
        WorkflowExecution execution = running(EXECUTION);
        when(executionRepository.findByStatus(WorkflowStatus.RUNNING)).thenReturn(List.of(execution));

        // Pre-crash state: b was in-flight (STARTED), c is PENDING with all deps satisfied.
        ActivityExecution stale = new ActivityExecution(EXECUTION, "b", "actB", ActivityStatus.STARTED,
                0, List.of(), 1, null, null, 4, Instant.now(), Instant.now());
        when(activityRepository.findStale(EXECUTION)).thenReturn(List.of(stale));
        ActivityExecution runnable = new ActivityExecution(EXECUTION, "c", "actC", ActivityStatus.PENDING,
                0, List.of(), 1, null, null, 2, Instant.now(), Instant.now());
        when(activityRepository.findRunnableTaskIds(EXECUTION)).thenReturn(List.of("c"));
        when(activityRepository.findById(EXECUTION, "c")).thenReturn(Optional.of(runnable));

        recovery.run(null);

        // The stale scan runs first and must not observe rows the safety net schedules below.
        InOrder order = inOrder(activityRepository);
        order.verify(activityRepository).findStale(EXECUTION);
        order.verify(activityRepository).findRunnableTaskIds(EXECUTION);

        // The stale activity is re-dispatched (action runs, mirroring after-commit execution).
        ArgumentCaptor<Runnable> action = ArgumentCaptor.forClass(Runnable.class);
        verify(afterCommitDispatcher).runAfterCommit(action.capture());
        action.getValue().run();
        verify(taskDispatcher).dispatch(any(ActivityTask.class));
        verify(scheduler).schedule(eq(EXECUTION), eq(List.of(runnable)));
    }

    @Test
    void doesNothingWhenNoRunningExecutions() {
        when(executionRepository.findByStatus(WorkflowStatus.RUNNING)).thenReturn(List.of());

        recovery.run(null);

        verifyNoInteractions(activityRepository, scheduler, afterCommitDispatcher, taskDispatcher);
    }

    @Test
    void shardScopedInstanceRecoversOnlyItsShards() {
        // Phase 5 HA: an instance owning shards 0-3 queries only those — never the full scan.
        ExecutionRecovery sharded = new ExecutionRecovery(executionRepository, activityRepository,
                scheduler, afterCommitDispatcher, taskDispatcher, new EngineShards(8, "0-3"));
        WorkflowExecution execution = running(EXECUTION);
        when(executionRepository.findByStatusInShards(WorkflowStatus.RUNNING, Set.of(0, 1, 2, 3)))
                .thenReturn(List.of(execution));
        when(activityRepository.findStale(EXECUTION)).thenReturn(List.of());
        when(activityRepository.findRunnableTaskIds(EXECUTION)).thenReturn(List.of());

        sharded.run(null);

        verify(executionRepository).findByStatusInShards(WorkflowStatus.RUNNING, Set.of(0, 1, 2, 3));
        verify(executionRepository, never()).findByStatus(any());
    }
}
