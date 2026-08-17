package io.fuseflow.engine.service;

import io.fuseflow.engine.dispatch.AfterCommitDispatcher;
import io.fuseflow.common.messaging.ActivityTask;
import io.fuseflow.engine.dispatch.TaskDispatcher;
import io.fuseflow.engine.model.ActivityExecution;
import io.fuseflow.engine.model.ActivityStatus;
import io.fuseflow.engine.model.WorkflowExecution;
import io.fuseflow.engine.model.WorkflowStatus;
import io.fuseflow.engine.repository.ActivityExecutionRepository;
import io.fuseflow.engine.repository.EventStore;
import io.fuseflow.engine.repository.WorkflowExecutionRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SchedulerTest {

    private static final UUID EXECUTION = UUID.randomUUID();

    private final ActivityExecutionRepository activityRepository = mock(ActivityExecutionRepository.class);
    private final WorkflowExecutionRepository executionRepository = mock(WorkflowExecutionRepository.class);
    private final EventStore eventStore = mock(EventStore.class);
    private final AfterCommitDispatcher afterCommitDispatcher = mock(AfterCommitDispatcher.class);
    private final TaskDispatcher taskDispatcher = mock(TaskDispatcher.class);
    private final Scheduler scheduler = new Scheduler(activityRepository, executionRepository, eventStore,
            afterCommitDispatcher, taskDispatcher);

    @BeforeEach
    void setUp() {
        when(executionRepository.findById(EXECUTION)).thenReturn(Optional.of(
                new WorkflowExecution(EXECUTION, UUID.randomUUID(), "wf", 1, "{\"k\":1}", null,
                        WorkflowStatus.RUNNING, 0, Instant.now(), Instant.now(), Instant.now(), null)));
    }

    private static ActivityExecution pending(String taskId, int remaining, List<String> dependents, long version) {
        Instant now = Instant.now();
        return new ActivityExecution(EXECUTION, taskId, "act-" + taskId, ActivityStatus.PENDING,
                remaining, dependents, 1, null, null, version, now, now);
    }

    @Test
    void schedulesRunnableActivityAppendsEventAndDispatchesAfterCommit() {
        ActivityExecution root = pending("a", 0, List.of("b"), 3);
        when(activityRepository.markScheduled(EXECUTION, "a", 3)).thenReturn(true);

        scheduler.schedule(EXECUTION, List.of(root), "{\"k\":1}");

        verify(eventStore).append(eq(EXECUTION), eq("ActivityScheduled"), any());
        ArgumentCaptor<Runnable> action = ArgumentCaptor.forClass(Runnable.class);
        verify(afterCommitDispatcher).runAfterCommit(action.capture());
        action.getValue().run();
        verify(taskDispatcher).dispatch(any(ActivityTask.class));
    }

    @Test
    void skipsActivitiesAlreadyScheduledOrTerminal() {
        ActivityExecution stale = pending("a", 0, List.of(), 0);
        when(activityRepository.markScheduled(EXECUTION, "a", 0)).thenReturn(false);

        scheduler.schedule(EXECUTION, List.of(stale), "{\"k\":1}");

        verify(eventStore, never()).append(eq(EXECUTION), eq("ActivityScheduled"), any());
        verify(afterCommitDispatcher, never()).runAfterCommit(any());
        verify(taskDispatcher, never()).dispatch(any());
    }

    @Test
    void doesNothingForEmptyActivityList() {
        scheduler.schedule(EXECUTION, List.of(), "{\"k\":1}");
        verify(executionRepository, never()).findById(any());
        verify(afterCommitDispatcher, never()).runAfterCommit(any());
    }

    @Test
    void decrementsDependentsAndSchedulesThoseWhoseCounterReachesZero() {
        // b depends on a (counter 1). After a completes, the decrement returns the updated row
        // (counter 0) in one round trip — no follow-up SELECT — and b becomes runnable.
        when(activityRepository.decrement(EXECUTION, "b"))
                .thenReturn(Optional.of(pending("b", 0, List.of("c"), 5)));
        when(activityRepository.markScheduled(EXECUTION, "b", 5)).thenReturn(true);

        scheduler.onActivityCompleted(EXECUTION, "a", List.of("b"));

        verify(activityRepository).decrement(EXECUTION, "b");
        verify(activityRepository, never()).findById(eq(EXECUTION), any());
        verify(activityRepository).markScheduled(EXECUTION, "b", 5);
        verify(eventStore).append(eq(EXECUTION), eq("ActivityScheduled"), any());
    }

    @Test
    void doesNotScheduleDependentsWhoseCounterIsStillPositive() {
        // e depends on c AND d (counter 2). c completes → counter 1 → still PENDING.
        when(activityRepository.decrement(EXECUTION, "e"))
                .thenReturn(Optional.of(pending("e", 1, List.of(), 9)));

        scheduler.onActivityCompleted(EXECUTION, "c", List.of("e"));

        verify(activityRepository, never()).markScheduled(eq(EXECUTION), eq("e"), anyLong());
        verify(executionRepository, never()).findById(any());
        verify(taskDispatcher, never()).dispatch(any());
    }

    @Test
    void skipsDecrementWhenDependentRowIsGoneOrAlreadySatisfied() {
        // A sibling branch already decremented the counter to 0 → decrement no-ops (empty).
        when(activityRepository.decrement(EXECUTION, "e")).thenReturn(Optional.empty());

        scheduler.onActivityCompleted(EXECUTION, "d", List.of("e"));

        verify(activityRepository, never()).findById(eq(EXECUTION), eq("e"));
        verify(activityRepository, never()).markScheduled(eq(EXECUTION), eq("e"), anyLong());
        verify(executionRepository, never()).findById(any());
    }
}
