package io.fuseflow.engine.service;

import io.fuseflow.engine.model.ActivityExecution;
import io.fuseflow.engine.model.ActivityStatus;
import io.fuseflow.engine.model.WorkflowExecution;
import io.fuseflow.engine.model.WorkflowStatus;
import io.fuseflow.engine.registry.PoolRoutingTable;
import io.fuseflow.engine.repository.ActivityExecutionRepository;
import io.fuseflow.engine.repository.DispatchOutboxRepository;
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
import static org.mockito.ArgumentMatchers.anyInt;
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
    private final PoolRoutingTable routingTable = mock(PoolRoutingTable.class);
    private final DispatchOutboxRepository outboxRepository = mock(DispatchOutboxRepository.class);

    private final Scheduler scheduler = new Scheduler(activityRepository, executionRepository, eventStore,
            routingTable, outboxRepository);

    @BeforeEach
    void setUp() {
        when(executionRepository.findById(EXECUTION)).thenReturn(Optional.of(
                new WorkflowExecution(EXECUTION, UUID.randomUUID(), "wf", 1, "{\"k\":1}", null,
                        WorkflowStatus.RUNNING, 0, Instant.now(), Instant.now(), Instant.now(), null)));
    }

    private static ActivityExecution pending(String taskId, int remaining, List<String> dependents, int attempt, long version) {
        Instant now = Instant.now();
        return new ActivityExecution(EXECUTION, taskId, "act-" + taskId, ActivityStatus.PENDING,
                remaining, dependents, attempt, null, null, version, now, now);
    }

    private static ActivityExecution pending(String taskId, int remaining, List<String> dependents, long version) {
        return pending(taskId, remaining, dependents, 1, version);
    }

    @Test
    void schedulesRunnableActivityAndWritesOutboxRow() {
        ActivityExecution root = pending("a", 0, List.of("b"), 3, 3);
        when(routingTable.resolveTopic("act-a", "a")).thenReturn(Optional.of("fuseflow-pool.io"));
        when(activityRepository.markScheduled(EXECUTION, "a", 3)).thenReturn(true);

        scheduler.schedule(EXECUTION, List.of(root), "{\"k\":1}");

        verify(eventStore).append(eq(EXECUTION), eq("ActivityScheduled"), any());
        verify(outboxRepository).insert(eq(EXECUTION), eq("a"), eq("act-a"), eq("{\"k\":1}"), eq(3));
    }

    @Test
    void skipsActivitiesAlreadyScheduledOrTerminal() {
        ActivityExecution stale = pending("a", 0, List.of(), 0);
        when(activityRepository.markScheduled(EXECUTION, "a", 0)).thenReturn(false);

        scheduler.schedule(EXECUTION, List.of(stale), "{\"k\":1}");

        verify(eventStore, never()).append(eq(EXECUTION), eq("ActivityScheduled"), any());
        verify(outboxRepository, never()).insert(any(), any(), any(), any(), anyInt());
    }

    @Test
    void doesNothingForEmptyActivityList() {
        scheduler.schedule(EXECUTION, List.of(), "{\"k\":1}");
        verify(executionRepository, never()).findById(any());
        verify(outboxRepository, never()).insert(any(), any(), any(), any(), anyInt());
    }

    @Test
    void decrementsDependentsAndSchedulesThoseWhoseCounterReachesZero() {
        // b depends on a (counter 1). After a completes, the decrement returns the updated row
        // (counter 0) in one round trip — no follow-up SELECT — and b becomes runnable.
        when(activityRepository.decrement(EXECUTION, "b"))
                .thenReturn(Optional.of(pending("b", 0, List.of("c"), 5)));
        when(routingTable.resolveTopic("act-b", "b")).thenReturn(Optional.of("fuseflow-pool.io"));
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
        verify(outboxRepository, never()).insert(any(), any(), any(), any(), anyInt());
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

    @Test
    void leavesUnroutableTaskPendingWithEvent() {
        // No ONLINE pool advertises the activity → the task must NOT be scheduled (no retry
        // clock, no attempts burned) — it waits PENDING for the pool-rejoin sweep.
        ActivityExecution root = pending("a", 0, List.of(), 1);
        when(routingTable.resolveTopic("act-a", "a")).thenReturn(Optional.empty());

        scheduler.schedule(EXECUTION, List.of(root), "{\"k\":1}");

        verify(eventStore).append(eq(EXECUTION), eq("ActivityUnroutable"), any());
        verify(activityRepository, never()).markScheduled(eq(EXECUTION), eq("a"), anyLong());
        verify(outboxRepository, never()).insert(any(), any(), any(), any(), anyInt());
    }

    @Test
    void checksRoutingOnlyForSchedulableTasks() {
        // Two runnable tasks: 'a' routable (scheduled), 'b' not (stays PENDING) — independent.
        ActivityExecution a = pending("a", 0, List.of(), 1);
        ActivityExecution b = pending("b", 0, List.of(), 1);
        when(routingTable.resolveTopic("act-a", "a")).thenReturn(Optional.of("fuseflow-pool.io"));
        when(routingTable.resolveTopic("act-b", "b")).thenReturn(Optional.empty());
        when(activityRepository.markScheduled(EXECUTION, "a", 1)).thenReturn(true);

        scheduler.schedule(EXECUTION, List.of(a, b), "{\"k\":1}");

        verify(activityRepository).markScheduled(EXECUTION, "a", 1);
        verify(activityRepository, never()).markScheduled(eq(EXECUTION), eq("b"), anyLong());
        verify(outboxRepository).insert(eq(EXECUTION), eq("a"), eq("act-a"), eq("{\"k\":1}"), eq(1));
        verify(eventStore).append(eq(EXECUTION), eq("ActivityUnroutable"), any());
    }
}
