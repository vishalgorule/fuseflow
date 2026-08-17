package io.fuseflow.engine.service;

import io.fuseflow.common.messaging.ActivityTask;
import io.fuseflow.engine.dispatch.ActivityResult;
import io.fuseflow.engine.messaging.WorkflowEventPublisher;
import io.fuseflow.engine.model.ActivityExecution;
import io.fuseflow.engine.model.ActivityStatus;
import io.fuseflow.engine.model.WorkflowExecution;
import io.fuseflow.engine.model.WorkflowStatus;
import io.fuseflow.engine.repository.ActivityExecutionRepository;
import io.fuseflow.engine.repository.EventStore;
import io.fuseflow.engine.repository.WorkflowExecutionRepository;
import io.fuseflow.engine.retry.RetryManager;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.mockito.ArgumentCaptor;

class ResultHandlerTest {

    private static final UUID EXECUTION = UUID.randomUUID();

    private final ActivityExecutionRepository activityRepository = mock(ActivityExecutionRepository.class);
    private final WorkflowExecutionRepository executionRepository = mock(WorkflowExecutionRepository.class);
    private final EventStore eventStore = mock(EventStore.class);
    private final Scheduler scheduler = mock(Scheduler.class);
    private final WorkflowEventPublisher workflowEventPublisher = mock(WorkflowEventPublisher.class);
    private final RetryManager retryManager = mock(RetryManager.class);
    private final WorkflowFinalizer workflowFinalizer = new WorkflowFinalizer(executionRepository,
            eventStore, workflowEventPublisher);
    private final ResultHandler resultHandler = new ResultHandler(activityRepository, executionRepository,
            eventStore, scheduler, retryManager, workflowFinalizer, new ObjectMapper());

    private static ActivityExecution activity(String taskId, ActivityStatus status, long version) {
        Instant now = Instant.now();
        return new ActivityExecution(EXECUTION, taskId, "act-" + taskId, status, 0, List.of("b"), 1,
                null, null, version, now, now);
    }

    private static ActivityResult success(String taskId) {
        return ActivityResult.success(new ActivityTask(EXECUTION, taskId, "act-" + taskId, null, 1), "{\"x\":1}");
    }

    private static ActivityResult failure(String taskId) {
        return ActivityResult.failure(new ActivityTask(EXECUTION, taskId, "act-" + taskId, null, 1), "boom");
    }

    @Test
    void completesActivityAppendsEventAndFansOut() {
        when(activityRepository.findById(EXECUTION, "a")).thenReturn(Optional.of(activity("a", ActivityStatus.STARTED, 4)));
        when(activityRepository.markCompleted(EXECUTION, "a", "{\"x\":1}", 4)).thenReturn(true);
        when(executionRepository.decrementRemainingActivities(EXECUTION)).thenReturn(2); // dependent still pending

        resultHandler.handleResult(success("a"));

        verify(eventStore).append(eq(EXECUTION), eq("ActivityCompleted"), any());
        verify(scheduler).onActivityCompleted(EXECUTION, "a", List.of("b"));
        verify(executionRepository, never()).markCompleted(any(), anyLong());
        verify(executionRepository, never()).markFailed(any(), anyLong());
    }

    @Test
    void completesWorkflowWhenItWasTheLastActivity() {
        when(activityRepository.findById(EXECUTION, "a")).thenReturn(Optional.of(activity("a", ActivityStatus.STARTED, 4)));
        when(activityRepository.markCompleted(EXECUTION, "a", "{\"x\":1}", 4)).thenReturn(true);
        when(executionRepository.decrementRemainingActivities(EXECUTION)).thenReturn(0);
        when(executionRepository.findById(EXECUTION)).thenReturn(Optional.of(
                new WorkflowExecution(EXECUTION, UUID.randomUUID(), "wf", 1, null, null,
                        WorkflowStatus.RUNNING, 7, Instant.now(), Instant.now(), Instant.now(), null)));
        when(executionRepository.markCompleted(EXECUTION, 7)).thenReturn(true);

        resultHandler.handleResult(success("a"));

        verify(eventStore).append(eq(EXECUTION), eq("WorkflowCompleted"), any());
        verify(workflowEventPublisher).publish(eq(EXECUTION), eq("WorkflowCompleted"), any());
    }

    @Test
    void acceptsResultsForScheduledActivitiesInKafkaMode() {
        // Kafka (Phase 4): a worker may complete before its STARTED signal is consumed, so
        // SCHEDULED is in-flight too.
        when(activityRepository.findById(EXECUTION, "a")).thenReturn(Optional.of(activity("a", ActivityStatus.SCHEDULED, 4)));
        when(activityRepository.markCompleted(EXECUTION, "a", "{\"x\":1}", 4)).thenReturn(true);
        when(executionRepository.decrementRemainingActivities(EXECUTION)).thenReturn(0);
        when(executionRepository.findById(EXECUTION)).thenReturn(Optional.of(
                new WorkflowExecution(EXECUTION, UUID.randomUUID(), "wf", 1, null, null,
                        WorkflowStatus.RUNNING, 7, Instant.now(), Instant.now(), Instant.now(), null)));
        when(executionRepository.markCompleted(EXECUTION, 7)).thenReturn(true);

        resultHandler.handleResult(success("a"));

        verify(eventStore).append(eq(EXECUTION), eq("WorkflowCompleted"), any());
    }

    @Test
    void routesFailuresThroughTheRetryManager() {
        // Phase 7: a FAILED result no longer fails the workflow inline — the retry manager
        // decides between retry (default) and terminal failure.
        when(activityRepository.findById(EXECUTION, "b")).thenReturn(Optional.of(activity("b", ActivityStatus.STARTED, 4)));

        resultHandler.handleResult(failure("b"));

        ArgumentCaptor<ActivityResult> captor = ArgumentCaptor.forClass(ActivityResult.class);
        verify(retryManager).onActivityFailed(captor.capture());
        assertThat(captor.getValue().success()).isFalse();
        assertThat(captor.getValue().error()).isEqualTo("boom");
        assertThat(captor.getValue().attempt()).isEqualTo(1);
        verify(activityRepository, never()).markFailed(any(), any(), any(), any(), anyLong());
        verify(scheduler, never()).onActivityCompleted(any(), any(), any());
    }

    @Test
    void ignoresResultsFromPreviousAttempts() {
        // A stale result from attempt 1 must not complete a row that already moved to attempt 2.
        when(activityRepository.findById(EXECUTION, "a")).thenReturn(Optional.of(activity("a", ActivityStatus.SCHEDULED, 4)));

        ActivityResult stale = ActivityResult.success(new ActivityTask(EXECUTION, "a", "act-a", null, 1), "{\"x\":1}");
        resultHandler.handleResult(stale);

        verifyNoInteractions(eventStore, scheduler, executionRepository);
        verify(retryManager, never()).onActivityFailed(any());
    }

    @Test
    void ignoresStaleOrDuplicateResults() {
        // Activity already terminal (e.g. re-delivered result after recovery).
        when(activityRepository.findById(EXECUTION, "a")).thenReturn(Optional.of(activity("a", ActivityStatus.COMPLETED, 5)));

        resultHandler.handleResult(success("a"));

        verifyNoInteractions(eventStore, scheduler, executionRepository);
        verify(activityRepository, never()).markCompleted(any(), any(), any(), anyLong());
        verify(activityRepository, never()).markFailed(any(), any(), any(), any(), anyLong());
    }

    @Test
    void ignoresResultsForUnknownActivities() {
        when(activityRepository.findById(EXECUTION, "ghost")).thenReturn(Optional.empty());

        resultHandler.handleResult(success("ghost"));

        verifyNoInteractions(eventStore, scheduler, executionRepository);
    }

    @Test
    void skipsWorkflowCompletionWhenTerminalTransitionLosesRace() {
        when(activityRepository.findById(EXECUTION, "a")).thenReturn(Optional.of(activity("a", ActivityStatus.STARTED, 4)));
        when(activityRepository.markCompleted(EXECUTION, "a", "{\"x\":1}", 4)).thenReturn(true);
        when(executionRepository.decrementRemainingActivities(EXECUTION)).thenReturn(0);
        when(executionRepository.findById(EXECUTION)).thenReturn(Optional.of(
                new WorkflowExecution(EXECUTION, UUID.randomUUID(), "wf", 1, null, null,
                        WorkflowStatus.RUNNING, 7, Instant.now(), Instant.now(), Instant.now(), null)));
        // A concurrent transition already marked the execution terminal.
        when(executionRepository.markCompleted(EXECUTION, 7)).thenReturn(false);

        resultHandler.handleResult(success("a"));

        verify(eventStore, never()).append(eq(EXECUTION), eq("WorkflowCompleted"), any());
    }
}
