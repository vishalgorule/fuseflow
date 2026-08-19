package io.fuseflow.engine.retry;

import io.fuseflow.engine.config.ReliabilityProperties;
import io.fuseflow.engine.dispatch.ActivityResult;
import io.fuseflow.engine.model.ActivityExecution;
import io.fuseflow.engine.model.ActivityStatus;
import io.fuseflow.engine.model.WorkflowExecution;
import io.fuseflow.engine.model.WorkflowStatus;
import io.fuseflow.engine.repository.ActivityExecutionRepository;
import io.fuseflow.engine.repository.WorkflowExecutionRepository;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TimeoutManagerTest {

    private static final UUID EXECUTION = UUID.randomUUID();

    private final ActivityExecutionRepository activityRepository = mock(ActivityExecutionRepository.class);
    private final WorkflowExecutionRepository executionRepository = mock(WorkflowExecutionRepository.class);
    private final RetryManager retryManager = mock(RetryManager.class);
    private final ReliabilityProperties properties = new ReliabilityProperties();

    private final TimeoutManager timeoutManager =
            new TimeoutManager(activityRepository, executionRepository, retryManager, properties);

    /** Default: the execution behind every timeout candidate is RUNNING (Phase 8 gate passes). */
    private void stubRunningExecutions() {
        Instant now = Instant.now();
        when(executionRepository.findByIds(any()))
                .thenReturn(List.of(new WorkflowExecution(EXECUTION, UUID.randomUUID(), "w", 1,
                        null, null, WorkflowStatus.RUNNING, 0, now, now, now, null, 0)));
    }

    private static ActivityExecution activity(String taskId, ActivityStatus status, int attempt) {
        Instant now = Instant.now();
        return new ActivityExecution(EXECUTION, taskId, "act-" + taskId, status, 0, List.of(), attempt,
                null, null, 1, now, now);
    }

    @Test
    void treatsNeverStartedActivitiesAsFailedAttempts() {
        stubRunningExecutions();
        properties.getTimeout().setStart(Duration.ofSeconds(60));
        when(activityRepository.findStartTimeouts(any()))
                .thenReturn(List.of(activity("a", ActivityStatus.SCHEDULED, 1)));

        timeoutManager.checkTimeouts();

        ArgumentCaptor<ActivityResult> captor = ArgumentCaptor.forClass(ActivityResult.class);
        verify(retryManager).onActivityFailed(captor.capture());
        ActivityResult result = captor.getValue();
        assertThat(result.success()).isFalse();
        assertThat(result.attempt()).isEqualTo(1);
        assertThat(result.error()).isEqualTo("start timeout after 60s");
        // No exception class name — the timeout itself is not an application error type.
        assertThat(result.errorType()).isNull();
    }

    @Test
    void treatsHungActivitiesAsFailedAttempts() {
        stubRunningExecutions();
        properties.getTimeout().setExecution(Duration.ofSeconds(300));
        when(activityRepository.findExecutionTimeouts(any()))
                .thenReturn(List.of(activity("b", ActivityStatus.STARTED, 1)));

        timeoutManager.checkTimeouts();

        ArgumentCaptor<ActivityResult> captor = ArgumentCaptor.forClass(ActivityResult.class);
        verify(retryManager).onActivityFailed(captor.capture());
        assertThat(captor.getValue().error()).isEqualTo("execution timeout after 300s");
        assertThat(captor.getValue().taskId()).isEqualTo("b");
    }

    @Test
    void doesNothingWhenNothingTimedOut() {
        when(activityRepository.findStartTimeouts(any())).thenReturn(List.of());
        when(activityRepository.findExecutionTimeouts(any())).thenReturn(List.of());

        timeoutManager.checkTimeouts();

        verify(retryManager, never()).onActivityFailed(any());
    }

    @Test
    void passesThroughAttemptNumberOfTimedOutRow() {
        stubRunningExecutions();
        properties.getTimeout().setStart(Duration.ofSeconds(5));
        when(activityRepository.findStartTimeouts(any()))
                .thenReturn(List.of(activity("a", ActivityStatus.SCHEDULED, 3)));

        timeoutManager.checkTimeouts();

        ArgumentCaptor<ActivityResult> captor = ArgumentCaptor.forClass(ActivityResult.class);
        verify(retryManager).onActivityFailed(captor.capture());
        assertThat(captor.getValue().attempt()).isEqualTo(3);
    }

    @Test
    void skipsTimeoutsForPausedOrTerminalExecutions() {
        // Phase 8: a paused execution's in-flight activities are exempt from the clock —
        // resume re-drives them; timeouts must not kill them while paused.
        Instant now = Instant.now();
        when(executionRepository.findByIds(any()))
                .thenReturn(List.of(new WorkflowExecution(EXECUTION, UUID.randomUUID(), "w", 1,
                        null, null, WorkflowStatus.PAUSED, 0, now, now, now, null, 0)));
        when(activityRepository.findStartTimeouts(any()))
                .thenReturn(List.of(activity("a", ActivityStatus.SCHEDULED, 1)));
        when(activityRepository.findExecutionTimeouts(any()))
                .thenReturn(List.of(activity("b", ActivityStatus.STARTED, 1)));

        timeoutManager.checkTimeouts();

        verify(retryManager, never()).onActivityFailed(any());
    }
}
