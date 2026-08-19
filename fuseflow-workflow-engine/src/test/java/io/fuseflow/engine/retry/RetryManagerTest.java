package io.fuseflow.engine.retry;

import io.fuseflow.common.dto.RetryPolicy;
import io.fuseflow.engine.config.ReliabilityProperties;
import io.fuseflow.engine.definition.WorkflowDefinitionReader;
import io.fuseflow.engine.definition.WorkflowDefinitionSnapshot;
import io.fuseflow.engine.dispatch.ActivityResult;
import io.fuseflow.engine.model.ActivityExecution;
import io.fuseflow.engine.model.ActivityStatus;
import io.fuseflow.engine.model.WorkflowExecution;
import io.fuseflow.engine.model.WorkflowStatus;
import io.fuseflow.engine.messaging.WorkflowEventPublisher;
import io.fuseflow.engine.repository.ActivityExecutionRepository;
import io.fuseflow.engine.repository.EventStore;
import io.fuseflow.engine.repository.WorkflowExecutionRepository;
import io.fuseflow.engine.service.WorkflowFinalizer;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.ObjectProvider;

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

class RetryManagerTest {

    private static final UUID EXECUTION = UUID.randomUUID();
    private static final UUID WORKFLOW = UUID.randomUUID();

    private final ActivityExecutionRepository activityRepository = mock(ActivityExecutionRepository.class);
    private final WorkflowExecutionRepository executionRepository = mock(WorkflowExecutionRepository.class);
    private final WorkflowDefinitionReader definitionReader = mock(WorkflowDefinitionReader.class);
    private final EventStore eventStore = mock(EventStore.class);
    private final WorkflowFinalizer workflowFinalizer = mock(WorkflowFinalizer.class);
    private final DeadLetterPublisher deadLetterPublisher = mock(DeadLetterPublisher.class);
    private final WorkflowEventPublisher workflowEventPublisher = mock(WorkflowEventPublisher.class);
    private final ReliabilityProperties properties = new ReliabilityProperties();

    /** Real ObjectProvider so the default ifAvailable(Consumer) executes against the mock publisher. */
    private final ObjectProvider<DeadLetterPublisher> provider = new ObjectProvider<>() {
        @Override
        public DeadLetterPublisher getObject() {
            return deadLetterPublisher;
        }

        @Override
        public DeadLetterPublisher getObject(Object... args) {
            return deadLetterPublisher;
        }

        @Override
        public DeadLetterPublisher getIfAvailable() {
            return deadLetterPublisher;
        }

        @Override
        public DeadLetterPublisher getIfUnique() {
            return deadLetterPublisher;
        }
    };

    private final RetryManager retryManager = new RetryManager(activityRepository, executionRepository,
            definitionReader, eventStore, workflowFinalizer, properties, provider, workflowEventPublisher);

    private static ActivityExecution activity(ActivityStatus status, int attempt, long version) {
        Instant now = Instant.now();
        return new ActivityExecution(EXECUTION, "a", "actA", status, 0, List.of(), attempt,
                null, null, version, now, now);
    }

    private static ActivityResult failure(int attempt) {
        return new ActivityResult(EXECUTION, "a", attempt, false, null, "boom", null);
    }

    private void seed(RetryPolicy workflowPolicy, RetryPolicy taskPolicy) {
        when(executionRepository.findById(EXECUTION)).thenReturn(Optional.of(
                new WorkflowExecution(EXECUTION, WORKFLOW, "wf", 1, null, null,
                        WorkflowStatus.RUNNING, 1, Instant.now(), Instant.now(), null, null)));
        WorkflowDefinitionSnapshot.Task task =
                new WorkflowDefinitionSnapshot.Task("a", "actA", List.of(), taskPolicy);
        when(definitionReader.find(WORKFLOW)).thenReturn(Optional.of(
                new WorkflowDefinitionSnapshot(WORKFLOW, "wf", 1, workflowPolicy, List.of(task))));
    }

    @Test
    void retriesWhenAttemptsRemain() {
        seed(null, null);
        when(activityRepository.findById(EXECUTION, "a"))
                .thenReturn(Optional.of(activity(ActivityStatus.STARTED, 1, 4)));
        when(activityRepository.markRetryWaiting(eq(EXECUTION), eq("a"), anyInt(), any(), any(), any(), eq(4L)))
                .thenReturn(true);

        retryManager.onActivityFailed(failure(1));

        verify(activityRepository).markRetryWaiting(eq(EXECUTION), eq("a"), eq(2), any(), eq("boom"), eq(null), eq(4L));
        verify(eventStore).append(eq(EXECUTION), eq("ActivityRetryScheduled"), any());
        // Option B: workers are told the failed attempt is superseded so its queued message
        // is skipped instead of executed.
        verify(workflowEventPublisher).publish(eq(EXECUTION), eq("ActivitySuperseded"),
                org.mockito.ArgumentMatchers.argThat(payload ->
                        Integer.valueOf(1).equals(payload.get("supersededAttempt"))
                                && Integer.valueOf(2).equals(payload.get("newAttempt"))
                                && "a".equals(payload.get("taskId"))));
        verify(activityRepository, never()).markFailed(any(), any(), any(), any(), anyLong());
        verify(workflowFinalizer, never()).failWorkflow(any(), any());
        verify(deadLetterPublisher, never()).publish(any(), any(), any());
    }

    @Test
    void failsTerminallyWhenAttemptsExhausted() {
        seed(null, null);
        // Default maxAttempts = 3, so attempt 3 is the last.
        when(activityRepository.findById(EXECUTION, "a"))
                .thenReturn(Optional.of(activity(ActivityStatus.STARTED, 3, 7)));
        when(activityRepository.markFailed(eq(EXECUTION), eq("a"), eq("boom"), eq(null), eq(7L)))
                .thenReturn(true);

        retryManager.onActivityFailed(failure(3));

        verify(activityRepository).markFailed(eq(EXECUTION), eq("a"), eq("boom"), eq(null), eq(7L));
        verify(eventStore).append(eq(EXECUTION), eq("ActivityFailed"), any());
        verify(deadLetterPublisher).publish(any(), eq("boom"), eq(null));
        verify(workflowFinalizer).failWorkflow(EXECUTION, "boom");
        verify(activityRepository, never()).markRetryWaiting(any(), any(), anyInt(), any(), any(), any(), anyLong());
    }

    @Test
    void failsImmediatelyForNonRetryableErrorType() {
        // Workflow-level policy marks NullPointerException non-retryable — even attempt 1 fails terminally.
        seed(new RetryPolicy(null, null, null, null, List.of("java.lang.NullPointerException")), null);
        when(activityRepository.findById(EXECUTION, "a"))
                .thenReturn(Optional.of(activity(ActivityStatus.STARTED, 1, 2)));
        when(activityRepository.markFailed(eq(EXECUTION), eq("a"), eq("boom"),
                eq("java.lang.NullPointerException"), eq(2L))).thenReturn(true);

        retryManager.onActivityFailed(new ActivityResult(EXECUTION, "a", 1, false, null,
                "boom", "java.lang.NullPointerException"));

        verify(activityRepository).markFailed(eq(EXECUTION), eq("a"), eq("boom"),
                eq("java.lang.NullPointerException"), eq(2L));
        verify(deadLetterPublisher).publish(any(), eq("boom"), eq("java.lang.NullPointerException"));
        verify(activityRepository, never()).markRetryWaiting(any(), any(), anyInt(), any(), any(), any(), anyLong());
    }

    @Test
    void wildcardPatternMatchesErrorTypePrefix() {
        seed(new RetryPolicy(null, null, null, null, List.of("io.fuseflow.*")), null);
        when(activityRepository.findById(EXECUTION, "a"))
                .thenReturn(Optional.of(activity(ActivityStatus.STARTED, 1, 2)));
        when(activityRepository.markFailed(eq(EXECUTION), eq("a"), eq("boom"),
                eq("io.fuseflow.sdk.BadThing"), eq(2L))).thenReturn(true);

        retryManager.onActivityFailed(new ActivityResult(EXECUTION, "a", 1, false, null,
                "boom", "io.fuseflow.sdk.BadThing"));

        verify(activityRepository).markFailed(eq(EXECUTION), eq("a"), eq("boom"),
                eq("io.fuseflow.sdk.BadThing"), eq(2L));
    }

    @Test
    void taskPolicyOverridesWorkflowPolicy() {
        // Task policy allows more attempts than the workflow policy.
        seed(new RetryPolicy(2, null, null, null, null), new RetryPolicy(5, null, null, null, null));
        when(activityRepository.findById(EXECUTION, "a"))
                .thenReturn(Optional.of(activity(ActivityStatus.STARTED, 4, 4)));
        when(activityRepository.markRetryWaiting(eq(EXECUTION), eq("a"), anyInt(), any(), any(), any(), eq(4L)))
                .thenReturn(true);

        retryManager.onActivityFailed(failure(4));

        // Attempt 4 < task maxAttempts 5 → retried, not failed.
        verify(activityRepository).markRetryWaiting(eq(EXECUTION), eq("a"), eq(5), any(), any(), any(), eq(4L));
        verify(workflowFinalizer, never()).failWorkflow(any(), any());
    }

    @Test
    void ignoresStaleResultFromPreviousAttempt() {
        when(activityRepository.findById(EXECUTION, "a"))
                .thenReturn(Optional.of(activity(ActivityStatus.STARTED, 2, 4)));

        // Result says attempt 1, row is on attempt 2 — a stale redelivery.
        retryManager.onActivityFailed(failure(1));

        verify(activityRepository, never()).markFailed(any(), any(), any(), any(), anyLong());
        verify(activityRepository, never()).markRetryWaiting(any(), any(), anyInt(), any(), any(), any(), anyLong());
        verify(workflowFinalizer, never()).failWorkflow(any(), any());
        verify(deadLetterPublisher, never()).publish(any(), any(), any());
    }

    @Test
    void ignoresFailureForAlreadyTerminalActivity() {
        when(activityRepository.findById(EXECUTION, "a"))
                .thenReturn(Optional.of(activity(ActivityStatus.FAILED, 1, 4)));

        retryManager.onActivityFailed(failure(1));

        verify(activityRepository, never()).markFailed(any(), any(), any(), any(), anyLong());
        verify(activityRepository, never()).markRetryWaiting(any(), any(), anyInt(), any(), any(), any(), anyLong());
    }

    @Test
    void ignoresFailureForUnknownActivity() {
        when(activityRepository.findById(EXECUTION, "a")).thenReturn(Optional.empty());

        retryManager.onActivityFailed(failure(1));

        verifyNoInteractionsWithTransitions();
    }

    @Test
    void schedulesExponentialBackoffFromFailedAttempt() {
        seed(null, new RetryPolicy(null, 2, true, 3.0, null));
        when(activityRepository.findById(EXECUTION, "a"))
                .thenReturn(Optional.of(activity(ActivityStatus.STARTED, 2, 4)));
        when(activityRepository.markRetryWaiting(eq(EXECUTION), eq("a"), anyInt(), any(), any(), any(), eq(4L)))
                .thenReturn(true);

        retryManager.onActivityFailed(failure(2));

        // fixed 2s * 3^(2-1) = 6s after the failed attempt 2.
        ArgumentCaptor<Instant> due = ArgumentCaptor.forClass(Instant.class);
        verify(activityRepository).markRetryWaiting(eq(EXECUTION), eq("a"), eq(3), due.capture(), any(), any(), eq(4L));
        long seconds = java.time.Duration.between(Instant.now(), due.getValue()).getSeconds();
        assertThat(seconds).isBetween(5L, 7L);
    }

    private void verifyNoInteractionsWithTransitions() {
        verify(activityRepository, never()).markFailed(any(), any(), any(), any(), anyLong());
        verify(activityRepository, never()).markRetryWaiting(any(), any(), anyInt(), any(), any(), any(), anyLong());
        verify(workflowFinalizer, never()).failWorkflow(any(), any());
    }
}
