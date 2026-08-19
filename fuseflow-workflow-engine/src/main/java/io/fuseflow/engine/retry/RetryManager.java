package io.fuseflow.engine.retry;

import io.fuseflow.common.dto.RetryPolicy;
import io.fuseflow.engine.config.ReliabilityProperties;
import io.fuseflow.engine.definition.WorkflowDefinitionReader;
import io.fuseflow.engine.definition.WorkflowDefinitionSnapshot;
import io.fuseflow.engine.dispatch.ActivityResult;
import io.fuseflow.engine.model.ActivityExecution;
import io.fuseflow.engine.model.ActivityStatus;
import io.fuseflow.engine.model.WorkflowExecution;
import io.fuseflow.engine.messaging.WorkflowEventPublisher;
import io.fuseflow.engine.repository.ActivityExecutionRepository;
import io.fuseflow.engine.repository.EventStore;
import io.fuseflow.engine.repository.WorkflowExecutionRepository;
import io.fuseflow.engine.service.WorkflowFinalizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Retry manager (Phase 7, FR-6): decides what a failed activity attempt means.
 *
 * <p>Resolution order for each knob of the retry policy: <b>task policy → workflow policy →
 * engine defaults</b> ({@code fuseflow.engine.retry.*}). A failure is retried when it is not
 * classified non-retryable ({@code nonRetryableExceptions} matched by exception class name)
 * and the attempt count is below {@code maxAttempts} — the row is parked as SCHEDULED on the
 * DB-polled due-time queue ({@code retry_due_at}) with the attempt bumped, and the
 * {@link RetryScheduler} re-dispatches it when due. When retries are exhausted or the failure
 * is non-retryable, the activity is marked FAILED, the execution fails, and the failure is
 * dead-lettered.
 *
 * <p>Idempotency: only in-flight activities (SCHEDULED/STARTED) whose {@code attempt} matches
 * the result are acted on — a stale redelivery from a previous attempt is ignored, and the
 * terminal/retry transitions are version-guarded so concurrent engines never double-act.
 */
@Service
public class RetryManager {

    private static final Logger log = LoggerFactory.getLogger(RetryManager.class);

    private final ActivityExecutionRepository activityRepository;
    private final WorkflowExecutionRepository executionRepository;
    private final WorkflowDefinitionReader definitionReader;
    private final EventStore eventStore;
    private final WorkflowFinalizer workflowFinalizer;
    private final ReliabilityProperties properties;
    private final ObjectProvider<DeadLetterPublisher> deadLetterPublisher;
    private final WorkflowEventPublisher workflowEventPublisher;

    public RetryManager(ActivityExecutionRepository activityRepository,
                        WorkflowExecutionRepository executionRepository,
                        WorkflowDefinitionReader definitionReader,
                        EventStore eventStore,
                        WorkflowFinalizer workflowFinalizer,
                        ReliabilityProperties properties,
                        ObjectProvider<DeadLetterPublisher> deadLetterPublisher,
                        WorkflowEventPublisher workflowEventPublisher) {
        this.activityRepository = activityRepository;
        this.executionRepository = executionRepository;
        this.definitionReader = definitionReader;
        this.eventStore = eventStore;
        this.workflowFinalizer = workflowFinalizer;
        this.properties = properties;
        this.deadLetterPublisher = deadLetterPublisher;
        this.workflowEventPublisher = workflowEventPublisher;
    }

    /**
     * Handles a failed attempt (from the {@code ResultHandler} on FAILED signals or from the
     * {@code TimeoutManager} for start/execution timeouts). Retries per policy or fails the
     * activity and the workflow when attempts are exhausted / the failure is non-retryable.
     */
    public void onActivityFailed(ActivityResult result) {
        ActivityExecution activity = activityRepository.findById(result.executionId(), result.taskId()).orElse(null);
        if (activity == null || (activity.status() != ActivityStatus.STARTED
                && activity.status() != ActivityStatus.SCHEDULED) || result.attempt() != activity.attempt()) {
            // Duplicate, stale or already-terminal result — ignore.
            log.debug("Ignoring stale failure for task {} of execution {} (attempt {})",
                    result.taskId(), result.executionId(), result.attempt());
            return;
        }

        String error = result.error() == null ? "activity failed" : result.error();
        String errorType = result.errorType();
        RetryPolicy policy = resolvePolicy(activity);
        if (isNonRetryable(policy, errorType) || activity.attempt() >= maxAttempts(policy)) {
            failTerminal(activity, error, errorType, policy);
        } else {
            scheduleRetry(activity, error, errorType, policy);
        }
    }

    // ---------------------------------------------------------------- internals

    private void scheduleRetry(ActivityExecution activity, String error, String errorType, RetryPolicy policy) {
        int newAttempt = activity.attempt() + 1;
        Instant dueAt = dueAt(activity.attempt(), policy);
        if (activityRepository.markRetryWaiting(activity.workflowExecutionId(), activity.taskId(),
                newAttempt, dueAt, error, errorType, activity.version())) {
            eventStore.append(activity.workflowExecutionId(), "ActivityRetryScheduled", payload(
                    "taskId", activity.taskId(),
                    "activityName", activity.activityName(),
                    "attempt", newAttempt,
                    "retryDueAt", dueAt.toString(),
                    "error", error,
                    "errorType", errorType));
            // Option B: tell workers the just-failed attempt is superseded, so any queued
            // message for it is skipped instead of executed (a worker-side control signal on
            // the workflow-events topic — the DB attempt guard remains the source of truth).
            workflowEventPublisher.publish(activity.workflowExecutionId(), "ActivitySuperseded", payload(
                    "taskId", activity.taskId(),
                    "activityName", activity.activityName(),
                    "supersededAttempt", activity.attempt(),
                    "newAttempt", newAttempt));
            log.info("Activity {} of execution {} failed on attempt {} — retry {} due {}",
                    activity.taskId(), activity.workflowExecutionId(), activity.attempt(),
                    newAttempt, dueAt);
        }
    }

    private void failTerminal(ActivityExecution activity, String error, String errorType, RetryPolicy policy) {
        if (activityRepository.markFailed(activity.workflowExecutionId(), activity.taskId(),
                error, errorType, activity.version())) {
            eventStore.append(activity.workflowExecutionId(), "ActivityFailed", payload(
                    "taskId", activity.taskId(),
                    "activityName", activity.activityName(),
                    "attempt", activity.attempt(),
                    "error", error,
                    "errorType", errorType));
            deadLetterPublisher.ifAvailable(publisher -> publisher.publish(activity, error, errorType));
            workflowFinalizer.failWorkflow(activity.workflowExecutionId(), error);
            log.info("Activity {} of execution {} failed terminally (attempt {}) — workflow failed",
                    activity.taskId(), activity.workflowExecutionId(), activity.attempt());
        }
    }

    /** Effective policy: task → workflow → engine defaults, per knob. */
    private RetryPolicy resolvePolicy(ActivityExecution activity) {
        RetryPolicy taskPolicy = null;
        RetryPolicy workflowPolicy = null;
        WorkflowExecution execution = executionRepository.findById(activity.workflowExecutionId()).orElse(null);
        if (execution != null) {
            WorkflowDefinitionSnapshot snapshot = definitionReader.find(execution.workflowId()).orElse(null);
            if (snapshot != null) {
                workflowPolicy = snapshot.retryPolicy();
                taskPolicy = snapshot.tasks().stream()
                        .filter(task -> task.id().equals(activity.taskId()))
                        .map(WorkflowDefinitionSnapshot.Task::retryPolicy)
                        .filter(java.util.Objects::nonNull)
                        .findFirst().orElse(null);
            }
        }
        ReliabilityProperties.Retry defaults = properties.getRetry();
        return new RetryPolicy(
                firstNonNull(policy(taskPolicy, RetryPolicy::maxAttempts),
                        policy(workflowPolicy, RetryPolicy::maxAttempts), defaults.getDefaultMaxAttempts()),
                firstNonNull(policy(taskPolicy, RetryPolicy::fixedDelaySeconds),
                        policy(workflowPolicy, RetryPolicy::fixedDelaySeconds),
                        (int) defaults.getDefaultFixedDelay().toSeconds()),
                firstNonNull(policy(taskPolicy, RetryPolicy::exponentialBackoff),
                        policy(workflowPolicy, RetryPolicy::exponentialBackoff),
                        defaults.isDefaultExponentialBackoff()),
                firstNonNull(policy(taskPolicy, RetryPolicy::backoffMultiplier),
                        policy(workflowPolicy, RetryPolicy::backoffMultiplier),
                        defaults.getDefaultBackoffMultiplier()),
                union(taskPolicy, workflowPolicy, defaults.getDefaultNonRetryableExceptions()));
    }

    private static <T> T policy(RetryPolicy policy, java.util.function.Function<RetryPolicy, T> getter) {
        return policy == null ? null : getter.apply(policy);
    }

    /** Matches the failure's exception class name (exact, or trailing {@code *} prefix). */
    private static boolean isNonRetryable(RetryPolicy policy, String errorType) {
        if (errorType == null || errorType.isBlank() || policy.nonRetryableExceptions() == null) {
            return false;
        }
        for (String pattern : policy.nonRetryableExceptions()) {
            if (pattern == null || pattern.isBlank()) {
                continue;
            }
            if (pattern.endsWith("*")) {
                if (errorType.startsWith(pattern.substring(0, pattern.length() - 1))) {
                    return true;
                }
            } else if (pattern.equals(errorType)) {
                return true;
            }
        }
        return false;
    }

    private int maxAttempts(RetryPolicy policy) {
        return policy.maxAttempts() == null ? 1 : policy.maxAttempts();
    }

    /** Delay until the next attempt: fixed, or {@code base * multiplier^(failedAttempt-1)}. */
    private static Instant dueAt(int justFailedAttempt, RetryPolicy policy) {
        long baseSeconds = policy.fixedDelaySeconds() == null ? 5 : policy.fixedDelaySeconds();
        double delay = baseSeconds;
        if (Boolean.TRUE.equals(policy.exponentialBackoff())) {
            double multiplier = policy.backoffMultiplier() == null ? 2.0 : policy.backoffMultiplier();
            delay = baseSeconds * Math.pow(multiplier, justFailedAttempt - 1);
        }
        return Instant.now().plusSeconds((long) delay);
    }

    /** Concatenates the non-retryable patterns from every level (task, workflow, defaults). */
    private static List<String> union(RetryPolicy task, RetryPolicy workflow, List<String> defaults) {
        List<String> result = new ArrayList<>();
        addPatterns(result, task == null ? null : task.nonRetryableExceptions());
        addPatterns(result, workflow == null ? null : workflow.nonRetryableExceptions());
        addPatterns(result, defaults);
        return result;
    }

    private static void addPatterns(List<String> target, List<String> patterns) {
        if (patterns != null) {
            for (String pattern : patterns) {
                if (pattern != null && !pattern.isBlank() && !target.contains(pattern)) {
                    target.add(pattern);
                }
            }
        }
    }

    @SafeVarargs
    private static <T> T firstNonNull(T... values) {
        for (T value : values) {
            if (value != null) {
                return value;
            }
        }
        return null;
    }

    private static Map<String, Object> payload(Object... kv) {
        Map<String, Object> payload = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            payload.put(String.valueOf(kv[i]), kv[i + 1]);
        }
        return payload;
    }
}
