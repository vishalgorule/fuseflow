package io.fuseflow.engine.retry;

import io.fuseflow.engine.config.ReliabilityProperties;
import io.fuseflow.engine.dispatch.ActivityResult;
import io.fuseflow.engine.model.ActivityExecution;
import io.fuseflow.engine.repository.ActivityExecutionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;

/**
 * Timeout manager (Phase 7, FR-7): converts activities that never make progress into failed
 * attempts, which the {@link RetryManager} then retries (or fails) per policy.
 *
 * <ul>
 *   <li><b>start timeout</b> — a SCHEDULED activity that never received a STARTED signal
 *       within {@code fuseflow.engine.timeout.start} (covers unroutable tasks and dispatches
 *       nobody picked up);</li>
 *   <li><b>execution timeout</b> — a STARTED activity that produced no result within
 *       {@code fuseflow.engine.timeout.execution} (covers hung or dead workers; the registry's
 *       heartbeat detection already routed their pool OFFLINE, so the retry lands on a live
 *       worker).</li>
 * </ul>
 *
 * <p>Retry-waiting rows (SCHEDULED with a future {@code retry_due_at}) are on the retry clock,
 * not the start clock, and are excluded — the {@link RetryScheduler} owns them.
 */
@Component
public class TimeoutManager {

    private static final Logger log = LoggerFactory.getLogger(TimeoutManager.class);

    private final ActivityExecutionRepository activityRepository;
    private final RetryManager retryManager;
    private final ReliabilityProperties properties;

    public TimeoutManager(ActivityExecutionRepository activityRepository,
                          RetryManager retryManager,
                          ReliabilityProperties properties) {
        this.activityRepository = activityRepository;
        this.retryManager = retryManager;
        this.properties = properties;
    }

    @Scheduled(fixedDelayString = "${fuseflow.engine.poll-interval:5s}")
    public void checkTimeouts() {
        Duration startTimeout = properties.getTimeout().getStart();
        Instant startCutoff = Instant.now().minus(startTimeout);
        for (ActivityExecution activity : activityRepository.findStartTimeouts(startCutoff)) {
            log.warn("Activity {} of execution {} never started within {} — treating as failed attempt {}",
                    activity.taskId(), activity.workflowExecutionId(), startTimeout, activity.attempt());
            retryManager.onActivityFailed(failure(activity, "start timeout after " + startTimeout.toSeconds() + "s"));
        }

        Duration executionTimeout = properties.getTimeout().getExecution();
        Instant executionCutoff = Instant.now().minus(executionTimeout);
        for (ActivityExecution activity : activityRepository.findExecutionTimeouts(executionCutoff)) {
            log.warn("Activity {} of execution {} produced no result within {} — treating as failed attempt {}",
                    activity.taskId(), activity.workflowExecutionId(), executionTimeout, activity.attempt());
            retryManager.onActivityFailed(failure(activity, "execution timeout after " + executionTimeout.toSeconds() + "s"));
        }
    }

    private static ActivityResult failure(ActivityExecution activity, String error) {
        return new ActivityResult(activity.workflowExecutionId(), activity.taskId(), activity.attempt(),
                false, null, error, null);
    }
}
