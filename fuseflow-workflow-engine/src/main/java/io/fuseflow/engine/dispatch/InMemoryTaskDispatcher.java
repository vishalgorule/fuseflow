package io.fuseflow.engine.dispatch;

import io.fuseflow.common.messaging.ActivityTask;
import io.fuseflow.engine.service.ActivityStateService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import io.fuseflow.engine.service.ResultHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

/**
 * Phase 2 {@link TaskDispatcher} (opt-in via {@code fuseflow.engine.dispatch-mode=in-memory}):
 * runs each {@link ActivityTask} on a worker thread pool, delegating the actual work to an
 * {@link ActivityExecutor} (demo auto-completer in the app, deterministic fakes in tests).
 * Since Phase 4 the {@link KafkaTaskDispatcher} is the default; this in-memory variant is kept
 * for tests and the demo auto-complete mode.
 *
 * <p>Flow per task: mark STARTED (durable, transactional) → execute → hand the result to the
 * {@link ResultHandler}. Executor exceptions are converted to failed results so the state
 * machine always advances.
 *
 * <p>{@code ResultHandler} is injected lazily: the runtime loop
 * {@code Scheduler → TaskDispatcher → ResultHandler → Scheduler} is a circular bean graph that
 * only resolves at runtime (after-commit dispatch and worker threads), so the cycle is broken
 * with a lazy proxy.
 */
@Component
@ConditionalOnProperty(name = "fuseflow.engine.dispatch-mode", havingValue = "in-memory")
public class InMemoryTaskDispatcher implements TaskDispatcher {

    private static final Logger log = LoggerFactory.getLogger(InMemoryTaskDispatcher.class);

    private final ThreadPoolTaskExecutor executor;
    private final ActivityStateService activityState;
    private final ActivityExecutor activityExecutor;
    private final ResultHandler resultHandler;

    public InMemoryTaskDispatcher(ThreadPoolTaskExecutor executor,
                                  ActivityStateService activityState,
                                  ActivityExecutor activityExecutor,
                                  @Lazy ResultHandler resultHandler) {
        this.executor = executor;
        this.activityState = activityState;
        this.activityExecutor = activityExecutor;
        this.resultHandler = resultHandler;
    }

    @Override
    public void dispatch(ActivityTask task) {
        executor.execute(() -> {
            try {
                if (!activityState.startActivity(task.executionId(), task.taskId(), task.attempt())) {
                    // Stale dispatch (already terminal or from a previous attempt) — nothing to do.
                    return;
                }
                ActivityResult result = activityExecutor.execute(task);
                resultHandler.handleResult(result);
            } catch (Exception ex) {
                log.error("Activity execution failed for task {} of execution {}",
                        task.taskId(), task.executionId(), ex);
                resultHandler.handleResult(ActivityResult.failure(task, ex.getMessage()));
            }
        });
    }
}
