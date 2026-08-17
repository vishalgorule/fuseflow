package io.fuseflow.engine.service;

import io.fuseflow.engine.model.ActivityExecution;
import io.fuseflow.engine.repository.ActivityExecutionRepository;
import io.fuseflow.engine.repository.EventStore;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;
import java.util.UUID;

/**
 * Durable SCHEDULED/STARTED → STARTED transition, invoked by the dispatcher before executing
 * an activity. Returns {@code false} for stale dispatches (activity already terminal), so the
 * dispatcher skips execution and never duplicates work.
 */
@Service
public class ActivityStateService {

    private final ActivityExecutionRepository activityRepository;
    private final EventStore eventStore;

    public ActivityStateService(ActivityExecutionRepository activityRepository, EventStore eventStore) {
        this.activityRepository = activityRepository;
        this.eventStore = eventStore;
    }

    /**
     * Durable SCHEDULED/STARTED → STARTED transition, invoked by the dispatcher before executing
     * an activity. Returns {@code false} for stale dispatches (activity already terminal or the
     * signal's {@code attempt} does not match the row — a stale redelivery from a previous
     * attempt must never mark a newer one).
     */
    @Transactional
    public boolean startActivity(UUID executionId, String taskId, int attempt) {
        ActivityExecution activity = activityRepository.findById(executionId, taskId).orElse(null);
        if (activity == null || attempt != activity.attempt()
                || !activityRepository.markStarted(executionId, taskId, activity.version())) {
            return false;
        }
        eventStore.append(executionId, "ActivityStarted",
                Map.of("taskId", taskId, "activityName", activity.activityName(), "attempt", attempt));
        return true;
    }
}
