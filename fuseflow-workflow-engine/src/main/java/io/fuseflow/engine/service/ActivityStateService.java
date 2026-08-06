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

    @Transactional
    public boolean startActivity(UUID executionId, String taskId) {
        ActivityExecution activity = activityRepository.findById(executionId, taskId).orElse(null);
        if (activity == null || !activityRepository.markStarted(executionId, taskId, activity.version())) {
            return false;
        }
        eventStore.append(executionId, "ActivityStarted",
                Map.of("taskId", taskId, "activityName", activity.activityName()));
        return true;
    }
}
