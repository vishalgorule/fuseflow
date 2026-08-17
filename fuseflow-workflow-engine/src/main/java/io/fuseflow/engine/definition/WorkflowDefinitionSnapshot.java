package io.fuseflow.engine.definition;

import io.fuseflow.common.dto.RetryPolicy;

import java.util.List;
import java.util.UUID;

/**
 * Read-only snapshot of a workflow definition as seen by the engine at execution start.
 * Populated by {@link WorkflowDefinitionReader} from the definition service's {@code definition}
 * schema; the engine never mutates another service's tables.
 *
 * @param retryPolicy workflow-level retry policy (Phase 7), null when unset
 */
public record WorkflowDefinitionSnapshot(UUID id, String name, long version, RetryPolicy retryPolicy,
                                         List<Task> tasks) {

    /** Convenience constructor for snapshots without a retry policy. */
    public WorkflowDefinitionSnapshot(UUID id, String name, long version, List<Task> tasks) {
        this(id, name, version, null, tasks);
    }

    public record Task(String id, String activity, List<String> dependsOn, RetryPolicy retryPolicy) {

        /** Convenience constructor for tasks without a retry policy. */
        public Task(String id, String activity, List<String> dependsOn) {
            this(id, activity, dependsOn, null);
        }
    }
}
