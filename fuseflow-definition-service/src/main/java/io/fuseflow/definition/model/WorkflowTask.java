package io.fuseflow.definition.model;

import java.util.UUID;

/**
 * A single task (node) within a workflow DAG.
 *
 * @param retryPolicyJson raw JSON of the per-task retry policy (Phase 7), null when unset
 */
public record WorkflowTask(UUID workflowId, String taskId, String activityName, String retryPolicyJson) {

    /** Convenience constructor for tasks without a retry policy. */
    public WorkflowTask(UUID workflowId, String taskId, String activityName) {
        this(workflowId, taskId, activityName, null);
    }
}
