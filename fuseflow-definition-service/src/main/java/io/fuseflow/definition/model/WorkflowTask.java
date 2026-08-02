package io.fuseflow.definition.model;

import java.util.UUID;

/** A single task (node) within a workflow DAG. */
public record WorkflowTask(UUID workflowId, String taskId, String activityName) {
}
