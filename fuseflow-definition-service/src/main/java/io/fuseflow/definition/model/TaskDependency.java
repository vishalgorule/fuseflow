package io.fuseflow.definition.model;

import java.util.UUID;

/** A directed edge in a workflow DAG: {@code taskId} depends on {@code dependsOn}. */
public record TaskDependency(UUID workflowId, String taskId, String dependsOn) {
}
