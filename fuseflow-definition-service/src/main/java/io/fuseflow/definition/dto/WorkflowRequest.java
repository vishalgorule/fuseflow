package io.fuseflow.definition.dto;

import java.util.List;

/**
 * Request body for registering or replacing a workflow definition.
 *
 * @param name        unique workflow name
 * @param description optional human-readable description
 * @param tasks       the DAG tasks; each carries its own {@code dependsOn} list
 */
public record WorkflowRequest(String name, String description, List<Task> tasks) {

    public record Task(String id, String activity, List<String> dependsOn) {
    }
}
