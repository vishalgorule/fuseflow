package io.fuseflow.definition.dto;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** Full representation of a workflow definition returned by the API. */
public record WorkflowResponse(
        UUID id,
        String name,
        String description,
        List<Task> tasks,
        long version,
        Instant createdAt,
        Instant updatedAt) {

    public record Task(String id, String activity, List<String> dependsOn) {
    }
}
