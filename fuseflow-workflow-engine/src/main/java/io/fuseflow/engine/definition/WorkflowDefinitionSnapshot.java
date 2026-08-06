package io.fuseflow.engine.definition;

import java.util.List;
import java.util.UUID;

/**
 * Read-only snapshot of a workflow definition as seen by the engine at execution start.
 * Populated by {@link WorkflowDefinitionReader} from the definition service's {@code definition}
 * schema; the engine never mutates another service's tables.
 */
public record WorkflowDefinitionSnapshot(UUID id, String name, long version, List<Task> tasks) {

    public record Task(String id, String activity, List<String> dependsOn) {
    }
}
