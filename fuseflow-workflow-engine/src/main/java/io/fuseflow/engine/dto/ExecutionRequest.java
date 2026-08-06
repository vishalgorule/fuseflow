package io.fuseflow.engine.dto;

import tools.jackson.databind.JsonNode;

import java.util.UUID;

/**
 * Request body for starting a workflow execution.
 *
 * @param workflowId the workflow definition to run (must exist in the definition schema)
 * @param input      optional JSON input passed to every activity of the execution
 */
public record ExecutionRequest(UUID workflowId, JsonNode input) {
}
