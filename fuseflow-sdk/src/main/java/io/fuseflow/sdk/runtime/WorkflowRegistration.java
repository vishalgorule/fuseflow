package io.fuseflow.sdk.runtime;

import io.fuseflow.common.dto.WorkflowRequest;

/**
 * A workflow discovered by the {@link WorkflowScanner} and ready for registration (Phase 6).
 * The {@link WorkflowRequest} is the shared wire contract the definition service validates
 * and persists. Pool/concurrency are worker concerns ({@code fuseflow.worker.*}) and never
 * part of a workflow definition.
 *
 * @param request the definition to register (name + DAG)
 */
public record WorkflowRegistration(WorkflowRequest request) {

    public String name() {
        return request.name();
    }
}
