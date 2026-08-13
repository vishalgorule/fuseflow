package io.fuseflow.registry.dto;

import io.fuseflow.common.dto.WorkerResponse;

/**
 * Result of a register call: the worker plus whether this registration created a new worker
 * or updated an existing one (drives the 201 vs 200 response status).
 */
public record WorkerRegistration(WorkerResponse worker, boolean created) {
}
