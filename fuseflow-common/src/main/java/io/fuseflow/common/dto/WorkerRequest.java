package io.fuseflow.common.dto;

import java.util.List;
import java.util.UUID;

/**
 * Request body for registering (or re-registering) a worker — the REST contract between the
 * worker SDK and the registry (Phase 3/4).
 *
 * @param id         stable worker identity, chosen by the worker and kept across restarts
 * @param host       identifier of the host the worker runs on
 * @param capacity   max concurrent activities the worker can run (defaults to 1)
 * @param activities activity names the worker advertises
 */
public record WorkerRequest(UUID id, String host, Integer capacity, List<String> activities) {
}
