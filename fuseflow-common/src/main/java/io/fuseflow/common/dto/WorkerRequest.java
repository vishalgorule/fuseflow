package io.fuseflow.common.dto;

import java.util.List;
import java.util.UUID;

/**
 * Request body for registering (or re-registering) a worker — the REST contract between the
 * worker SDK and the registry (Phase 3/4).
 *
 * @param id          stable worker identity, chosen by the worker and kept across restarts
 * @param host        identifier of the host the worker runs on
 * @param activities  activity names the worker advertises
 * @param poolName    the worker pool (capability group) this worker joins (Phase 5); workers in
 *                    the same pool share a consumer group and the pool's dispatch topic. Null
 *                    defaults to {@code default}.
 * @param concurrency pool-level declared parallelism (Phase 5) — drives the pool topic's
 *                    partition count. Null defaults to 1.
 */
public record WorkerRequest(
        UUID id,
        String host,
        List<String> activities,
        String poolName,
        Integer concurrency) {

    /** Backward-compatible convenience for callers without pool identity (pool = default). */
    public WorkerRequest(UUID id, String host, List<String> activities) {
        this(id, host, activities, null, null);
    }
}
