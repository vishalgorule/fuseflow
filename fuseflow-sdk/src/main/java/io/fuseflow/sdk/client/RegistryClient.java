package io.fuseflow.sdk.client;

import io.fuseflow.common.dto.WorkerRequest;
import io.fuseflow.common.dto.WorkerResponse;
import org.springframework.web.client.RestClient;

import java.util.List;
import java.util.UUID;

/**
 * Thin REST client for the worker registry (Phase 4, FR-4): registration, heartbeats and
 * deregistration. The registry API contract (DTOs) is shared via {@code fuseflow-common}.
 */
public class RegistryClient {

    private final RestClient restClient;

    public RegistryClient(String registryBaseUrl) {
        this.restClient = RestClient.builder().baseUrl(registryBaseUrl).build();
    }

    /**
     * Registers (or re-registers) the worker; the registry upserts on the client-supplied id.
     * {@code poolName} is the worker pool (Phase 5) — null lets the registry default it;
     * {@code concurrency} is the pool-level declared parallelism driving the pool queue's
     * partition count (null defaults to 1).
     */
    public WorkerResponse register(UUID id, String host, List<String> activities,
                                   String poolName, Integer concurrency) {
        return restClient.post()
                .uri("/api/v1/workers")
                .body(new WorkerRequest(id, host, activities, poolName, concurrency))
                .retrieve()
                .body(WorkerResponse.class);
    }

    /** Sends a heartbeat — a liveness touch; the registry derives liveness from freshness. */
    public void heartbeat(UUID id) {
        restClient.post()
                .uri("/api/v1/workers/{id}/heartbeat", id)
                .retrieve()
                .toBodilessEntity();
    }

    /** Deregisters the worker (called on shutdown). */
    public void deregister(UUID id) {
        restClient.delete()
                .uri("/api/v1/workers/{id}", id)
                .retrieve()
                .toBodilessEntity();
    }
}
