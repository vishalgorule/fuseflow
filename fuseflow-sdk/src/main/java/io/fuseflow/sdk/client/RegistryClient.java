package io.fuseflow.sdk.client;

import io.fuseflow.common.dto.HeartbeatRequest;
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

    /** Registers (or re-registers) the worker; the registry upserts on the client-supplied id. */
    public WorkerResponse register(UUID id, String host, int capacity, List<String> activities) {
        return restClient.post()
                .uri("/api/v1/workers")
                .body(new WorkerRequest(id, host, capacity, activities))
                .retrieve()
                .body(WorkerResponse.class);
    }

    /** Sends a heartbeat, optionally refreshing the reported capacity. */
    public void heartbeat(UUID id, int capacity) {
        restClient.post()
                .uri("/api/v1/workers/{id}/heartbeat", id)
                .body(new HeartbeatRequest(capacity))
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
