package io.fuseflow.engine.registry;

import io.fuseflow.common.dto.WorkerResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.util.List;

/**
 * Engine-side client for the worker registry (Phase 5): used to seed the pool routing table.
 * The registry is only consulted on boot and on {@code worker-events} — never on the dispatch
 * hot path (the {@link PoolRoutingTable} cache serves dispatches).
 */
@Component
public class EngineRegistryClient {

    private static final Logger log = LoggerFactory.getLogger(EngineRegistryClient.class);

    private final RestClient restClient;

    public EngineRegistryClient(@Value("${fuseflow.registry.base-url:http://localhost:8083}") String registryBaseUrl) {
        this.restClient = RestClient.builder().baseUrl(registryBaseUrl).build();
    }

    /** All registered workers (any status) — the source for the routing table seed. */
    public List<WorkerResponse> listWorkers() {
        WorkerResponse[] workers = restClient.get()
                .uri("/api/v1/workers")
                .retrieve()
                .body(WorkerResponse[].class);
        return workers == null ? List.of() : List.of(workers);
    }

    /** Whether the registry is reachable; logs at debug to keep boot quiet when it is down. */
    public boolean isReachable() {
        try {
            listWorkers();
            return true;
        } catch (Exception ex) {
            log.debug("Worker registry unreachable: {}", ex.getMessage());
            return false;
        }
    }
}
