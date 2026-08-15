package io.fuseflow.registry.controller;

import io.fuseflow.common.dto.ClusterConfigResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Service discovery endpoint (Phase 5): the registry is the single endpoint a worker is
 * configured with; {@code GET /api/v1/config} tells the worker where the broker is so the SDK
 * can configure its transport without the user knowing any Kafka internals.
 */
@RestController
@RequestMapping("/api/v1/config")
public class ConfigController {

    private final String bootstrapServers;

    public ConfigController(@Value("${spring.kafka.bootstrap-servers:localhost:9092}") String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    @GetMapping
    public ClusterConfigResponse get() {
        return new ClusterConfigResponse(bootstrapServers);
    }
}
