package io.fuseflow.engine.config;

import org.springframework.context.annotation.Configuration;

/**
 * Engine configuration (Phase 4+). The in-memory dispatch mode (Phase 2) has been removed;
 * all activity dispatch goes through Kafka via {@code KafkaTaskDispatcher} and the dispatch
 * outbox.
 */
@Configuration
public class EngineConfig {
}
