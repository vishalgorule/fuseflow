package io.fuseflow.sdk.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.UUID;

/**
 * Worker runtime configuration ({@code fuseflow.worker.*}). All fields are optional — the
 * {@code FuseFlowWorker} applies sensible defaults (random worker id, local host, 5s heartbeats,
 * pool {@code default}, 10 registration retries at 2s). One knob — the {@code pool} — drives the
 * routing key, the pool's dispatch queue and the consumer group, so they can never diverge.
 *
 * @param id               stable worker identity; generated per boot when null
 * @param host             host identifier advertised to the registry
 * @param heartbeatInterval how often to heartbeat the registry
 * @param pool             the pool this worker joins (Phase 5): instances with the same pool
 *                         share its dispatch queue ({@code fuseflow-pool.<pool>}) and consumer
 *                         group for parallelism; different pools are routed to independently.
 *                         null defaults to {@code default}
 * @param concurrency      pool-level declared parallelism (Phase 5) — drives the pool queue's
 *                         partition count; null defaults to 1
 * @param registerRetries  registration attempts before giving up at boot
 * @param registerRetryDelay delay between registration attempts
 */
@ConfigurationProperties(prefix = "fuseflow.worker")
public record WorkerProperties(
        UUID id,
        String host,
        Duration heartbeatInterval,
        String pool,
        Integer concurrency,
        Integer registerRetries,
        Duration registerRetryDelay) {
}
