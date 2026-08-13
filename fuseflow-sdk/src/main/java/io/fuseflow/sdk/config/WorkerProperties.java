package io.fuseflow.sdk.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.UUID;

/**
 * Worker runtime configuration ({@code fuseflow.worker.*}). All fields are optional — the
 * {@code FuseFlowWorker} applies sensible defaults (random worker id, local host, capacity 1,
 * 5s heartbeats, group {@code fuseflow-workers}, 10 registration retries at 2s).
 *
 * @param id                 stable worker identity; generated per boot when null
 * @param host               host identifier advertised to the registry
 * @param capacity           max concurrent activities this worker runs
 * @param heartbeatInterval  how often to heartbeat the registry
 * @param groupId            Kafka consumer group (workers sharing capabilities usually share it)
 * @param registerRetries    registration attempts before giving up at boot
 * @param registerRetryDelay delay between registration attempts
 */
@ConfigurationProperties(prefix = "fuseflow.worker")
public record WorkerProperties(
        UUID id,
        String host,
        Integer capacity,
        Duration heartbeatInterval,
        String groupId,
        Integer registerRetries,
        Duration registerRetryDelay) {
}
