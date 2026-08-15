package io.fuseflow.common.dto;

/**
 * Response of the registry's discovery endpoint ({@code GET /api/v1/config}) — the single
 * infrastructure fact a worker needs beyond the registry itself. The registry is the one
 * endpoint users configure; everything else (broker address, topics) is SDK-internal.
 *
 * @param bootstrapServers the Kafka broker address the registry uses (comma-separated list allowed)
 */
public record ClusterConfigResponse(String bootstrapServers) {
}
