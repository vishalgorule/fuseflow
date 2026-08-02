package io.fuseflow.common.dto;

import io.fuseflow.common.correlation.CorrelationId;

import java.time.Instant;

/**
 * Uniform error response body used across all FuseFlow services.
 *
 * @param code         stable machine-readable error code
 * @param message      human-readable message
 * @param timestamp    when the error occurred
 * @param correlationId id of the request that failed (for log correlation)
 */
public record ApiError(String code, String message, Instant timestamp, String correlationId) {

    public static ApiError of(String code, String message) {
        return new ApiError(code, message, Instant.now(), CorrelationId.get());
    }
}
