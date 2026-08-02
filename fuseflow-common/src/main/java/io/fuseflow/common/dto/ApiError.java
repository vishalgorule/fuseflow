package io.fuseflow.common.dto;

import io.fuseflow.common.correlation.CorrelationId;

import java.time.Instant;
import java.util.List;

/**
 * Uniform error response body used across all FuseFlow services.
 *
 * @param code          stable machine-readable error code
 * @param message       human-readable message
 * @param timestamp     when the error occurred
 * @param correlationId id of the request that failed (for log correlation)
 * @param errors        optional field-level validation errors (omitted when {@code null})
 */
public record ApiError(String code, String message, Instant timestamp, String correlationId, List<FieldError> errors) {

    /** A single field-level validation error. */
    public record FieldError(String field, String message) {
    }

    public static ApiError of(String code, String message) {
        return new ApiError(code, message, Instant.now(), CorrelationId.get(), null);
    }

    public static ApiError of(String code, String message, List<FieldError> errors) {
        return new ApiError(code, message, Instant.now(), CorrelationId.get(), errors);
    }
}
