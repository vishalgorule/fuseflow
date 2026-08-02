package io.fuseflow.common.exception;

import io.fuseflow.common.dto.ApiError;
import org.springframework.http.HttpStatus;

import java.util.List;

/**
 * Base exception for all FuseFlow API errors. Carries an HTTP status, a stable
 * machine-readable error code and, optionally, field-level validation errors.
 */
public class ApiException extends RuntimeException {

    private final HttpStatus status;
    private final String code;
    private final List<ApiError.FieldError> errors;

    public ApiException(HttpStatus status, String code, String message) {
        this(status, code, message, null);
    }

    public ApiException(HttpStatus status, String code, String message, List<ApiError.FieldError> errors) {
        super(message);
        this.status = status;
        this.code = code;
        this.errors = errors;
    }

    public static ApiException badRequest(String code, String message) {
        return new ApiException(HttpStatus.BAD_REQUEST, code, message);
    }

    public static ApiException badRequest(String code, String message, List<ApiError.FieldError> errors) {
        return new ApiException(HttpStatus.BAD_REQUEST, code, message, errors);
    }

    public static ApiException notFound(String code, String message) {
        return new ApiException(HttpStatus.NOT_FOUND, code, message);
    }

    public static ApiException conflict(String code, String message) {
        return new ApiException(HttpStatus.CONFLICT, code, message);
    }

    public HttpStatus getStatus() {
        return status;
    }

    public String getCode() {
        return code;
    }

    public List<ApiError.FieldError> getErrors() {
        return errors;
    }
}
