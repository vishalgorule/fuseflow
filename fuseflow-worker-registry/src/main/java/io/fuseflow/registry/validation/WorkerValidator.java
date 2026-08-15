package io.fuseflow.registry.validation;

import io.fuseflow.common.dto.ApiError;
import io.fuseflow.common.dto.WorkerRequest;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Validates worker registration payloads (FR-4). Structural checks only: identity, host and a
 * non-empty, non-duplicate activity list. Whether a worker capable of an activity exists is a
 * schedule-time concern of the engine — the registry accepts any names.
 */
@Component
public final class WorkerValidator {

    /** Returns the field-level errors for a registration request; empty = valid. */
    public List<ApiError.FieldError> validate(WorkerRequest request) {
        List<ApiError.FieldError> errors = new ArrayList<>();

        if (request == null) {
            return List.of(new ApiError.FieldError("body", "request body is required"));
        }
        if (request.id() == null) {
            errors.add(new ApiError.FieldError("id", "worker id is required"));
        }
        if (request.host() == null || request.host().isBlank()) {
            errors.add(new ApiError.FieldError("host", "host is required"));
        }
        if (request.poolName() != null && request.poolName().isBlank()) {
            errors.add(new ApiError.FieldError("poolName", "pool name must not be blank"));
        }
        if (request.concurrency() != null && request.concurrency() < 1) {
            errors.add(new ApiError.FieldError("concurrency", "concurrency must be at least 1"));
        }

        List<String> activities = request.activities();
        if (activities == null || activities.isEmpty()) {
            errors.add(new ApiError.FieldError("activities", "at least one activity is required"));
            return List.copyOf(errors);
        }
        Set<String> seen = new HashSet<>();
        for (int i = 0; i < activities.size(); i++) {
            String activity = activities.get(i);
            String field = "activities[" + i + "]";
            if (activity == null || activity.isBlank()) {
                errors.add(new ApiError.FieldError(field, "activity name is required"));
            } else if (!seen.add(activity)) {
                errors.add(new ApiError.FieldError(field, "duplicate activity '" + activity + "'"));
            }
        }
        return List.copyOf(errors);
    }
}
