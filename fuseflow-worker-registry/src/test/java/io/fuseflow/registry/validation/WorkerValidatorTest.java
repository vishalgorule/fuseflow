package io.fuseflow.registry.validation;

import io.fuseflow.common.dto.ApiError;
import io.fuseflow.common.dto.HeartbeatRequest;
import io.fuseflow.common.dto.WorkerRequest;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class WorkerValidatorTest {

    private final WorkerValidator validator = new WorkerValidator();

    private static WorkerRequest request(UUID id, String host, Integer capacity, List<String> activities) {
        return new WorkerRequest(id, host, capacity, activities);
    }

    @Test
    void acceptsValidRegistration() {
        assertThat(validator.validate(
                request(UUID.randomUUID(), "worker-1", 4, List.of("resizeImage", "uploadImage"))))
                .isEmpty();
    }

    @Test
    void acceptsNullCapacity() {
        assertThat(validator.validate(request(UUID.randomUUID(), "worker-1", null, List.of("actA"))))
                .isEmpty();
    }

    @Test
    void rejectsMissingId() {
        List<ApiError.FieldError> errors = validator.validate(request(null, "worker-1", 1, List.of("actA")));
        assertThat(errors).anySatisfy(error -> {
            assertThat(error.field()).isEqualTo("id");
            assertThat(error.message()).contains("worker id is required");
        });
    }

    @Test
    void rejectsBlankHost() {
        List<ApiError.FieldError> errors = validator.validate(request(UUID.randomUUID(), "  ", 1, List.of("actA")));
        assertThat(errors).anySatisfy(error -> assertThat(error.field()).isEqualTo("host"));
    }

    @Test
    void rejectsMissingActivities() {
        assertThat(validator.validate(request(UUID.randomUUID(), "h", 1, null)))
                .anySatisfy(error -> assertThat(error.field()).isEqualTo("activities"));
        assertThat(validator.validate(request(UUID.randomUUID(), "h", 1, List.of())))
                .anySatisfy(error -> assertThat(error.field()).isEqualTo("activities"));
    }

    @Test
    void rejectsBlankActivityName() {
        List<ApiError.FieldError> errors = validator.validate(request(UUID.randomUUID(), "h", 1, List.of(" ", "actA")));
        assertThat(errors).anySatisfy(error -> assertThat(error.field()).isEqualTo("activities[0]"));
    }

    @Test
    void rejectsDuplicateActivities() {
        List<ApiError.FieldError> errors = validator.validate(request(UUID.randomUUID(), "h", 1, List.of("actA", "actA")));
        assertThat(errors).anySatisfy(error -> {
            assertThat(error.field()).isEqualTo("activities[1]");
            assertThat(error.message()).contains("duplicate activity 'actA'");
        });
    }

    @Test
    void rejectsCapacityBelowOne() {
        List<ApiError.FieldError> errors = validator.validate(request(UUID.randomUUID(), "h", 0, List.of("actA")));
        assertThat(errors).anySatisfy(error -> {
            assertThat(error.field()).isEqualTo("capacity");
            assertThat(error.message()).contains("capacity must be at least 1");
        });
    }

    @Test
    void rejectsNullRequest() {
        assertThat(validator.validate(null)).anySatisfy(error -> {
            assertThat(error.field()).isEqualTo("body");
            assertThat(error.message()).contains("request body is required");
        });
    }

    @Test
    void validatesHeartbeatCapacity() {
        assertThat(validator.validateHeartbeat(new HeartbeatRequest(-1)))
                .anySatisfy(error -> assertThat(error.field()).isEqualTo("capacity"));
        assertThat(validator.validateHeartbeat(new HeartbeatRequest(2))).isEmpty();
        assertThat(validator.validateHeartbeat(new HeartbeatRequest(null))).isEmpty();
        assertThat(validator.validateHeartbeat(null)).isEmpty();
    }
}
