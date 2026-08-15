package io.fuseflow.registry.validation;

import io.fuseflow.common.dto.ApiError;
import io.fuseflow.common.dto.WorkerRequest;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class WorkerValidatorTest {

    private final WorkerValidator validator = new WorkerValidator();

    private static WorkerRequest request(UUID id, String host, List<String> activities) {
        return new WorkerRequest(id, host, activities);
    }

    @Test
    void acceptsValidRegistration() {
        assertThat(validator.validate(
                request(UUID.randomUUID(), "worker-1", List.of("resizeImage", "uploadImage"))))
                .isEmpty();
    }

    @Test
    void rejectsMissingId() {
        List<ApiError.FieldError> errors = validator.validate(request(null, "worker-1", List.of("actA")));
        assertThat(errors).anySatisfy(error -> {
            assertThat(error.field()).isEqualTo("id");
            assertThat(error.message()).contains("worker id is required");
        });
    }

    @Test
    void rejectsBlankHost() {
        List<ApiError.FieldError> errors = validator.validate(request(UUID.randomUUID(), "  ", List.of("actA")));
        assertThat(errors).anySatisfy(error -> assertThat(error.field()).isEqualTo("host"));
    }

    @Test
    void rejectsMissingActivities() {
        assertThat(validator.validate(request(UUID.randomUUID(), "h", null)))
                .anySatisfy(error -> assertThat(error.field()).isEqualTo("activities"));
        assertThat(validator.validate(request(UUID.randomUUID(), "h", List.of())))
                .anySatisfy(error -> assertThat(error.field()).isEqualTo("activities"));
    }

    @Test
    void rejectsBlankActivityName() {
        List<ApiError.FieldError> errors = validator.validate(request(UUID.randomUUID(), "h", List.of(" ", "actA")));
        assertThat(errors).anySatisfy(error -> assertThat(error.field()).isEqualTo("activities[0]"));
    }

    @Test
    void rejectsDuplicateActivities() {
        List<ApiError.FieldError> errors = validator.validate(request(UUID.randomUUID(), "h", List.of("actA", "actA")));
        assertThat(errors).anySatisfy(error -> {
            assertThat(error.field()).isEqualTo("activities[1]");
            assertThat(error.message()).contains("duplicate activity 'actA'");
        });
    }

    @Test
    void acceptsPoolIdentity() {
        assertThat(validator.validate(
                new WorkerRequest(UUID.randomUUID(), "h", List.of("actA"), "media", 8)))
                .isEmpty();
        // Pool fields are optional — legacy registrations stay valid.
        assertThat(validator.validate(
                new WorkerRequest(UUID.randomUUID(), "h", List.of("actA"), null, null)))
                .isEmpty();
    }

    @Test
    void rejectsBlankPoolName() {
        List<ApiError.FieldError> errors = validator.validate(
                new WorkerRequest(UUID.randomUUID(), "h", List.of("actA"), "  ", null));
        assertThat(errors).anySatisfy(error -> assertThat(error.field()).isEqualTo("poolName"));
    }

    @Test
    void rejectsConcurrencyBelowOne() {
        List<ApiError.FieldError> errors = validator.validate(
                new WorkerRequest(UUID.randomUUID(), "h", List.of("actA"), "media", 0));
        assertThat(errors).anySatisfy(error -> assertThat(error.field()).isEqualTo("concurrency"));
    }

    @Test
    void rejectsNullRequest() {
        assertThat(validator.validate(null)).anySatisfy(error -> {
            assertThat(error.field()).isEqualTo("body");
            assertThat(error.message()).contains("request body is required");
        });
    }
}
