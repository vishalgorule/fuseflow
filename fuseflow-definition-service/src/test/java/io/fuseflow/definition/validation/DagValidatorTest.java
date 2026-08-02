package io.fuseflow.definition.validation;

import io.fuseflow.common.dto.ApiError;
import io.fuseflow.definition.dto.WorkflowRequest;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class DagValidatorTest {

    private final DagValidator validator = new DagValidator();

    private static WorkflowRequest.Task task(String id, String activity, String... dependsOn) {
        return new WorkflowRequest.Task(id, activity, dependsOn.length == 0 ? null : List.of(dependsOn));
    }

    private static WorkflowRequest request(WorkflowRequest.Task... tasks) {
        return new WorkflowRequest("workflow", "desc", tasks.length == 0 ? null : List.of(tasks));
    }

    @Test
    void acceptsLinearDag() {
        assertThat(validator.validate(request(task("a", "actA"), task("b", "actB", "a")))).isEmpty();
    }

    @Test
    void acceptsDiamondDag() {
        WorkflowRequest.Task a = task("a", "actA");
        WorkflowRequest.Task b = task("b", "actB", "a");
        WorkflowRequest.Task c = task("c", "actC", "b");
        WorkflowRequest.Task d = task("d", "actD", "b");
        WorkflowRequest.Task e = task("e", "actE", "c", "d");
        assertThat(validator.validate(request(a, b, c, d, e))).isEmpty();
    }

    @Test
    void acceptsTasksWithoutDependencies() {
        assertThat(validator.validate(request(task("a", "actA"), task("b", "actB")))).isEmpty();
    }

    @Test
    void acceptsTaskWithDependencyDeclaredLater() {
        assertThat(validator.validate(request(task("b", "actB", "a"), task("a", "actA")))).isEmpty();
    }

    @Test
    void rejectsDuplicateTaskIds() {
        List<ApiError.FieldError> errors = validator.validate(request(task("a", "actA"), task("a", "actB")));
        assertThat(errors).anySatisfy(error -> {
            assertThat(error.field()).isEqualTo("tasks[1].id");
            assertThat(error.message()).contains("duplicate task id 'a'");
        });
    }

    @Test
    void rejectsSelfDependency() {
        List<ApiError.FieldError> errors = validator.validate(request(task("a", "actA", "a")));
        assertThat(errors).anySatisfy(error -> {
            assertThat(error.field()).isEqualTo("tasks");
            assertThat(error.message()).contains("circular dependency detected: a -> a");
        });
    }

    @Test
    void rejectsDirectCycle() {
        WorkflowRequest.Task a = task("a", "actA", "b");
        WorkflowRequest.Task b = task("b", "actB", "a");
        List<ApiError.FieldError> errors = validator.validate(request(a, b));
        assertThat(errors).anySatisfy(error -> {
            assertThat(error.field()).isEqualTo("tasks");
            assertThat(error.message()).contains("circular dependency");
        });
    }

    @Test
    void rejectsIndirectCycle() {
        WorkflowRequest.Task a = task("a", "actA", "c");
        WorkflowRequest.Task b = task("b", "actB", "a");
        WorkflowRequest.Task c = task("c", "actC", "b");
        List<ApiError.FieldError> errors = validator.validate(request(a, b, c));
        assertThat(errors).anySatisfy(error -> {
            assertThat(error.field()).isEqualTo("tasks");
            assertThat(error.message()).contains("circular dependency");
        });
    }

    @Test
    void rejectsMissingDependency() {
        List<ApiError.FieldError> errors = validator.validate(request(task("a", "actA"), task("b", "actB", "ghost")));
        assertThat(errors).anySatisfy(error -> {
            assertThat(error.field()).isEqualTo("tasks[1].dependsOn[0]");
            assertThat(error.message()).contains("dependency 'ghost' is not defined");
        });
    }

    @Test
    void rejectsBlankTaskId() {
        List<ApiError.FieldError> errors = validator.validate(request(task("", "actA")));
        assertThat(errors).anySatisfy(error -> {
            assertThat(error.field()).isEqualTo("tasks[0].id");
            assertThat(error.message()).contains("task id is required");
        });
    }

    @Test
    void rejectsBlankActivity() {
        List<ApiError.FieldError> errors = validator.validate(request(task("a", "")));
        assertThat(errors).anySatisfy(error -> {
            assertThat(error.field()).isEqualTo("tasks[0].activity");
            assertThat(error.message()).contains("activity is required for task 'a'");
        });
    }

    @Test
    void rejectsBlankDependency() {
        List<ApiError.FieldError> errors = validator.validate(request(task("a", "actA"), task("b", "actB", "")));
        assertThat(errors).anySatisfy(error -> {
            assertThat(error.field()).isEqualTo("tasks[1].dependsOn[0]");
            assertThat(error.message()).contains("dependency must not be blank");
        });
    }

    @Test
    void rejectsBlankName() {
        List<ApiError.FieldError> errors = validator.validate(new WorkflowRequest("  ", null, List.of(task("a", "actA"))));
        assertThat(errors).anySatisfy(error -> {
            assertThat(error.field()).isEqualTo("name");
            assertThat(error.message()).contains("name is required");
        });
    }

    @Test
    void rejectsEmptyTaskList() {
        List<ApiError.FieldError> errors = validator.validate(new WorkflowRequest("w", null, List.of()));
        assertThat(errors).anySatisfy(error -> assertThat(error.field()).isEqualTo("tasks"));
    }

    @Test
    void rejectsNullRequest() {
        List<ApiError.FieldError> errors = validator.validate(null);
        assertThat(errors).anySatisfy(error -> {
            assertThat(error.field()).isEqualTo("body");
            assertThat(error.message()).contains("request body is required");
        });
    }

    @Test
    void rejectsNullTaskElement() {
        List<ApiError.FieldError> errors = validator.validate(
                new WorkflowRequest("w", null, java.util.Arrays.asList(task("a", "actA"), null)));
        assertThat(errors).anySatisfy(error -> {
            assertThat(error.field()).isEqualTo("tasks[1]");
            assertThat(error.message()).contains("task must not be null");
        });
    }

    @Test
    void reportsAllErrorsAtOnce() {
        List<ApiError.FieldError> errors = validator.validate(request(task("a", ""), task("a", "actB", "ghost")));
        assertThat(errors)
                .extracting(ApiError.FieldError::field)
                .containsExactlyInAnyOrder("tasks[0].activity", "tasks[1].id", "tasks[1].dependsOn[0]");
    }
}
