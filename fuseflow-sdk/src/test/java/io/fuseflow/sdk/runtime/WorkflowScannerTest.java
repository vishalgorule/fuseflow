package io.fuseflow.sdk.runtime;

import io.fuseflow.common.dto.WorkflowRequest;
import io.fuseflow.common.validation.DagValidator;
import io.fuseflow.sdk.annotation.Activity;
import io.fuseflow.sdk.annotation.Retry;
import io.fuseflow.sdk.annotation.Step;
import io.fuseflow.sdk.annotation.Steps;
import io.fuseflow.sdk.annotation.Workflow;
import io.fuseflow.sdk.core.ActivityContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkflowScannerTest {

    @Test
    void scansWorkflowBeansAndBuildsTheDag() {
        try (AnnotationConfigApplicationContext context =
                     new AnnotationConfigApplicationContext(ScanConfig.class)) {
            WorkflowRegistry registry = context.getBean(WorkflowRegistry.class);
            assertThat(registry.all()).hasSize(1);

            WorkflowRegistration registration = registry.all().get(0);
            assertThat(registration.name()).isEqualTo("diamond");
            assertThat(registration.request().description()).isEqualTo("desc");

            List<WorkflowRequest.Task> tasks = registration.request().tasks();
            assertThat(tasks).extracting(WorkflowRequest.Task::id)
                    .containsExactly("a", "b", "c", "d", "e");
            assertThat(tasks).extracting(WorkflowRequest.Task::activity)
                    .containsExactly("actA", "actB", "actC", "actD", "actE");
            // Parallel branches + fan-in join fall out of the dependency graph.
            assertThat(tasks.get(2).dependsOn()).containsExactly("b");
            assertThat(tasks.get(3).dependsOn()).containsExactly("b");
            assertThat(tasks.get(4).dependsOn()).containsExactlyInAnyOrder("c", "d");
        }
    }

    @Test
    void scansContainerFormStepsAnnotation() {
        // The explicit @Steps({...}) container (used by the order-fulfillment sample) must be
        // unwrapped exactly like the repeatable form.
        try (AnnotationConfigApplicationContext context =
                     new AnnotationConfigApplicationContext(ContainerConfig.class)) {
            WorkflowRegistry registry = context.getBean(WorkflowRegistry.class);
            assertThat(registry.all()).hasSize(1);

            WorkflowRegistration registration = registry.all().get(0);
            assertThat(registration.name()).isEqualTo("container");

            List<WorkflowRequest.Task> tasks = registration.request().tasks();
            assertThat(tasks).extracting(WorkflowRequest.Task::id)
                    .containsExactly("validate", "charge", "pack", "ship", "notify");
            assertThat(tasks).extracting(WorkflowRequest.Task::activity)
                    .containsExactly("validateOrder", "chargePayment", "packItems", "shipOrder", "notifyCustomer");
            assertThat(tasks.get(3).dependsOn()).containsExactlyInAnyOrder("charge", "pack");
            assertThat(tasks.get(4).dependsOn()).containsExactly("ship");
        }
    }

    @Test
    void scansSelfContainedActivityMethodWorkflow() {
        // Phase 6 minimal style: the @Workflow class's own @Activity methods are the steps;
        // activity name defaults to the method name, id defaults to the activity name.
        try (AnnotationConfigApplicationContext context =
                     new AnnotationConfigApplicationContext(SelfContainedConfig.class)) {
            WorkflowRegistry registry = context.getBean(WorkflowRegistry.class);
            assertThat(registry.all()).hasSize(1);

            WorkflowRegistration registration = registry.all().get(0);
            assertThat(registration.name()).isEqualTo("order-fulfillment");
            assertThat(registration.request().description()).isEqualTo("desc");

            List<WorkflowRequest.Task> tasks = registration.request().tasks();
            // Sorted by task id.
            assertThat(tasks).extracting(WorkflowRequest.Task::id)
                    .containsExactly("charge", "notify", "pack", "ship", "validate");
            assertThat(tasks).extracting(WorkflowRequest.Task::activity)
                    .containsExactly("chargePayment", "notifyCustomer", "packItems", "shipOrder", "validateOrder");
            assertThat(tasks.get(0).dependsOn()).containsExactly("validate");
            assertThat(tasks.get(3).dependsOn()).containsExactlyInAnyOrder("charge", "pack");
            assertThat(tasks.get(4).dependsOn()).isNull();
        }
    }

    @Test
    void mapsRetryPoliciesFromAnnotations() {
        // Phase 7: @Retry on the workflow (default) and on steps/activities (per-task override)
        // maps onto the shared wire record; unset knobs stay null so the next level can decide.
        try (AnnotationConfigApplicationContext context =
                     new AnnotationConfigApplicationContext(RetryPolicyConfig.class)) {
            WorkflowRegistration registration = context.getBean(WorkflowRegistry.class).all().get(0);
            WorkflowRequest request = registration.request();

            assertThat(request.retryPolicy()).isNotNull();
            assertThat(request.retryPolicy().maxAttempts()).isEqualTo(4);
            assertThat(request.retryPolicy().fixedDelaySeconds()).isEqualTo(3);
            assertThat(request.retryPolicy().exponentialBackoff()).isTrue();
            assertThat(request.retryPolicy().backoffMultiplier()).isEqualTo(2.0);
            assertThat(request.retryPolicy().nonRetryableExceptions())
                    .containsExactly("java.lang.IllegalArgumentException");

            // Per-task override on a @Step.
            assertThat(request.tasks()).extracting(WorkflowRequest.Task::id)
                    .containsExactly("a", "b");
            assertThat(request.tasks().get(0).retryPolicy()).isNotNull();
            assertThat(request.tasks().get(0).retryPolicy().maxAttempts()).isEqualTo(1);
            assertThat(request.tasks().get(0).retryPolicy().fixedDelaySeconds()).isNull();
            // Task without a policy falls through to the workflow policy.
            assertThat(request.tasks().get(1).retryPolicy()).isNull();
        }
    }

    @Test
    void mapsRetryPolicyOnSelfContainedActivityMethods() {
        try (AnnotationConfigApplicationContext context =
                     new AnnotationConfigApplicationContext(SelfContainedRetryConfig.class)) {
            WorkflowRequest request = context.getBean(WorkflowRegistry.class).all().get(0).request();
            // Workflow-level policy is present; per-method @Retry overrides it.
            assertThat(request.retryPolicy().maxAttempts()).isEqualTo(5);
            assertThat(request.tasks()).extracting(WorkflowRequest.Task::id)
                    .containsExactly("charge", "validate");
            assertThat(request.tasks().get(0).retryPolicy().maxAttempts()).isEqualTo(1);
            assertThat(request.tasks().get(1).retryPolicy()).isNull();
        }
    }

    @Test
    void rejectsCyclesAtScanTime() {
        assertThatThrownBy(() -> new AnnotationConfigApplicationContext(CyclicConfig.class))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("circular dependency detected");
    }

    @Test
    void rejectsDanglingDependenciesAtScanTime() {
        assertThatThrownBy(() -> new AnnotationConfigApplicationContext(DanglingConfig.class))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("dependency 'ghost' is not defined");
    }

    @Test
    void rejectsDuplicateStepIdsAtScanTime() {
        assertThatThrownBy(() -> new AnnotationConfigApplicationContext(DuplicateStepConfig.class))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("duplicate task id 'a'");
    }

    @Test
    void rejectsDuplicateWorkflowNamesWithinOneProject() {
        assertThatThrownBy(() -> new AnnotationConfigApplicationContext(DuplicateNameConfig.class))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Duplicate @Workflow registration: 'dup'");
    }

    @Test
    void registersWorkflowReferencingUndeclaredActivity() {
        // Cross-project workflows are allowed at runtime — capability is checked at schedule
        // time; the compile-time processor hard-errors on these in the same project.
        try (AnnotationConfigApplicationContext context =
                     new AnnotationConfigApplicationContext(UndeclaredActivityConfig.class)) {
            WorkflowRegistry registry = context.getBean(WorkflowRegistry.class);
            assertThat(registry.all()).hasSize(1);
            assertThat(registry.all().get(0).request().tasks().get(0).activity()).isEqualTo("externalActivity");
        }
    }

    // ------------------------------------------------------------ fixtures

    @Workflow(name = "diamond", description = "desc")
    @Step(id = "a", activity = "actA")
    @Step(id = "b", activity = "actB", dependsOn = "a")
    @Step(id = "c", activity = "actC", dependsOn = "b")
    @Step(id = "d", activity = "actD", dependsOn = "b")
    @Step(id = "e", activity = "actE", dependsOn = {"c", "d"})
    static class DiamondWorkflow {
    }

    @Workflow(name = "order-fulfillment", description = "desc")
    static class SelfContainedWorkflow {
        @Activity(id = "validate")
        public Map<String, Object> validateOrder(ActivityContext ctx) {
            return Map.of();
        }

        @Activity(id = "charge", dependsOn = "validate")
        public Map<String, Object> chargePayment(ActivityContext ctx) {
            return Map.of();
        }

        @Activity(id = "pack", dependsOn = "validate")
        public Map<String, Object> packItems(ActivityContext ctx) {
            return Map.of();
        }

        @Activity(id = "ship", dependsOn = {"charge", "pack"})
        public Map<String, Object> shipOrder(ActivityContext ctx) {
            return Map.of();
        }

        @Activity(id = "notify", dependsOn = "ship")
        public Map<String, Object> notifyCustomer(ActivityContext ctx) {
            return Map.of();
        }
    }

    @Workflow(name = "container")
    @Steps({
            @Step(id = "validate", activity = "validateOrder"),
            @Step(id = "charge", activity = "chargePayment", dependsOn = "validate"),
            @Step(id = "pack", activity = "packItems", dependsOn = "validate"),
            @Step(id = "ship", activity = "shipOrder", dependsOn = {"charge", "pack"}),
            @Step(id = "notify", activity = "notifyCustomer", dependsOn = "ship")
    })
    static class ContainerWorkflow {
    }

    @Workflow(name = "retry-policy", description = "desc",
            retry = @Retry(maxAttempts = 4, fixedDelaySeconds = 3, exponentialBackoff = true,
                    backoffMultiplier = 2.0, nonRetryableExceptions = "java.lang.IllegalArgumentException"))
    @Step(id = "a", activity = "actA", retry = @Retry(maxAttempts = 1))
    @Step(id = "b", activity = "actB")
    static class RetryPolicyWorkflow {
    }

    @Workflow(name = "self-contained-retry", retry = @Retry(maxAttempts = 5, fixedDelaySeconds = 2))
    static class SelfContainedRetryWorkflow {
        @Activity(id = "validate")
        public String validate(ActivityContext ctx) {
            return null;
        }

        @Activity(id = "charge", dependsOn = "validate", retry = @Retry(maxAttempts = 1))
        public String charge(ActivityContext ctx) {
            return null;
        }
    }

    @Workflow(name = "cyclic")
    @Step(id = "a", activity = "actA", dependsOn = "b")
    @Step(id = "b", activity = "actB", dependsOn = "a")
    static class CyclicWorkflow {
    }

    @Workflow(name = "dangling")
    @Step(id = "a", activity = "actA", dependsOn = "ghost")
    static class DanglingWorkflow {
    }

    @Workflow(name = "duplicate-steps")
    @Step(id = "a", activity = "actA")
    @Step(id = "a", activity = "actB")
    static class DuplicateStepWorkflow {
    }

    @Workflow(name = "dup")
    @Step(id = "a", activity = "actA")
    static class DupOne {
    }

    @Workflow(name = "dup")
    @Step(id = "b", activity = "actB")
    static class DupTwo {
    }

    @Workflow(name = "external")
    @Step(id = "a", activity = "externalActivity")
    static class ExternalWorkflow {
    }

    @Configuration
    static class BaseConfig {
        @Bean
        WorkflowRegistry workflowRegistry() {
            return new WorkflowRegistry();
        }

        @Bean
        DagValidator dagValidator() {
            return new DagValidator();
        }

        @Bean
        WorkflowScanner workflowScanner(ApplicationContext context, WorkflowRegistry registry,
                                        DagValidator dagValidator, ObjectProvider<ActivityRegistry> activityRegistry) {
            return new WorkflowScanner(context, registry, dagValidator, activityRegistry);
        }
    }

    @Configuration
    static class ScanConfig extends BaseConfig {
        @Bean
        DiamondWorkflow diamondWorkflow() {
            return new DiamondWorkflow();
        }
    }

    @Configuration
    static class SelfContainedConfig extends BaseConfig {
        @Bean
        SelfContainedWorkflow selfContainedWorkflow() {
            return new SelfContainedWorkflow();
        }
    }

    @Configuration
    static class ContainerConfig extends BaseConfig {
        @Bean
        ContainerWorkflow containerWorkflow() {
            return new ContainerWorkflow();
        }
    }

    @Configuration
    static class RetryPolicyConfig extends BaseConfig {
        @Bean
        RetryPolicyWorkflow retryPolicyWorkflow() {
            return new RetryPolicyWorkflow();
        }
    }

    @Configuration
    static class SelfContainedRetryConfig extends BaseConfig {
        @Bean
        SelfContainedRetryWorkflow selfContainedRetryWorkflow() {
            return new SelfContainedRetryWorkflow();
        }
    }

    @Configuration
    static class CyclicConfig extends BaseConfig {
        @Bean
        CyclicWorkflow cyclicWorkflow() {
            return new CyclicWorkflow();
        }
    }

    @Configuration
    static class DanglingConfig extends BaseConfig {
        @Bean
        DanglingWorkflow danglingWorkflow() {
            return new DanglingWorkflow();
        }
    }

    @Configuration
    static class DuplicateStepConfig extends BaseConfig {
        @Bean
        DuplicateStepWorkflow duplicateStepWorkflow() {
            return new DuplicateStepWorkflow();
        }
    }

    @Configuration
    static class DuplicateNameConfig extends BaseConfig {
        @Bean
        DupOne dupOne() {
            return new DupOne();
        }

        @Bean
        DupTwo dupTwo() {
            return new DupTwo();
        }
    }

    @Configuration
    static class UndeclaredActivityConfig extends BaseConfig {
        @Bean
        ExternalWorkflow externalWorkflow() {
            return new ExternalWorkflow();
        }
    }
}
