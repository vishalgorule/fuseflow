package io.fuseflow.sdk.runtime;

import io.fuseflow.common.dto.ApiError;
import io.fuseflow.common.dto.RetryPolicy;
import io.fuseflow.common.dto.WorkflowRequest;
import io.fuseflow.common.validation.DagValidator;
import io.fuseflow.sdk.annotation.Activity;
import io.fuseflow.sdk.annotation.Retry;
import io.fuseflow.sdk.annotation.Step;
import io.fuseflow.sdk.annotation.Workflow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.ApplicationContext;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Scans Spring beans for {@code @Workflow} classes and registers their definitions in the
 * {@link WorkflowRegistry} (Phase 6). Runs via {@link SmartInitializingSingleton} — the same
 * timing as the {@link ActivityScanner}, after all singleton beans exist and before the
 * {@link WorkflowRegistrar} starts.
 *
 * <p>Two declaration styles are supported and detected automatically:
 * <ul>
 *   <li><b>Self-contained (default, Phase 6):</b> the {@code @Workflow} class's own
 *       {@code @Activity} methods are the steps — {@code id()} is the task id (defaults to
 *       the activity name) and {@code dependsOn()} the edges. The same methods are also
 *       registered as executable activities by the {@link ActivityScanner}, so one class
 *       defines and implements the whole workflow.</li>
 *   <li><b>Metadata-only {@code @Step}:</b> steps are pure declarations referencing
 *       activities by name; the activities live in other beans (or another deployable).</li>
 * </ul>
 *
 * <p>Each {@code @Workflow} class is validated locally with the shared {@link DagValidator}
 * (the exact rules the definition service applies): duplicate steps, dangling dependencies
 * and cycles fail fast at boot with field-level messages. In the {@code @Step} style, step
 * activities referencing no locally-declared {@code @Activity} are a <b>warning</b> (they may
 * be provided by another deployable — the engine checks capability at schedule time); the
 * compile-time annotation processor turns same-project unknown references into hard errors at
 * build time.
 */
public class WorkflowScanner implements SmartInitializingSingleton {

    private static final Logger log = LoggerFactory.getLogger(WorkflowScanner.class);

    private final ApplicationContext applicationContext;
    private final WorkflowRegistry registry;
    private final DagValidator dagValidator;
    private final ObjectProvider<ActivityRegistry> activityRegistry;

    public WorkflowScanner(ApplicationContext applicationContext, WorkflowRegistry registry,
                           DagValidator dagValidator, ObjectProvider<ActivityRegistry> activityRegistry) {
        this.applicationContext = applicationContext;
        this.registry = registry;
        this.dagValidator = dagValidator;
        this.activityRegistry = activityRegistry;
    }

    @Override
    public void afterSingletonsInstantiated() {
        List<String> declaredActivities = declaredActivities();
        for (String beanName : applicationContext.getBeanNamesForType(Object.class, false, false)) {
            Object bean = applicationContext.getBean(beanName);
            Class<?> targetClass = AopUtils.getTargetClass(bean);
            Workflow workflow = targetClass.getAnnotation(Workflow.class);
            if (workflow != null) {
                registerWorkflow(targetClass, workflow, declaredActivities);
            }
        }
        if (!registry.all().isEmpty()) {
            log.info("Scanned {} @Workflow definition(s): {}",
                    registry.all().size(),
                    registry.all().stream().map(WorkflowRegistration::name).toList());
        }
    }

    // ---------------------------------------------------------------- internals

    private void registerWorkflow(Class<?> workflowClass, Workflow workflow, List<String> declaredActivities) {
        Step[] steps = workflowClass.getAnnotationsByType(Step.class);
        List<WorkflowRequest.Task> tasks = steps.length > 0
                ? tasksFromSteps(steps)
                : tasksFromActivityMethods(workflowClass);

        String description = workflow.description() == null || workflow.description().isBlank()
                ? null : workflow.description();
        // Phase 8: the annotation's version label travels in the wire request; the definition
        // service treats (name, version) as the unique key and defaults blanks to "1".
        WorkflowRequest request = new WorkflowRequest(workflow.name(), workflow.version(), description,
                toPolicy(workflow.retry()), tasks);

        List<ApiError.FieldError> errors = dagValidator.validate(request);
        if (!errors.isEmpty()) {
            throw new IllegalStateException("Invalid @Workflow '" + workflow.name() + "' on "
                    + workflowClass.getName() + ":\n  "
                    + String.join("\n  ", errors.stream()
                            .map(e -> e.field() + ": " + e.message()).toList()));
        }

        // Self-contained @Activity-style workflows declare their activities in place, so the
        // undeclared-activity check only applies to the metadata-only @Step style.
        if (steps.length > 0) {
            warnUndeclaredActivities(workflowClass, request, declaredActivities);
        }

        registry.register(new WorkflowRegistration(request));
    }

    /** Tasks from the metadata-only {@code @Step}/{@code @Steps} declarations (declaration order). */
    private List<WorkflowRequest.Task> tasksFromSteps(Step[] steps) {
        List<WorkflowRequest.Task> tasks = new ArrayList<>();
        for (Step step : steps) {
            List<String> dependsOn = step.dependsOn() == null || step.dependsOn().length == 0
                    ? null : List.of(step.dependsOn());
            tasks.add(new WorkflowRequest.Task(step.id(), step.activity(), dependsOn, toPolicy(step.retry())));
        }
        return tasks;
    }

    /**
     * Tasks from the self-contained style: each {@code @Activity} method of the workflow class
     * is a step. Activity name = {@code value()} (blank → method name); task id = {@code id()}
     * (blank → activity name). Sorted by task id for a deterministic DAG.
     */
    private List<WorkflowRequest.Task> tasksFromActivityMethods(Class<?> workflowClass) {
        List<WorkflowRequest.Task> tasks = new ArrayList<>();
        for (Method method : workflowClass.getMethods()) {
            Activity annotation = method.getAnnotation(Activity.class);
            if (annotation == null) {
                continue;
            }
            String activityName = annotation.value() == null || annotation.value().isBlank()
                    ? method.getName() : annotation.value();
            String taskId = annotation.id() == null || annotation.id().isBlank()
                    ? activityName : annotation.id();
            List<String> dependsOn = annotation.dependsOn() == null || annotation.dependsOn().length == 0
                    ? null : List.of(annotation.dependsOn());
            tasks.add(new WorkflowRequest.Task(taskId, activityName, dependsOn, toPolicy(annotation.retry())));
        }
        tasks.sort(Comparator.comparing(WorkflowRequest.Task::id));
        return tasks;
    }

    /** Maps a {@code @Retry} annotation to the shared wire record; null when nothing is set. */
    private static RetryPolicy toPolicy(Retry retry) {
        if (retry == null) {
            return null;
        }
        RetryPolicy policy = new RetryPolicy(
                retry.maxAttempts() > 0 ? retry.maxAttempts() : null,
                retry.fixedDelaySeconds() > 0 ? retry.fixedDelaySeconds() : null,
                retry.exponentialBackoff() ? Boolean.TRUE : null,
                retry.backoffMultiplier() > 0 ? retry.backoffMultiplier() : null,
                retry.nonRetryableExceptions().length > 0 ? List.of(retry.nonRetryableExceptions()) : null);
        return policy.isEmpty() ? null : policy;
    }

    /** Warns (does not fail) when a step references an activity not declared in this project. */
    private void warnUndeclaredActivities(Class<?> workflowClass, WorkflowRequest request,
                                          List<String> declaredActivities) {
        List<String> undeclared = request.tasks().stream()
                .map(WorkflowRequest.Task::activity)
                .filter(activity -> !declaredActivities.contains(activity))
                .distinct()
                .toList();
        if (!undeclared.isEmpty()) {
            log.warn("@Workflow '{}' ({}) references activities not declared in this project: {} — "
                            + "if they are provided by another deployable this is fine (capability is "
                            + "checked at schedule time), otherwise the workflow will fail when run.",
                    request.name(), workflowClass.getName(), undeclared);
        }
    }

    /** Activities declared via {@code @Activity} beans in this application (empty when none). */
    private List<String> declaredActivities() {
        ActivityRegistry registry = activityRegistry.getIfAvailable();
        return registry == null ? List.of() : registry.names();
    }
}
