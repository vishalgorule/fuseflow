package io.fuseflow.sdk.processor;

import io.fuseflow.common.dto.ApiError;
import io.fuseflow.common.dto.RetryPolicy;
import io.fuseflow.common.dto.WorkflowRequest;
import io.fuseflow.common.validation.DagValidator;
import io.fuseflow.sdk.annotation.Activity;
import io.fuseflow.sdk.annotation.Retry;
import io.fuseflow.sdk.annotation.Step;
import io.fuseflow.sdk.annotation.Workflow;

import io.fuseflow.sdk.core.ActivityContext;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Compile-time validation of {@link @Workflow} definitions (Phase 6). Registered via
 * {@code META-INF/services/javax.annotation.processing.Processor} in the SDK, so any module
 * depending on the SDK gets it automatically.
 *
 * <p>Runs the exact same {@link DagValidator} rules the definition service applies at REST
 * time (duplicate steps, dangling dependencies, cycles, blank fields — zero drift), turning
 * them into <b>compile errors</b> with field-level messages. It additionally hard-errors when
 * a {@code @Step} references an activity not declared via {@code @Activity} in the same
 * compilation — the workflow definition and its activities live in one project, so a typo'd
 * activity name fails the build instead of the run. (Cross-project workflows — a definition
 * referencing activities another deployable implements — cannot be expressed with this
 * compile-time check; use the JSON API for those. The runtime {@code WorkflowScanner}
 * downgrades the same check to a warning as a backstop for {@code -proc:none} builds.)
 */
public class FuseFlowWorkflowProcessor extends AbstractProcessor {

    private static final String WORKFLOW_ANNOTATION = Workflow.class.getCanonicalName();
    private static final String ACTIVITY_ANNOTATION = Activity.class.getCanonicalName();

    private final DagValidator dagValidator = new DagValidator();

    @Override
    public Set<String> getSupportedAnnotationTypes() {
        return Set.of(WORKFLOW_ANNOTATION, ACTIVITY_ANNOTATION);
    }

    @Override
    public SourceVersion getSupportedSourceVersion() {
        return SourceVersion.latestSupported();
    }

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        if (roundEnv.processingOver()) {
            return false;
        }
        Set<String> declaredActivities = declaredActivities(roundEnv);
        for (Element element : roundEnv.getElementsAnnotatedWith(Workflow.class)) {
            validateWorkflow((TypeElement) element, declaredActivities);
        }
        return false;
    }

    // ---------------------------------------------------------------- internals

    private Set<String> declaredActivities(RoundEnvironment roundEnv) {
        Set<String> names = new HashSet<>();
        for (Element element : roundEnv.getElementsAnnotatedWith(Activity.class)) {
            Activity annotation = element.getAnnotation(Activity.class);
            // Phase 6: blank value = activity name is the method name (Temporal-style).
            String name = annotation == null ? "" : annotation.value();
            if (name == null || name.isBlank()) {
                name = element.getSimpleName().toString();
            }
            if (!names.add(name)) {
                error(element, "Duplicate @Activity name '" + name + "'");
            }
        }
        return names;
    }

    private void validateWorkflow(TypeElement workflowClass, Set<String> declaredActivities) {
        Workflow workflow = workflowClass.getAnnotation(Workflow.class);
        if (workflow == null) {
            return;
        }
        String name = workflow.name();
        Step[] steps = workflowClass.getAnnotationsByType(Step.class);
        List<WorkflowRequest.Task> tasks = steps.length > 0
                ? tasksFromSteps(steps)
                : tasksFromActivityMethods(workflowClass);
        WorkflowRequest request = new WorkflowRequest(name, workflow.version(),
                workflow.description() == null || workflow.description().isBlank() ? null : workflow.description(),
                toPolicy(workflow.retry()), tasks);

        // Structural rules — identical to the definition service (zero drift).
        for (ApiError.FieldError error : dagValidator.validate(request)) {
            error(workflowClass, "Invalid @Workflow '" + name + "': " + error.field() + ": " + error.message());
        }

        // Activity references must be declared in the same compilation. The self-contained
        // style declares its activities in place, so only the metadata-only @Step style needs
        // the cross-check.
        if (steps.length == 0) {
            return;
        }
        Set<String> declared = new HashSet<>(declaredActivities);
        for (Step step : steps) {
            if (step.activity() != null && !step.activity().isBlank() && !declared.contains(step.activity())) {
                error(workflowClass, "Invalid @Workflow '" + name + "': step '" + step.id()
                        + "' references activity '" + step.activity()
                        + "' which is not declared via @Activity in this compilation");
            }
        }
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
     * is a step. Validates the method signature at compile time (public, single
     * {@link ActivityContext} parameter). Sorted by task id for a deterministic DAG.
     */
    private List<WorkflowRequest.Task> tasksFromActivityMethods(TypeElement workflowClass) {
        List<WorkflowRequest.Task> tasks = new ArrayList<>();
        for (Element enclosed : workflowClass.getEnclosedElements()) {
            if (enclosed.getKind() != ElementKind.METHOD) {
                continue;
            }
            ExecutableElement method = (ExecutableElement) enclosed;
            Activity annotation = method.getAnnotation(Activity.class);
            if (annotation == null) {
                continue;
            }
            validateActivityMethod(method);

            String activityName = annotation.value() == null || annotation.value().isBlank()
                    ? method.getSimpleName().toString() : annotation.value();
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

    private void validateActivityMethod(ExecutableElement method) {
        if (!method.getModifiers().contains(Modifier.PUBLIC)) {
            error(method, "@Activity method must be public");
        }
        if (method.getParameters().size() != 1
                || !ActivityContext.class.getName()
                        .equals(method.getParameters().get(0).asType().toString())) {
            error(method, "@Activity method must take a single "
                    + ActivityContext.class.getSimpleName() + " parameter");
        }
    }

    private void error(Element element, String message) {
        processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, message, element);
    }
}
