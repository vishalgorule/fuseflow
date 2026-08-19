package io.fuseflow.common.dto;

import java.util.List;

/**
 * Request body for registering a workflow definition. Shared wire contract (Phase 6): the
 * SDK's {@code @Workflow}/{@code @Step} scanner builds one of these and the definition
 * service validates + persists it — the two sides cannot drift.
 *
 * <p>Phase 7: {@code retryPolicy} (workflow-level default) and {@code Task.retryPolicy}
 * (per-task override) are optional; a per-task policy wins over the workflow policy, which
 * wins over the engine's configured defaults.
 *
 * <p>Phase 8: {@code semanticVersion} identifies a version of the workflow. Versions are
 * immutable snapshots — a {@code (name, semanticVersion)} pair can be registered only once;
 * changing the DAG means registering a new version. {@code null}/{@code blank} is normalized
 * to {@code "1"} by the definition service (and the SDK defaults {@code @Workflow.version()}
 * the same way), so pre-Phase 8 callers keep working unchanged.
 *
 * @param name            unique workflow name
 * @param semanticVersion version label (default "1"); {@code (name, version)} is the unique key
 * @param description     optional human-readable description
 * @param retryPolicy     optional workflow-level retry policy (null = engine defaults)
 * @param tasks           the DAG tasks; each carries its own {@code dependsOn} list
 */
public record WorkflowRequest(String name, String semanticVersion, String description,
                              RetryPolicy retryPolicy, List<Task> tasks) {

    /** Convenience constructor without an explicit version (defaults to "1"). */
    public WorkflowRequest(String name, String description, RetryPolicy retryPolicy, List<Task> tasks) {
        this(name, null, description, retryPolicy, tasks);
    }

    /** Convenience constructor for callers without a retry policy or explicit version. */
    public WorkflowRequest(String name, String description, List<Task> tasks) {
        this(name, null, description, null, tasks);
    }

    public record Task(String id, String activity, List<String> dependsOn, RetryPolicy retryPolicy) {

        /** Convenience constructor for callers without a per-task retry policy. */
        public Task(String id, String activity, List<String> dependsOn) {
            this(id, activity, dependsOn, null);
        }
    }
}
