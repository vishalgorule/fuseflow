package io.fuseflow.common.dto;

import java.util.List;

/**
 * Request body for registering or replacing a workflow definition. Shared wire contract
 * (Phase 6): the SDK's {@code @Workflow}/{@code @Step} scanner builds one of these and the
 * definition service validates + persists it — the two sides cannot drift.
 *
 * <p>Phase 7: {@code retryPolicy} (workflow-level default) and {@code Task.retryPolicy}
 * (per-task override) are optional; a per-task policy wins over the workflow policy, which
 * wins over the engine's configured defaults.
 *
 * @param name        unique workflow name
 * @param description optional human-readable description
 * @param retryPolicy optional workflow-level retry policy (null = engine defaults)
 * @param tasks       the DAG tasks; each carries its own {@code dependsOn} list
 */
public record WorkflowRequest(String name, String description, RetryPolicy retryPolicy, List<Task> tasks) {

    /** Convenience constructor for callers without a retry policy. */
    public WorkflowRequest(String name, String description, List<Task> tasks) {
        this(name, description, null, tasks);
    }

    public record Task(String id, String activity, List<String> dependsOn, RetryPolicy retryPolicy) {

        /** Convenience constructor for callers without a per-task retry policy. */
        public Task(String id, String activity, List<String> dependsOn) {
            this(id, activity, dependsOn, null);
        }
    }
}
