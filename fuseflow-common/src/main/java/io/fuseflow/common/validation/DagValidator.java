package io.fuseflow.common.validation;

import io.fuseflow.common.dto.ApiError;
import io.fuseflow.common.dto.WorkflowRequest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Structural validator for workflow definitions (FR-1). Lives in {@code fuseflow-common} so
 * the definition service, the SDK's runtime {@code WorkflowScanner} and the SDK's
 * compile-time annotation processor all share one implementation (zero drift).
 *
 * <p>Validates task ids/activities and the DAG shape: duplicate tasks, dangling
 * dependencies and cycles are rejected. Activity <em>capability</em> (is a worker
 * registered for this activity?) is deliberately NOT checked here — workers register
 * dynamically, so that check happens at schedule time in the engine (see the design
 * note in docs/implementation-plan.md, Phase 1).
 */
public final class DagValidator {

    /** Returns the field-level errors for a workflow request; empty = valid. */
    public List<ApiError.FieldError> validate(WorkflowRequest request) {
        List<ApiError.FieldError> errors = new ArrayList<>();

        if (request == null) {
            return List.of(new ApiError.FieldError("body", "request body is required"));
        }
        if (request.name() == null || request.name().isBlank()) {
            errors.add(new ApiError.FieldError("name", "name is required"));
        }

        List<WorkflowRequest.Task> tasks = request.tasks();
        if (tasks == null || tasks.isEmpty()) {
            errors.add(new ApiError.FieldError("tasks", "at least one task is required"));
            return List.copyOf(errors);
        }

        // Duplicate task ids + blank id/activity.
        Map<String, Integer> indexByTaskId = new HashMap<>();
        for (int i = 0; i < tasks.size(); i++) {
            WorkflowRequest.Task task = tasks.get(i);
            String field = "tasks[" + i + "]";
            if (task == null) {
                errors.add(new ApiError.FieldError(field, "task must not be null"));
                continue;
            }
            if (task.id() == null || task.id().isBlank()) {
                errors.add(new ApiError.FieldError(field + ".id", "task id is required"));
            } else if (indexByTaskId.putIfAbsent(task.id(), i) != null) {
                errors.add(new ApiError.FieldError(field + ".id", "duplicate task id '" + task.id() + "'"));
            }
            if (task.activity() == null || task.activity().isBlank()) {
                String taskId = task.id() == null ? "" : task.id();
                errors.add(new ApiError.FieldError(field + ".activity", "activity is required for task '" + taskId + "'"));
            }
        }

        // Dependencies: blank entries + references to undefined tasks.
        for (int i = 0; i < tasks.size(); i++) {
            WorkflowRequest.Task task = tasks.get(i);
            if (task == null || task.dependsOn() == null) {
                continue;
            }
            for (int j = 0; j < task.dependsOn().size(); j++) {
                String dep = task.dependsOn().get(j);
                String field = "tasks[" + i + "].dependsOn[" + j + "]";
                if (dep == null || dep.isBlank()) {
                    errors.add(new ApiError.FieldError(field, "dependency must not be blank"));
                } else if (!indexByTaskId.containsKey(dep)) {
                    errors.add(new ApiError.FieldError(field, "dependency '" + dep + "' is not defined in the workflow"));
                }
            }
        }

        // Cycles (DFS over the graph of valid task ids).
        List<String> cycle = findCycle(tasks, indexByTaskId.keySet());
        if (cycle != null) {
            errors.add(new ApiError.FieldError("tasks",
                    "circular dependency detected: " + String.join(" -> ", cycle)));
        }

        return List.copyOf(errors);
    }

    /**
     * Detects a cycle in the DAG built from the tasks' {@code dependsOn} edges.
     * Returns the cycle path (e.g. {@code [a, b, a]}) or {@code null} if acyclic.
     */
    private List<String> findCycle(List<WorkflowRequest.Task> tasks, Set<String> knownTaskIds) {
        // LinkedHashMap keeps declaration order, so cycle paths are deterministic.
        Map<String, Set<String>> adjacency = new LinkedHashMap<>();
        for (WorkflowRequest.Task task : tasks) {
            if (task == null) {
                continue;
            }
            String id = task.id();
            if (id == null || id.isBlank() || task.dependsOn() == null) {
                continue;
            }
            Set<String> deps = adjacency.computeIfAbsent(id, k -> new LinkedHashSet<>());
            for (String dep : task.dependsOn()) {
                if (dep != null && knownTaskIds.contains(dep)) {
                    deps.add(dep);
                }
            }
        }

        Set<String> visiting = new LinkedHashSet<>();
        Set<String> visited = new HashSet<>();
        for (String start : adjacency.keySet()) {
            if (!visited.contains(start)) {
                List<String> cycle = dfs(start, adjacency, visiting, visited, new ArrayList<>());
                if (cycle != null) {
                    return cycle;
                }
            }
        }
        return null;
    }

    private List<String> dfs(String node, Map<String, Set<String>> adjacency, Set<String> visiting,
                             Set<String> visited, List<String> path) {
        visiting.add(node);
        path.add(node);
        for (String next : adjacency.getOrDefault(node, Set.of())) {
            if (visiting.contains(next)) {
                // Cycle found: path from the first occurrence of next to the end, then close the loop.
                int start = path.indexOf(next);
                List<String> cycle = new ArrayList<>(path.subList(start, path.size()));
                cycle.add(next);
                return cycle;
            }
            if (!visited.contains(next)) {
                List<String> cycle = dfs(next, adjacency, visiting, visited, path);
                if (cycle != null) {
                    return cycle;
                }
            }
        }
        path.remove(path.size() - 1);
        visiting.remove(node);
        visited.add(node);
        return null;
    }
}
