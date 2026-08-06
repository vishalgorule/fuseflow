package io.fuseflow.engine.model;

import io.fuseflow.engine.definition.WorkflowDefinitionSnapshot;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Materialized dependency graph for one execution: for every task, the number of remaining
 * upstream dependencies and the ordered list of dependent task ids (reverse edges).
 *
 * <p>Pure computation over a definition snapshot (no I/O) so it can be unit-tested directly.
 * Declaration order is preserved for deterministic scheduling. The definition service has
 * already validated the DAG (no cycles, no dangling refs), so unknown dependencies are only
 * defensively ignored.
 */
public final class DagModel {

    private DagModel() {
    }

    /** A task with its dependency bookkeeping. */
    public record DagTask(String taskId, String activityName, int remainingDependencies, List<String> dependents) {
    }

    /** Computes the dependency counts + reverse edges for every task in declaration order. */
    public static List<DagTask> from(WorkflowDefinitionSnapshot snapshot) {
        List<WorkflowDefinitionSnapshot.Task> tasks = snapshot.tasks();
        Map<String, Integer> indexByTaskId = new LinkedHashMap<>();
        for (int i = 0; i < tasks.size(); i++) {
            indexByTaskId.put(tasks.get(i).id(), i);
        }

        // Reverse adjacency: dependents[x] = tasks that list x in their dependsOn.
        Map<String, Set<String>> dependents = new LinkedHashMap<>();
        for (WorkflowDefinitionSnapshot.Task task : tasks) {
            if (task.dependsOn() == null) {
                continue;
            }
            for (String dep : task.dependsOn()) {
                if (indexByTaskId.containsKey(dep)) {
                    dependents.computeIfAbsent(dep, key -> new LinkedHashSet<>()).add(task.id());
                }
            }
        }

        List<DagTask> result = new ArrayList<>(tasks.size());
        for (WorkflowDefinitionSnapshot.Task task : tasks) {
            int remaining = 0;
            if (task.dependsOn() != null) {
                for (String dep : task.dependsOn()) {
                    if (indexByTaskId.containsKey(dep)) {
                        remaining++;
                    }
                }
            }
            result.add(new DagTask(task.id(), task.activity(), remaining, List.copyOf(dependents.getOrDefault(task.id(), Set.of()))));
        }
        return result;
    }
}
