package io.fuseflow.engine.model;

import io.fuseflow.engine.definition.WorkflowDefinitionSnapshot;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class DagModelTest {

    private static final UUID WORKFLOW = UUID.randomUUID();

    private static WorkflowDefinitionSnapshot snapshot(WorkflowDefinitionSnapshot.Task... tasks) {
        return new WorkflowDefinitionSnapshot(WORKFLOW, "wf", 1, List.of(tasks));
    }

    private static WorkflowDefinitionSnapshot.Task task(String id, String activity, String... dependsOn) {
        return new WorkflowDefinitionSnapshot.Task(id, activity, dependsOn.length == 0 ? List.of() : List.of(dependsOn));
    }

    @Test
    void computesRemainingDependenciesAndDependentsForDiamond() {
        List<DagModel.DagTask> dag = DagModel.from(snapshot(
                task("a", "actA"),
                task("b", "actB", "a"),
                task("c", "actC", "b"),
                task("d", "actD", "b"),
                task("e", "actE", "c", "d")));

        assertThat(dag).extracting(DagModel.DagTask::taskId)
                .containsExactly("a", "b", "c", "d", "e");

        // Remaining dependency counts.
        assertThat(dag.get(0).remainingDependencies()).isZero();   // a: root
        assertThat(dag.get(1).remainingDependencies()).isEqualTo(1); // b ← a
        assertThat(dag.get(2).remainingDependencies()).isEqualTo(1); // c ← b
        assertThat(dag.get(3).remainingDependencies()).isEqualTo(1); // d ← b
        assertThat(dag.get(4).remainingDependencies()).isEqualTo(2); // e ← c,d (fan-in join)

        // Reverse edges (dependents) in declaration order.
        assertThat(dag.get(0).dependents()).containsExactly("b");        // a → b
        assertThat(dag.get(1).dependents()).containsExactly("c", "d");   // b → c,d (fan-out)
        assertThat(dag.get(2).dependents()).containsExactly("e");        // c → e
        assertThat(dag.get(3).dependents()).containsExactly("e");        // d → e
        assertThat(dag.get(4).dependents()).isEmpty();                   // e: sink
    }

    @Test
    void handlesParallelRootTasks() {
        List<DagModel.DagTask> dag = DagModel.from(snapshot(
                task("a", "actA"),
                task("b", "actB"),
                task("join", "actJoin", "a", "b")));

        assertThat(dag).extracting(DagModel.DagTask::remainingDependencies)
                .containsExactly(0, 0, 2);
        assertThat(dag.get(2).dependents()).isEmpty();
        assertThat(dag.get(0).dependents()).containsExactly("join");
        assertThat(dag.get(1).dependents()).containsExactly("join");
    }

    @Test
    void ignoresUnknownDependenciesDefensively() {
        // The definition service validates DAGs, but unknown refs must not break counting.
        List<DagModel.DagTask> dag = DagModel.from(snapshot(task("a", "actA", "ghost")));
        assertThat(dag.get(0).remainingDependencies()).isZero();
        assertThat(dag.get(0).dependents()).isEmpty();
    }

    @Test
    void handlesTasksWithoutDependencyLists() {
        List<DagModel.DagTask> dag = DagModel.from(snapshot(
                new WorkflowDefinitionSnapshot.Task("a", "actA", null)));
        assertThat(dag.get(0).remainingDependencies()).isZero();
        assertThat(dag.get(0).dependents()).isEmpty();
    }
}
