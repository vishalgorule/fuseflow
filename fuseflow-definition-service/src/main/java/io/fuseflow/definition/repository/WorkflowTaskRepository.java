package io.fuseflow.definition.repository;

import io.fuseflow.definition.model.TaskDependency;
import io.fuseflow.definition.model.WorkflowTask;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

/** JDBC access to {@code definition.workflow_task} and {@code definition.task_dependency}. */
@Repository
public class WorkflowTaskRepository {

    private static final String TASK_TABLE = "definition.workflow_task";
    private static final String DEP_TABLE = "definition.task_dependency";

    private final JdbcClient jdbc;

    public WorkflowTaskRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Replaces the complete task graph of a workflow (used by create and update).
     * Callers must wrap this in a transaction.
     */
    public void replaceAll(UUID workflowId, List<WorkflowTask> tasks, List<TaskDependency> dependencies) {
        jdbc.sql("DELETE FROM " + DEP_TABLE + " WHERE workflow_id = :workflowId")
                .param("workflowId", workflowId)
                .update();
        jdbc.sql("DELETE FROM " + TASK_TABLE + " WHERE workflow_id = :workflowId")
                .param("workflowId", workflowId)
                .update();

        for (WorkflowTask task : tasks) {
            jdbc.sql("INSERT INTO " + TASK_TABLE + " (workflow_id, task_id, activity_name, retry_policy) "
                            + "VALUES (:workflowId, :taskId, :activity, CAST(:retryPolicy AS jsonb))")
                    .param("workflowId", task.workflowId())
                    .param("taskId", task.taskId())
                    .param("activity", task.activityName())
                    .param("retryPolicy", task.retryPolicyJson())
                    .update();
        }
        for (TaskDependency dependency : dependencies) {
            jdbc.sql("INSERT INTO " + DEP_TABLE + " (workflow_id, task_id, depends_on) "
                            + "VALUES (:workflowId, :taskId, :dependsOn)")
                    .param("workflowId", dependency.workflowId())
                    .param("taskId", dependency.taskId())
                    .param("dependsOn", dependency.dependsOn())
                    .update();
        }
    }

    public List<WorkflowTask> findTasks(UUID workflowId) {
        return jdbc.sql("SELECT workflow_id, task_id, activity_name, retry_policy FROM " + TASK_TABLE
                        + " WHERE workflow_id = :workflowId")
                .param("workflowId", workflowId)
                .query(this::mapTask)
                .list();
    }

    public List<TaskDependency> findDependencies(UUID workflowId) {
        return jdbc.sql("SELECT workflow_id, task_id, depends_on FROM " + DEP_TABLE
                        + " WHERE workflow_id = :workflowId ORDER BY task_id, depends_on")
                .param("workflowId", workflowId)
                .query(this::mapDependency)
                .list();
    }

    /** Batch loads tasks for many workflows in a single query (avoids N+1 on list). */
    public List<WorkflowTask> findTasksForWorkflows(List<UUID> workflowIds) {
        if (workflowIds.isEmpty()) {
            return List.of();
        }
        return jdbc.sql("SELECT workflow_id, task_id, activity_name, retry_policy FROM " + TASK_TABLE
                        + " WHERE workflow_id IN (:workflowIds)")
                .param("workflowIds", workflowIds)
                .query(this::mapTask)
                .list();
    }

    /** Batch loads dependencies for many workflows in a single query (avoids N+1 on list). */
    public List<TaskDependency> findDependenciesForWorkflows(List<UUID> workflowIds) {
        if (workflowIds.isEmpty()) {
            return List.of();
        }
        return jdbc.sql("SELECT workflow_id, task_id, depends_on FROM " + DEP_TABLE
                        + " WHERE workflow_id IN (:workflowIds) ORDER BY task_id, depends_on")
                .param("workflowIds", workflowIds)
                .query(this::mapDependency)
                .list();
    }

    private WorkflowTask mapTask(ResultSet rs, int rowNum) throws SQLException {
        return new WorkflowTask(
                rs.getObject("workflow_id", UUID.class),
                rs.getString("task_id"),
                rs.getString("activity_name"),
                rs.getString("retry_policy"));
    }

    private TaskDependency mapDependency(ResultSet rs, int rowNum) throws SQLException {
        return new TaskDependency(
                rs.getObject("workflow_id", UUID.class),
                rs.getString("task_id"),
                rs.getString("depends_on"));
    }
}
