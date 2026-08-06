package io.fuseflow.engine.repository;

import io.fuseflow.engine.model.WorkflowExecution;
import io.fuseflow.engine.model.WorkflowStatus;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** JDBC access to {@code engine.workflow_execution}. */
@Repository
public class WorkflowExecutionRepository {

    private static final String TABLE = "engine.workflow_execution";

    private final JdbcClient jdbc;

    public WorkflowExecutionRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public void insert(WorkflowExecution execution) {
        jdbc.sql("""
                        INSERT INTO %s (id, workflow_id, workflow_name, definition_version, input, status, created_at, updated_at, started_at)
                        VALUES (:id, :workflowId, :workflowName, :definitionVersion, CAST(:input AS jsonb), :status, :createdAt, :updatedAt, :startedAt)
                        """.formatted(TABLE))
                .param("id", execution.id())
                .param("workflowId", execution.workflowId())
                .param("workflowName", execution.workflowName())
                .param("definitionVersion", execution.definitionVersion())
                .param("input", execution.input())
                .param("status", execution.status().name())
                .param("createdAt", Timestamp.from(execution.createdAt()))
                .param("updatedAt", Timestamp.from(execution.updatedAt()))
                .param("startedAt", Timestamp.from(execution.startedAt()))
                .update();
    }

    public Optional<WorkflowExecution> findById(UUID id) {
        return jdbc.sql("""
                        SELECT id, workflow_id, workflow_name, definition_version, input, output, status, version,
                               created_at, updated_at, started_at, completed_at
                        FROM %s WHERE id = :id
                        """.formatted(TABLE))
                .param("id", id)
                .query(this::mapRow)
                .optional();
    }

    public List<WorkflowExecution> findAll() {
        return jdbc.sql("""
                        SELECT id, workflow_id, workflow_name, definition_version, input, output, status, version,
                               created_at, updated_at, started_at, completed_at
                        FROM %s ORDER BY created_at DESC, id
                        """.formatted(TABLE))
                .query(this::mapRow)
                .list();
    }

    /** Used by boot-time recovery to re-drive executions that were RUNNING when the engine stopped. */
    public List<WorkflowExecution> findByStatus(WorkflowStatus status) {
        return jdbc.sql("""
                        SELECT id, workflow_id, workflow_name, definition_version, input, output, status, version,
                               created_at, updated_at, started_at, completed_at
                        FROM %s WHERE status = :status ORDER BY created_at
                        """.formatted(TABLE))
                .param("status", status.name())
                .query(this::mapRow)
                .list();
    }

    /**
     * Terminal success transition (RUNNING → COMPLETED). Optimistic lock: returns {@code false}
     * when the row's version no longer matches (a concurrent transition already won).
     */
    public boolean markCompleted(UUID id, long expectedVersion) {
        return jdbc.sql("""
                        UPDATE %s
                        SET status = 'COMPLETED', version = version + 1, updated_at = :updatedAt, completed_at = :completedAt
                        WHERE id = :id AND status = 'RUNNING' AND version = :expectedVersion
                        """.formatted(TABLE))
                .param("id", id)
                .param("updatedAt", Timestamp.from(Instant.now()))
                .param("completedAt", Timestamp.from(Instant.now()))
                .param("expectedVersion", expectedVersion)
                .update() == 1;
    }

    /** Terminal failure transition (RUNNING → FAILED); optimistic lock as above. */
    public boolean markFailed(UUID id, long expectedVersion) {
        return jdbc.sql("""
                        UPDATE %s
                        SET status = 'FAILED', version = version + 1, updated_at = :updatedAt, completed_at = :completedAt
                        WHERE id = :id AND status = 'RUNNING' AND version = :expectedVersion
                        """.formatted(TABLE))
                .param("id", id)
                .param("updatedAt", Timestamp.from(Instant.now()))
                .param("completedAt", Timestamp.from(Instant.now()))
                .param("expectedVersion", expectedVersion)
                .update() == 1;
    }

    private WorkflowExecution mapRow(ResultSet rs, int rowNum) throws SQLException {
        Timestamp completedAt = rs.getTimestamp("completed_at");
        return new WorkflowExecution(
                rs.getObject("id", UUID.class),
                rs.getObject("workflow_id", UUID.class),
                rs.getString("workflow_name"),
                rs.getLong("definition_version"),
                rs.getString("input"),
                rs.getString("output"),
                WorkflowStatus.valueOf(rs.getString("status")),
                rs.getLong("version"),
                rs.getTimestamp("created_at").toInstant(),
                rs.getTimestamp("updated_at").toInstant(),
                rs.getTimestamp("started_at").toInstant(),
                completedAt == null ? null : completedAt.toInstant());
    }
}
