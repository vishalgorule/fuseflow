package io.fuseflow.engine.repository;

import io.fuseflow.engine.model.ActivityExecution;
import io.fuseflow.engine.model.ActivityStatus;
import io.fuseflow.engine.model.DagModel;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * JDBC access to {@code engine.activity_execution}. All state transitions are guarded
 * conditional updates (optimistic locking): a zero-row update means a concurrent transition
 * or stale dispatch already won, and callers simply skip their side effects.
 */
@Repository
public class ActivityExecutionRepository {

    private static final String TABLE = "engine.activity_execution";

    private final JdbcClient jdbc;
    private final ObjectMapper objectMapper;

    public ActivityExecutionRepository(JdbcClient jdbc, ObjectMapper objectMapper) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
    }

    /** Seeds one row per task with the materialized dependency graph (status PENDING, attempt 1). */
    public void insertAll(UUID executionId, List<DagModel.DagTask> tasks) {
        for (DagModel.DagTask task : tasks) {
            jdbc.sql("""
                            INSERT INTO %s (workflow_execution_id, task_id, activity_name, status, remaining_dependencies, dependents, attempt, created_at, updated_at)
                            VALUES (:executionId, :taskId, :activityName, 'PENDING', :remaining, CAST(:dependents AS jsonb), 1, :now, :now)
                            """.formatted(TABLE))
                    .param("executionId", executionId)
                    .param("taskId", task.taskId())
                    .param("activityName", task.activityName())
                    .param("remaining", task.remainingDependencies())
                    .param("dependents", toJson(task.dependents()))
                    .param("now", Timestamp.from(Instant.now()))
                    .update();
        }
    }

    public List<ActivityExecution> findForExecution(UUID executionId) {
        return jdbc.sql("""
                        SELECT workflow_execution_id, task_id, activity_name, status, remaining_dependencies,
                               dependents, attempt, output, error, version, created_at, updated_at
                        FROM %s WHERE workflow_execution_id = :executionId ORDER BY task_id
                        """.formatted(TABLE))
                .param("executionId", executionId)
                .query(this::mapRow)
                .list();
    }

    public Optional<ActivityExecution> findById(UUID executionId, String taskId) {
        return jdbc.sql("""
                        SELECT workflow_execution_id, task_id, activity_name, status, remaining_dependencies,
                               dependents, attempt, output, error, version, created_at, updated_at
                        FROM %s WHERE workflow_execution_id = :executionId AND task_id = :taskId
                        """.formatted(TABLE))
                .param("executionId", executionId)
                .param("taskId", taskId)
                .query(this::mapRow)
                .optional();
    }

    /**
     * Batch loads activities for many executions in a single query (avoids N+1 on list, mirroring
     * the Phase 1 {@code findTasksForWorkflows} convention).
     */
    public List<ActivityExecution> findForExecutions(List<UUID> executionIds) {
        if (executionIds.isEmpty()) {
            return List.of();
        }
        return jdbc.sql("""
                        SELECT workflow_execution_id, task_id, activity_name, status, remaining_dependencies,
                               dependents, attempt, output, error, version, created_at, updated_at
                        FROM %s WHERE workflow_execution_id IN (:executionIds)
                        ORDER BY workflow_execution_id, task_id
                        """.formatted(TABLE))
                .param("executionIds", executionIds)
                .query(this::mapRow)
                .list();
    }

    // ---------------------------------------------------------------- recovery scans

    /** PENDING activities whose dependencies are all satisfied (safety net on recovery). */
    public List<String> findRunnableTaskIds(UUID executionId) {
        return jdbc.sql("""
                        SELECT task_id FROM %s
                        WHERE workflow_execution_id = :executionId AND status = 'PENDING' AND remaining_dependencies = 0
                        """.formatted(TABLE))
                .param("executionId", executionId)
                .query((rs, rowNum) -> rs.getString("task_id"))
                .list();
    }

    /** Activities that were handed to a dispatcher but never finished (re-dispatch on recovery). */
    public List<ActivityExecution> findStale(UUID executionId) {
        return jdbc.sql("""
                        SELECT workflow_execution_id, task_id, activity_name, status, remaining_dependencies,
                               dependents, attempt, output, error, version, created_at, updated_at
                        FROM %s
                        WHERE workflow_execution_id = :executionId AND status IN ('SCHEDULED', 'STARTED')
                        """.formatted(TABLE))
                .param("executionId", executionId)
                .query(this::mapRow)
                .list();
    }

    /** Number of activities still in a non-terminal state (0 → workflow can complete). */
    public long countNonTerminal(UUID executionId) {
        return jdbc.sql("""
                        SELECT COUNT(*) FROM %s
                        WHERE workflow_execution_id = :executionId AND status IN ('PENDING', 'SCHEDULED', 'STARTED')
                        """.formatted(TABLE))
                .param("executionId", executionId)
                .query((rs, rowNum) -> rs.getLong(1))
                .optional()
                .orElse(0L);
    }

    // ---------------------------------------------------------------- transitions

    /** PENDING → SCHEDULED (idempotent: false if already scheduled or terminal). */
    public boolean markScheduled(UUID executionId, String taskId, long expectedVersion) {
        return jdbc.sql("""
                        UPDATE %s
                        SET status = 'SCHEDULED', version = version + 1, updated_at = :now
                        WHERE workflow_execution_id = :executionId AND task_id = :taskId
                          AND status = 'PENDING' AND version = :expectedVersion
                        """.formatted(TABLE))
                .param("executionId", executionId)
                .param("taskId", taskId)
                .param("expectedVersion", expectedVersion)
                .param("now", Timestamp.from(Instant.now()))
                .update() == 1;
    }

    /**
     * SCHEDULED/STARTED → STARTED. Allowing STARTED → STARTED makes re-dispatch after recovery
     * idempotent: with the in-memory dispatcher there is no durable in-flight progress, so a
     * restarted engine re-hands the activity to the dispatcher.
     */
    public boolean markStarted(UUID executionId, String taskId, long expectedVersion) {
        return jdbc.sql("""
                        UPDATE %s
                        SET status = 'STARTED', version = version + 1, updated_at = :now
                        WHERE workflow_execution_id = :executionId AND task_id = :taskId
                          AND status IN ('SCHEDULED', 'STARTED') AND version = :expectedVersion
                        """.formatted(TABLE))
                .param("executionId", executionId)
                .param("taskId", taskId)
                .param("expectedVersion", expectedVersion)
                .param("now", Timestamp.from(Instant.now()))
                .update() == 1;
    }

    /**
     * In-flight → COMPLETED with output (idempotent; false if stale/duplicate completion).
     * Accepts SCHEDULED or STARTED: with the Kafka dispatcher (Phase 4) a worker may complete
     * before its STARTED signal is consumed.
     */
    public boolean markCompleted(UUID executionId, String taskId, String output, long expectedVersion) {
        return jdbc.sql("""
                        UPDATE %s
                        SET status = 'COMPLETED', output = CAST(:output AS jsonb), version = version + 1, updated_at = :now
                        WHERE workflow_execution_id = :executionId AND task_id = :taskId
                          AND status IN ('SCHEDULED', 'STARTED') AND version = :expectedVersion
                        """.formatted(TABLE))
                .param("executionId", executionId)
                .param("taskId", taskId)
                .param("output", output)
                .param("expectedVersion", expectedVersion)
                .param("now", Timestamp.from(Instant.now()))
                .update() == 1;
    }

    /**
     * In-flight → FAILED with error message (idempotent; false if stale/duplicate completion).
     * Accepts SCHEDULED or STARTED (see {@link #markCompleted}).
     */
    public boolean markFailed(UUID executionId, String taskId, String error, long expectedVersion) {
        return jdbc.sql("""
                        UPDATE %s
                        SET status = 'FAILED', error = :error, version = version + 1, updated_at = :now
                        WHERE workflow_execution_id = :executionId AND task_id = :taskId
                          AND status IN ('SCHEDULED', 'STARTED') AND version = :expectedVersion
                        """.formatted(TABLE))
                .param("executionId", executionId)
                .param("taskId", taskId)
                .param("error", error)
                .param("expectedVersion", expectedVersion)
                .param("now", Timestamp.from(Instant.now()))
                .update() == 1;
    }

    /**
     * Decrements a dependent activity's remaining-dependency count. Returns {@code false} when
     * the row does not exist, is no longer PENDING, or the counter is already 0 (concurrent
     * decrement from a sibling branch already satisfied it). Never goes negative.
     */
    public boolean decrement(UUID executionId, String taskId) {
        return jdbc.sql("""
                        UPDATE %s
                        SET remaining_dependencies = remaining_dependencies - 1, version = version + 1, updated_at = :now
                        WHERE workflow_execution_id = :executionId AND task_id = :taskId
                          AND status = 'PENDING' AND remaining_dependencies > 0
                        """.formatted(TABLE))
                .param("executionId", executionId)
                .param("taskId", taskId)
                .param("now", Timestamp.from(Instant.now()))
                .update() == 1;
    }

    // ---------------------------------------------------------------- helpers

    private String toJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to serialize activity metadata", ex);
        }
    }

    private ActivityExecution mapRow(ResultSet rs, int rowNum) throws SQLException {
        return new ActivityExecution(
                rs.getObject("workflow_execution_id", UUID.class),
                rs.getString("task_id"),
                rs.getString("activity_name"),
                ActivityStatus.valueOf(rs.getString("status")),
                rs.getInt("remaining_dependencies"),
                parseDependents(rs.getString("dependents")),
                rs.getInt("attempt"),
                rs.getString("output"),
                rs.getString("error"),
                rs.getLong("version"),
                rs.getTimestamp("created_at").toInstant(),
                rs.getTimestamp("updated_at").toInstant());
    }

    private List<String> parseDependents(String json) {
        try {
            return objectMapper.readValue(json, new TypeReference<List<String>>() {
            });
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to deserialize dependents", ex);
        }
    }
}
