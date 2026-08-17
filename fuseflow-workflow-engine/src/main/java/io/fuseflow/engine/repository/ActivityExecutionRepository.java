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

    private static final String SELECT_COLUMNS =
            "workflow_execution_id, task_id, activity_name, status, remaining_dependencies, dependents,"
                    + " attempt, output, error, retry_due_at, error_type, version, created_at, updated_at";

    public List<ActivityExecution> findForExecution(UUID executionId) {
        return jdbc.sql("""
                        SELECT %s FROM %s WHERE workflow_execution_id = :executionId ORDER BY task_id
                        """.formatted(SELECT_COLUMNS, TABLE))
                .param("executionId", executionId)
                .query(this::mapRow)
                .list();
    }

    public Optional<ActivityExecution> findById(UUID executionId, String taskId) {
        return jdbc.sql("""
                        SELECT %s FROM %s WHERE workflow_execution_id = :executionId AND task_id = :taskId
                        """.formatted(SELECT_COLUMNS, TABLE))
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
                        SELECT %s FROM %s WHERE workflow_execution_id IN (:executionIds)
                        ORDER BY workflow_execution_id, task_id
                        """.formatted(SELECT_COLUMNS, TABLE))
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

    /**
     * Activities that were handed to a dispatcher but never finished (re-dispatch on recovery).
     * Phase 7: retry-waiting rows (SCHEDULED with a future {@code retry_due_at}) are on the
     * retry clock, not the dispatch clock — recovery must not fire them early, so they are
     * excluded until due.
     */
    public List<ActivityExecution> findStale(UUID executionId) {
        return jdbc.sql("""
                        SELECT %s FROM %s
                        WHERE workflow_execution_id = :executionId AND status IN ('SCHEDULED', 'STARTED')
                          AND (retry_due_at IS NULL OR retry_due_at <= now())
                        """.formatted(SELECT_COLUMNS, TABLE))
                .param("executionId", executionId)
                .query(this::mapRow)
                .list();
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
     * Accepts SCHEDULED or STARTED (see {@link #markCompleted}); records the exception class
     * name for the ActivityFailed/dead-letter surface.
     */
    public boolean markFailed(UUID executionId, String taskId, String error, String errorType, long expectedVersion) {
        return jdbc.sql("""
                        UPDATE %s
                        SET status = 'FAILED', error = :error, error_type = :errorType,
                            version = version + 1, updated_at = :now
                        WHERE workflow_execution_id = :executionId AND task_id = :taskId
                          AND status IN ('SCHEDULED', 'STARTED') AND version = :expectedVersion
                        """.formatted(TABLE))
                .param("executionId", executionId)
                .param("taskId", taskId)
                .param("error", error)
                .param("errorType", errorType)
                .param("expectedVersion", expectedVersion)
                .param("now", Timestamp.from(Instant.now()))
                .update() == 1;
    }

    // ---------------------------------------------------------------- retries (Phase 7)

    /**
     * A failed-but-retryable attempt: bumps the attempt, parks the row as SCHEDULED on the
     * due-time queue, and records the failure for diagnostics. The retry poller re-dispatches
     * with the new attempt when {@code retryDueAt} arrives.
     */
    public boolean markRetryWaiting(UUID executionId, String taskId, int newAttempt,
                                    Instant retryDueAt, String error, String errorType, long expectedVersion) {
        return jdbc.sql("""
                        UPDATE %s
                        SET status = 'SCHEDULED', attempt = :attempt, retry_due_at = :dueAt,
                            error = :error, error_type = :errorType,
                            version = version + 1, updated_at = :now
                        WHERE workflow_execution_id = :executionId AND task_id = :taskId
                          AND status IN ('SCHEDULED', 'STARTED') AND version = :expectedVersion
                        """.formatted(TABLE))
                .param("executionId", executionId)
                .param("taskId", taskId)
                .param("attempt", newAttempt)
                .param("dueAt", Timestamp.from(retryDueAt))
                .param("error", error)
                .param("errorType", errorType)
                .param("now", Timestamp.from(Instant.now()))
                .param("expectedVersion", expectedVersion)
                .update() == 1;
    }

    /**
     * Takes a due retry off the clock (guarded). The poller then re-dispatches with the row's
     * current attempt; a concurrent poller that already claimed it gets {@code false}.
     */
    public boolean clearRetryDue(UUID executionId, String taskId, long expectedVersion) {
        return jdbc.sql("""
                        UPDATE %s
                        SET retry_due_at = NULL, version = version + 1, updated_at = :now
                        WHERE workflow_execution_id = :executionId AND task_id = :taskId
                          AND status = 'SCHEDULED' AND retry_due_at IS NOT NULL AND version = :expectedVersion
                        """.formatted(TABLE))
                .param("executionId", executionId)
                .param("taskId", taskId)
                .param("now", Timestamp.from(Instant.now()))
                .param("expectedVersion", expectedVersion)
                .update() == 1;
    }

    /**
     * Due retries (SCHEDULED, retry clock elapsed) for the retry poller — oldest first, bounded
     * to {@code limit} per cycle (Phase 7 scale): a burst of due retries (e.g. after a
     * correlated outage) drains across cycles instead of loading every due row into memory at
     * once. Unclaimed rows stay due (oldest first → no starvation) and are picked up next cycle;
     * the version-guarded claim prevents concurrent pollers from double-dispatching.
     */
    public List<ActivityExecution> findDueRetries(int limit) {
        return jdbc.sql("""
                        SELECT %s FROM %s
                        WHERE status = 'SCHEDULED' AND retry_due_at IS NOT NULL AND retry_due_at <= now()
                        ORDER BY retry_due_at
                        LIMIT :limit
                        """.formatted(SELECT_COLUMNS, TABLE))
                .param("limit", limit)
                .query(this::mapRow)
                .list();
    }

    /** SCHEDULED activities that never started within the window (start-timeout scan). */
    public List<ActivityExecution> findStartTimeouts(Instant cutoff) {
        return jdbc.sql("""
                        SELECT %s FROM %s
                        WHERE status = 'SCHEDULED' AND retry_due_at IS NULL AND updated_at < :cutoff
                        """.formatted(SELECT_COLUMNS, TABLE))
                .param("cutoff", Timestamp.from(cutoff))
                .query(this::mapRow)
                .list();
    }

    /** STARTED activities that produced no result within the window (execution-timeout scan). */
    public List<ActivityExecution> findExecutionTimeouts(Instant cutoff) {
        return jdbc.sql("""
                        SELECT %s FROM %s
                        WHERE status = 'STARTED' AND updated_at < :cutoff
                        """.formatted(SELECT_COLUMNS, TABLE))
                .param("cutoff", Timestamp.from(cutoff))
                .query(this::mapRow)
                .list();
    }

    /**
     * Decrements a dependent activity's remaining-dependency count in one round trip, returning
     * the <em>updated</em> row (fresh counter + version) when the decrement won — the caller
     * schedules the dependent iff its counter reached 0, without a follow-up SELECT. Empty when
     * the row does not exist, is no longer PENDING, or the counter is already 0 (a concurrent
     * decrement from a sibling branch already satisfied it). Never goes negative.
     */
    public Optional<ActivityExecution> decrement(UUID executionId, String taskId) {
        return jdbc.sql("""
                        UPDATE %s
                        SET remaining_dependencies = remaining_dependencies - 1, version = version + 1, updated_at = :now
                        WHERE workflow_execution_id = :executionId AND task_id = :taskId
                          AND status = 'PENDING' AND remaining_dependencies > 0
                        RETURNING %s
                        """.formatted(TABLE, SELECT_COLUMNS))
                .param("executionId", executionId)
                .param("taskId", taskId)
                .param("now", Timestamp.from(Instant.now()))
                .query(this::mapRow)
                .optional();
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
        Timestamp retryDueAt = rs.getTimestamp("retry_due_at");
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
                retryDueAt == null ? null : retryDueAt.toInstant(),
                rs.getString("error_type"),
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
