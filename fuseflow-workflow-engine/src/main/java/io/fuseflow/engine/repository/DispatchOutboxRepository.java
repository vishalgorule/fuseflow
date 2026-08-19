package io.fuseflow.engine.repository;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * JDBC access to {@code engine.dispatch_outbox} — the durable hand-off between the scheduling
 * transaction and the Kafka publish (post-Phase 7 hardening). A dispatch row is written in the
 * same transaction that marks the activity SCHEDULED, so an engine crash between commit and
 * publish can never lose a task: the poller publishes PENDING rows (immediately, or after
 * restart, or when the routing table gains the capability for unroutable ones).
 */
@Repository
public class DispatchOutboxRepository {

    private static final String TABLE = "engine.dispatch_outbox";
    private static final String COLUMNS =
            "id, workflow_execution_id, task_id, activity_name, input, attempt, status, error, created_at, published_at";

    private final JdbcClient jdbc;

    public DispatchOutboxRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** A pending dispatch row, rebuilt into an {@code ActivityTask} by the publisher. */
    public record Entry(UUID id, UUID workflowExecutionId, String taskId, String activityName,
                        String input, int attempt, String status, String error,
                        Instant createdAt, Instant publishedAt) {
    }

    /**
     * Inserts one dispatch row in the scheduling transaction (idempotent per
     * {@code (execution, task, attempt)} — a retry is a new attempt, so it never collides).
     */
    public void insert(UUID executionId, String taskId, String activityName, String input, int attempt) {
        jdbc.sql("""
                        INSERT INTO %s (workflow_execution_id, task_id, activity_name, input, attempt, created_at)
                        VALUES (:executionId, :taskId, :activityName, CAST(:input AS jsonb), :attempt, :now)
                        ON CONFLICT (workflow_execution_id, task_id, attempt) DO NOTHING
                        """.formatted(TABLE))
                .param("executionId", executionId)
                .param("taskId", taskId)
                .param("activityName", activityName)
                .param("input", input)
                .param("attempt", attempt)
                .param("now", Timestamp.from(Instant.now()))
                .update();
    }

    /** PENDING dispatch rows, oldest first, bounded per poll cycle (Phase 7 scale convention). */
    public List<Entry> findPending(int limit) {
        return jdbc.sql("""
                        SELECT %s FROM %s WHERE status = 'PENDING' ORDER BY created_at LIMIT :limit
                        """.formatted(COLUMNS, TABLE))
                .param("limit", limit)
                .query(this::mapRow)
                .list();
    }

    /** Marks the row published after the dispatch is handed to the dispatcher (guarded). */
    public boolean markPublished(UUID id) {
        return jdbc.sql("""
                        UPDATE %s SET status = 'PUBLISHED', published_at = :now
                        WHERE id = :id AND status = 'PENDING'
                        """.formatted(TABLE))
                .param("id", id)
                .param("now", Timestamp.from(Instant.now()))
                .update() == 1;
    }

    /**
     * Records the first unroutable sighting. Returns {@code true} only once per row — the
     * publisher uses it to append the {@code ActivityUnroutable} event exactly once instead of
     * spamming history every poll while the pool is away.
     */
    public boolean markUnroutable(UUID id, String reason) {
        return jdbc.sql("""
                        UPDATE %s SET error = :reason
                        WHERE id = :id AND status = 'PENDING' AND error IS NULL
                        """.formatted(TABLE))
                .param("id", id)
                .param("reason", reason)
                .update() == 1;
    }

    private Entry mapRow(ResultSet rs, int rowNum) throws SQLException {
        Timestamp publishedAt = rs.getTimestamp("published_at");
        return new Entry(
                rs.getObject("id", UUID.class),
                rs.getObject("workflow_execution_id", UUID.class),
                rs.getString("task_id"),
                rs.getString("activity_name"),
                rs.getString("input"),
                rs.getInt("attempt"),
                rs.getString("status"),
                rs.getString("error"),
                rs.getTimestamp("created_at").toInstant(),
                publishedAt == null ? null : publishedAt.toInstant());
    }
}
