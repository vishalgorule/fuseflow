package io.fuseflow.engine.repository;

import io.fuseflow.engine.model.WorkflowEvent;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import tools.jackson.databind.ObjectMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Append-only event log for execution history (event sourcing, FR-9). Every state transition
 * appends an immutable row via this single store; {@link #history} returns the deterministic
 * per-execution order (BIGSERIAL id). State transitions and their events are committed in the
 * same transaction (persist → append event → publish, architecture §10.1).
 */
@Repository
public class EventStore {

    private final JdbcClient jdbc;
    private final ObjectMapper objectMapper;

    public EventStore(JdbcClient jdbc, ObjectMapper objectMapper) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
    }

    public void append(UUID executionId, String eventType, Map<String, Object> payload) {
        jdbc.sql("""
                        INSERT INTO engine.workflow_event (workflow_execution_id, event_type, payload)
                        VALUES (:executionId, :eventType, CAST(:payload AS jsonb))
                        """)
                .param("executionId", executionId)
                .param("eventType", eventType)
                .param("payload", objectMapper.valueToTree(payload).toString())
                .update();
    }

    /** All events for an execution in immutable insertion order. */
    public List<WorkflowEvent> history(UUID executionId) {
        return jdbc.sql("""
                        SELECT id, workflow_execution_id, event_type, payload, created_at
                        FROM engine.workflow_event
                        WHERE workflow_execution_id = :executionId
                        ORDER BY id
                        """)
                .param("executionId", executionId)
                .query(this::mapRow)
                .list();
    }

    private WorkflowEvent mapRow(ResultSet rs, int rowNum) throws SQLException {
        return new WorkflowEvent(
                rs.getLong("id"),
                rs.getObject("workflow_execution_id", UUID.class),
                rs.getString("event_type"),
                rs.getString("payload"),
                rs.getTimestamp("created_at").toInstant());
    }
}
