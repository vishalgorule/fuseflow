package io.fuseflow.definition.repository;

import io.fuseflow.definition.model.WorkflowDefinition;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** JDBC access to {@code definition.workflow_definition}. */
@Repository
public class WorkflowDefinitionRepository {

    private static final String TABLE = "definition.workflow_definition";

    private static final String COLUMNS =
            "id, name, semantic_version, description, retry_policy, version, created_at, updated_at";

    private final JdbcClient jdbc;

    public WorkflowDefinitionRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public void insert(WorkflowDefinition definition) {
        jdbc.sql("""
                        INSERT INTO %s (id, name, semantic_version, description, retry_policy, version, created_at, updated_at)
                        VALUES (:id, :name, :semanticVersion, :description, CAST(:retryPolicy AS jsonb), :version, :createdAt, :updatedAt)
                        """.formatted(TABLE))
                .param("id", definition.id())
                .param("name", definition.name())
                .param("semanticVersion", definition.semanticVersion())
                .param("description", definition.description())
                .param("retryPolicy", definition.retryPolicyJson())
                .param("version", definition.version())
                .param("createdAt", Timestamp.from(definition.createdAt()))
                .param("updatedAt", Timestamp.from(definition.updatedAt()))
                .update();
    }

    public Optional<WorkflowDefinition> findById(UUID id) {
        return jdbc.sql("""
                        SELECT %s FROM %s WHERE id = :id
                        """.formatted(COLUMNS, TABLE))
                .param("id", id)
                .query(this::mapRow)
                .optional();
    }

    /** All versions of a workflow name, newest first (Phase 8). */
    public List<WorkflowDefinition> findAllByName(String name) {
        return jdbc.sql("""
                        SELECT %s FROM %s WHERE name = :name ORDER BY semantic_version DESC, created_at DESC
                        """.formatted(COLUMNS, TABLE))
                .param("name", name)
                .query(this::mapRow)
                .list();
    }

    /** The exact (name, semanticVersion) snapshot; empty when that version does not exist. */
    public Optional<WorkflowDefinition> findByNameAndVersion(String name, String semanticVersion) {
        return jdbc.sql("""
                        SELECT %s FROM %s WHERE name = :name AND semantic_version = :semanticVersion
                        """.formatted(COLUMNS, TABLE))
                .param("name", name)
                .param("semanticVersion", semanticVersion)
                .query(this::mapRow)
                .optional();
    }

    public List<WorkflowDefinition> findAll() {
        return jdbc.sql("""
                        SELECT %s FROM %s ORDER BY created_at DESC, id
                        """.formatted(COLUMNS, TABLE))
                .query(this::mapRow)
                .list();
    }

    /** Returns {@code false} if no workflow with the given id exists. */
    public boolean delete(UUID id) {
        return jdbc.sql("DELETE FROM %s WHERE id = :id".formatted(TABLE))
                .param("id", id)
                .update() == 1;
    }

    private WorkflowDefinition mapRow(ResultSet rs, int rowNum) throws SQLException {
        return new WorkflowDefinition(
                rs.getObject("id", UUID.class),
                rs.getString("name"),
                rs.getString("semantic_version"),
                rs.getString("description"),
                rs.getString("retry_policy"),
                rs.getLong("version"),
                rs.getTimestamp("created_at").toInstant(),
                rs.getTimestamp("updated_at").toInstant());
    }
}
