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

    private final JdbcClient jdbc;

    public WorkflowDefinitionRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public void insert(WorkflowDefinition definition) {
        jdbc.sql("""
                        INSERT INTO %s (id, name, description, version, created_at, updated_at)
                        VALUES (:id, :name, :description, :version, :createdAt, :updatedAt)
                        """.formatted(TABLE))
                .param("id", definition.id())
                .param("name", definition.name())
                .param("description", definition.description())
                .param("version", definition.version())
                .param("createdAt", Timestamp.from(definition.createdAt()))
                .param("updatedAt", Timestamp.from(definition.updatedAt()))
                .update();
    }

    public Optional<WorkflowDefinition> findById(UUID id) {
        return jdbc.sql("""
                        SELECT id, name, description, version, created_at, updated_at
                        FROM %s WHERE id = :id
                        """.formatted(TABLE))
                .param("id", id)
                .query(this::mapRow)
                .optional();
    }

    public Optional<WorkflowDefinition> findByName(String name) {
        return jdbc.sql("""
                        SELECT id, name, description, version, created_at, updated_at
                        FROM %s WHERE name = :name
                        """.formatted(TABLE))
                .param("name", name)
                .query(this::mapRow)
                .optional();
    }

    public List<WorkflowDefinition> findAll() {
        return jdbc.sql("""
                        SELECT id, name, description, version, created_at, updated_at
                        FROM %s ORDER BY created_at DESC, id
                        """.formatted(TABLE))
                .query(this::mapRow)
                .list();
    }

    /**
     * Optimistic update of the mutable columns; returns {@code false} when the
     * row's version no longer matches {@code expectedVersion} (concurrent write).
     */
    public boolean update(UUID id, String name, String description, long expectedVersion) {
        return jdbc.sql("""
                        UPDATE %s
                        SET name = :name, description = :description,
                            version = version + 1, updated_at = :updatedAt
                        WHERE id = :id AND version = :expectedVersion
                        """.formatted(TABLE))
                .param("id", id)
                .param("name", name)
                .param("description", description)
                .param("updatedAt", Timestamp.from(Instant.now()))
                .param("expectedVersion", expectedVersion)
                .update() == 1;
    }

    /** Returns {@code false} if no workflow with the given id exists. */
    public boolean delete(UUID id) {
        return jdbc.sql("DELETE FROM %s WHERE id = :id".formatted(TABLE))
                .param("id", id)
                .update() == 1;
    }

    public boolean existsByNameExcluding(String name, UUID excludeId) {
        return jdbc.sql("SELECT 1 FROM %s WHERE name = :name AND id <> :excludeId".formatted(TABLE))
                .param("name", name)
                .param("excludeId", excludeId)
                .query((rs, rowNum) -> rs.getInt(1))
                .optional()
                .isPresent();
    }

    private WorkflowDefinition mapRow(ResultSet rs, int rowNum) throws SQLException {
        return new WorkflowDefinition(
                rs.getObject("id", UUID.class),
                rs.getString("name"),
                rs.getString("description"),
                rs.getLong("version"),
                rs.getTimestamp("created_at").toInstant(),
                rs.getTimestamp("updated_at").toInstant());
    }
}
