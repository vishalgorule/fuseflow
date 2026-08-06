package io.fuseflow.engine.definition;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Read-only access to the definition service's {@code definition} schema.
 *
 * <p>Phase 2 decision (confirmed with the user): the engine reads workflow definitions
 * directly via schema-qualified SQL instead of calling the definition service over REST.
 * This couples the engine to the definition tables, deviating from the per-service-ownership
 * convention — mitigated by (a) strictly read-only queries, (b) snapshotting name + version
 * onto {@code engine.workflow_execution} so recovery never depends on this schema, and
 * (c) isolating the lookup behind this class so it can be swapped for REST/Kafka later
 * without touching engine internals.
 */
@Repository
public class WorkflowDefinitionReader {

    private final JdbcClient jdbc;

    public WorkflowDefinitionReader(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Loads a workflow definition (tasks + dependencies) or {@link Optional#empty()} if unknown.
     * Read-only transaction so the existence check + task graph are read as one consistent
     * snapshot even if the definition service updates the definition concurrently.
     */
    @Transactional(readOnly = true)
    public Optional<WorkflowDefinitionSnapshot> find(UUID workflowId) {
        Optional<DefinitionRow> row = jdbc.sql("""
                        SELECT id, name, version
                        FROM definition.workflow_definition
                        WHERE id = :id
                        """)
                .param("id", workflowId)
                .query(this::mapDefinition)
                .optional();
        return row.map(this::loadGraph);
    }

    private WorkflowDefinitionSnapshot loadGraph(DefinitionRow definition) {
        Map<String, WorkflowDefinitionSnapshot.Task> tasksById = new LinkedHashMap<>();
        jdbc.sql("""
                        SELECT workflow_id, task_id, activity_name
                        FROM definition.workflow_task
                        WHERE workflow_id = :workflowId
                        """)
                .param("workflowId", definition.id())
                .query((rs, rowNum) -> {
                    String taskId = rs.getString("task_id");
                    tasksById.put(taskId, new WorkflowDefinitionSnapshot.Task(taskId, rs.getString("activity_name"), List.of()));
                    return null;
                })
                .list();

        jdbc.sql("""
                        SELECT task_id, depends_on
                        FROM definition.task_dependency
                        WHERE workflow_id = :workflowId
                        ORDER BY task_id, depends_on
                        """)
                .param("workflowId", definition.id())
                .query((rs, rowNum) -> {
                    WorkflowDefinitionSnapshot.Task task = tasksById.get(rs.getString("task_id"));
                    if (task != null) {
                        tasksById.put(task.id(),
                                new WorkflowDefinitionSnapshot.Task(task.id(), task.activity(),
                                        concat(task.dependsOn(), rs.getString("depends_on"))));
                    }
                    return null;
                })
                .list();

        return new WorkflowDefinitionSnapshot(definition.id(), definition.name(), definition.version(),
                List.copyOf(tasksById.values()));
    }

    private static List<String> concat(List<String> existing, String extra) {
        return java.util.stream.Stream.concat(existing.stream(), java.util.stream.Stream.of(extra)).toList();
    }

    private DefinitionRow mapDefinition(ResultSet rs, int rowNum) throws SQLException {
        return new DefinitionRow(rs.getObject("id", UUID.class), rs.getString("name"), rs.getLong("version"));
    }

    private record DefinitionRow(UUID id, String name, long version) {
    }
}
