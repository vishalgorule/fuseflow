package io.fuseflow.engine;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.List;
import java.util.UUID;

/**
 * Seeds the definition service's {@code definition} schema inside the Testcontainers Postgres
 * so the engine's {@code WorkflowDefinitionReader} (which reads that schema directly) has
 * workflows to execute. Mirrors the Phase 1 DDL; each test run starts from a fresh container.
 */
final class DefinitionSeeder {

    private DefinitionSeeder() {
    }

    /** A workflow definition to seed. */
    record WorkflowDef(UUID id, String name, List<Task> tasks) {

        record Task(String id, String activity, List<String> dependsOn) {
        }
    }

    static void createSchema(Connection connection) throws Exception {
        try (Statement statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA IF NOT EXISTS definition");
            statement.execute("""
                    CREATE TABLE IF NOT EXISTS definition.workflow_definition (
                        id          UUID PRIMARY KEY,
                        name        TEXT NOT NULL,
                        description TEXT,
                        version     BIGINT NOT NULL DEFAULT 0,
                        created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
                        updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
                        CONSTRAINT uq_workflow_definition_name UNIQUE (name)
                    )
                    """);
            statement.execute("""
                    CREATE TABLE IF NOT EXISTS definition.workflow_task (
                        workflow_id   UUID NOT NULL REFERENCES definition.workflow_definition (id) ON DELETE CASCADE,
                        task_id       TEXT NOT NULL,
                        activity_name TEXT NOT NULL,
                        PRIMARY KEY (workflow_id, task_id)
                    )
                    """);
            statement.execute("""
                    CREATE TABLE IF NOT EXISTS definition.task_dependency (
                        workflow_id UUID NOT NULL,
                        task_id     TEXT NOT NULL,
                        depends_on  TEXT NOT NULL,
                        PRIMARY KEY (workflow_id, task_id, depends_on),
                        FOREIGN KEY (workflow_id, task_id) REFERENCES definition.workflow_task (workflow_id, task_id) ON DELETE CASCADE,
                        FOREIGN KEY (workflow_id, depends_on) REFERENCES definition.workflow_task (workflow_id, task_id) ON DELETE CASCADE
                    )
                    """);
        }
    }

    static void seed(Connection connection, WorkflowDef... definitions) throws Exception {
        try (PreparedStatement def = connection.prepareStatement("""
                        INSERT INTO definition.workflow_definition (id, name, description, version)
                        VALUES (?, ?, ?, 1)
                        """);
             PreparedStatement task = connection.prepareStatement("""
                        INSERT INTO definition.workflow_task (workflow_id, task_id, activity_name)
                        VALUES (?, ?, ?)
                        """);
             PreparedStatement dep = connection.prepareStatement("""
                        INSERT INTO definition.task_dependency (workflow_id, task_id, depends_on)
                        VALUES (?, ?, ?)
                        """)) {
            for (WorkflowDef definition : definitions) {
                def.setObject(1, definition.id());
                def.setString(2, definition.name());
                def.setString(3, "seeded for integration tests");
                def.addBatch();
            }
            def.executeBatch();

            for (WorkflowDef definition : definitions) {
                for (WorkflowDef.Task t : definition.tasks()) {
                    task.setObject(1, definition.id());
                    task.setString(2, t.id());
                    task.setString(3, t.activity());
                    task.addBatch();
                }
            }
            task.executeBatch();

            for (WorkflowDef definition : definitions) {
                for (WorkflowDef.Task t : definition.tasks()) {
                    for (String dependsOn : t.dependsOn()) {
                        dep.setObject(1, definition.id());
                        dep.setString(2, t.id());
                        dep.setString(3, dependsOn);
                        dep.addBatch();
                    }
                }
            }
            dep.executeBatch();
        }
    }
}
