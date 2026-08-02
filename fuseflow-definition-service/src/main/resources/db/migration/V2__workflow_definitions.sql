-- Phase 1: Workflow definition tables (owned by the Workflow Definition Service).
--
-- A workflow is a DAG of tasks; each task invokes an activity that is executed
-- by external workers. Dependencies are expressed per task via task_dependency;
-- referential integrity guarantees a dependency can never point at a task that
-- is not part of the same workflow.

CREATE TABLE definition.workflow_definition (
    id          UUID PRIMARY KEY,
    name        TEXT NOT NULL,
    description TEXT,
    version     BIGINT NOT NULL DEFAULT 0,          -- optimistic-lock column
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_workflow_definition_name UNIQUE (name)
);

-- Note: only workflow_definition carries a version column (optimistic-lock convention).
-- workflow_task/task_dependency are replaced wholesale on every update, so versioning
-- them individually would be meaningless; the definition row is the locking unit.
CREATE TABLE definition.workflow_task (
    workflow_id   UUID NOT NULL REFERENCES definition.workflow_definition (id) ON DELETE CASCADE,
    task_id       TEXT NOT NULL,
    activity_name TEXT NOT NULL,
    PRIMARY KEY (workflow_id, task_id)
);

CREATE TABLE definition.task_dependency (
    workflow_id UUID NOT NULL,
    task_id     TEXT NOT NULL,
    depends_on  TEXT NOT NULL,
    PRIMARY KEY (workflow_id, task_id, depends_on),
    FOREIGN KEY (workflow_id, task_id) REFERENCES definition.workflow_task (workflow_id, task_id) ON DELETE CASCADE,
    FOREIGN KEY (workflow_id, depends_on) REFERENCES definition.workflow_task (workflow_id, task_id) ON DELETE CASCADE
);
