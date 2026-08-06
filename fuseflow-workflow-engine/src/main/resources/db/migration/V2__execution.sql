-- Phase 2: Workflow engine tables (owned by the Workflow Engine).
--
-- Durable execution model:
--   * workflow_execution   — one row per runnable instance of a workflow definition.
--   * activity_execution   — one row per DAG task per execution; carries the materialized
--                            dependency graph (remaining_dependencies + dependents) so the
--                            scheduler never re-scans the definition DAG (dependency counting).
--   * workflow_event       — immutable, append-only event log (event sourcing / FR-9); the
--                            BIGSERIAL id gives a deterministic total order per execution.
--
-- The engine reads workflow definitions (read-only) from the definition service's
-- `definition` schema; it snapshots name + definition version onto the execution row so
-- recovery is self-contained and definitions are immutable once execution begins.

CREATE TABLE engine.workflow_execution (
    id                 UUID PRIMARY KEY,
    workflow_id        UUID NOT NULL,            -- definition id (no cross-schema FK; read-only from `definition`)
    workflow_name      TEXT NOT NULL,            -- snapshot from the definition
    definition_version BIGINT NOT NULL,          -- snapshot from the definition (optimistic-lock version)
    input              JSONB,
    output             JSONB,                    -- reserved for aggregate workflow output (not written in Phase 2)
    status             TEXT NOT NULL,            -- RUNNING | COMPLETED | FAILED
    version            BIGINT NOT NULL DEFAULT 0, -- optimistic-lock column
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at       TIMESTAMPTZ
);

CREATE TABLE engine.activity_execution (
    workflow_execution_id  UUID NOT NULL REFERENCES engine.workflow_execution (id) ON DELETE CASCADE,
    task_id                TEXT NOT NULL,
    activity_name          TEXT NOT NULL,
    status                 TEXT NOT NULL,         -- PENDING | SCHEDULED | STARTED | COMPLETED | FAILED
    remaining_dependencies INT  NOT NULL DEFAULT 0,
    dependents             JSONB NOT NULL,        -- task ids to decrement when this activity completes
    attempt                INT  NOT NULL DEFAULT 1,
    output                 JSONB,
    error                  TEXT,
    version                BIGINT NOT NULL DEFAULT 0, -- optimistic-lock column
    created_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (workflow_execution_id, task_id)
);

-- Recovery scan: RUNNING executions with non-terminal activities.
CREATE INDEX idx_activity_execution_recovery
    ON engine.activity_execution (workflow_execution_id, status)
    WHERE status IN ('PENDING', 'SCHEDULED', 'STARTED');

CREATE TABLE engine.workflow_event (
    id                    BIGSERIAL PRIMARY KEY,  -- monotonic insert order per execution
    workflow_execution_id UUID NOT NULL REFERENCES engine.workflow_execution (id) ON DELETE CASCADE,
    event_type            TEXT NOT NULL,
    payload               JSONB NOT NULL,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_workflow_event_history
    ON engine.workflow_event (workflow_execution_id, id);
