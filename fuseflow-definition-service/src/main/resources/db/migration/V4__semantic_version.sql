-- Phase 8: workflow definition multi-versioning.
--
-- A workflow now has a semantic version label (TEXT, default '1'), and the unique key becomes
-- (name, semantic_version): multiple versions of the same workflow can coexist, and each is an
-- immutable snapshot — changing the DAG means registering a new version, never mutating an
-- existing one. The existing BIGINT `version` column stays as the optimistic-lock / ETag column
-- (retained for backward compatibility; immutable rows never bump it in practice).
--
-- Existing rows migrate cleanly via the column default '1'.

ALTER TABLE definition.workflow_definition ADD COLUMN semantic_version TEXT NOT NULL DEFAULT '1';

ALTER TABLE definition.workflow_definition DROP CONSTRAINT uq_workflow_definition_name;
ALTER TABLE definition.workflow_definition
    ADD CONSTRAINT uq_workflow_definition_name_version UNIQUE (name, semantic_version);

-- SDK-side idempotent upsert lookups: by name (all versions) and by exact (name, version).
CREATE INDEX idx_workflow_definition_name ON definition.workflow_definition (name);
