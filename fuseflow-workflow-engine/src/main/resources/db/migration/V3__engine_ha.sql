-- Phase 5: engine high availability — execution sharding.
--
-- shard = floorMod(execution_id.hashCode(), shard_count), computed at start and stored on the
-- row so boot-time recovery can be scoped per engine instance: each instance recovers only the
-- shards it owns (fuseflow.engine.owned-shards), preventing duplicate re-dispatch when several
-- engine instances run the same activity-results consumer group.

ALTER TABLE engine.workflow_execution ADD COLUMN shard INT NOT NULL DEFAULT 0;

-- Shard-scoped recovery scan (replaces the full-table RUNNING scan).
CREATE INDEX idx_workflow_execution_recovery
    ON engine.workflow_execution (status, shard);
