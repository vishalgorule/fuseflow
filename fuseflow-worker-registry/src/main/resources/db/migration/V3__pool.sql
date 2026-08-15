-- Phase 5: worker pool identity (pool-based routing).
--
-- * pool_name   — the worker pool (capability group) this worker joins. Workers in the same
--                 pool share a consumer group and the pool's dispatch topic
--                 (fuseflow-pool.<poolName>); the engine routes each task to exactly one pool.
-- * concurrency — pool-level declared parallelism advertised at registration; drives the pool
--                 topic's partition count (min(concurrency, cap)). May be null (defaults to 1).
--
-- Default 'default' keeps pre-Phase 5 registrations valid; existing rows migrate cleanly.

ALTER TABLE registry.worker ADD COLUMN pool_name TEXT NOT NULL DEFAULT 'default';
ALTER TABLE registry.worker ADD COLUMN concurrency INT;

-- Pool liveness lookup for the engine's routing table (which pools have ≥ 1 ONLINE worker).
CREATE INDEX idx_worker_pool ON registry.worker (pool_name, status);
