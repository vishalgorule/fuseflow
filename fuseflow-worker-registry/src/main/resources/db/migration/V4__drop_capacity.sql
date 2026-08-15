-- Phase 5 follow-up: per-worker capacity was declared but never consumed — parallelism is a
-- pool-level property (pool concurrency drives the pool topic's partition count, and Kafka
-- consumer groups self-pace), so drop it from the worker row and the heartbeat log.
ALTER TABLE registry.worker DROP COLUMN capacity;
ALTER TABLE registry.worker_heartbeat DROP COLUMN capacity;
