-- Phase 7 scale: per-execution completion counter.
--
-- workflow_execution.remaining_activities tracks how many activities are still non-terminal,
-- decremented transactionally on every terminal completion. When it reaches 0 the workflow is
-- complete — replacing the per-completion COUNT(*) scan (countNonTerminal) with one guarded
-- O(1) decrement. The UPDATE row lock serializes sibling completions, so exactly one of them
-- sees 0 and "last one wins" is exact and race-free, including multi-sink DAGs.
ALTER TABLE engine.workflow_execution ADD COLUMN remaining_activities INT NOT NULL DEFAULT 0;

-- Backfill: RUNNING executions get their live non-terminal activity count; terminal ones get
-- 0, which is correct — recovery never touches completed/failed executions again. Flyway runs
-- this before the engine's boot-time recovery (ApplicationRunner), so no writes race it.
UPDATE engine.workflow_execution w
SET remaining_activities = (
    SELECT COUNT(*) FROM engine.activity_execution a
    WHERE a.workflow_execution_id = w.id
      AND a.status IN ('PENDING', 'SCHEDULED', 'STARTED')
);
