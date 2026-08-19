-- Post-Phase 7 reliability hardening: the dispatch outbox.
--
-- The engine's dispatch path used to be *mark SCHEDULED in a transaction → publish to Kafka
-- after commit* (AfterCommitDispatcher). A crash between the DB commit and the Kafka publish
-- lost the dispatch for good — recovered only at the next engine boot, or by burning a
-- start-timeout retry. The outbox closes that window: the dispatch row is written in the SAME
-- transaction that marks the activity SCHEDULED, and a poller (DispatchOutboxPublisher)
-- publishes it — immediately, or after a crash, or when the routing table gains the capability
-- (unroutable tasks wait here instead of burning retry attempts).
--
-- * id / workflow_execution_id / task_id / activity_name / input / attempt — enough to
--   rebuild the ActivityTask exactly as scheduled.
-- * status — PENDING (waiting for the publisher) → PUBLISHED (handed to Kafka). A PUBLISHED
--   message can still be lost if the broker is down — boot-time recovery re-dispatches it,
--   because findStale only excludes PENDING rows.
-- * error — the first unroutable reason; the publisher appends the ActivityUnroutable event
--   once per row (dedup via "error IS NULL"), then silently retries it each poll.
-- * published_at — the dispatch-time clock for the start-timeout scan: a task that waited in
--   the outbox for a pool must not be start-timeout'd by its stale updated_at.
CREATE TABLE IF NOT EXISTS engine.dispatch_outbox (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_execution_id UUID NOT NULL,
    task_id TEXT NOT NULL,
    activity_name TEXT NOT NULL,
    input JSONB,
    attempt INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING',
    error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);

-- One dispatch per (execution, task, attempt) — retries insert a new attempt; recovery's
-- direct re-dispatch of PUBLISHED-but-lost rows intentionally bypasses the outbox.
CREATE UNIQUE INDEX IF NOT EXISTS uq_dispatch_outbox_exec_task_attempt
    ON engine.dispatch_outbox (workflow_execution_id, task_id, attempt);

-- Publisher poll (PENDING, oldest first) + the start-timeout NOT EXISTS correlation.
CREATE INDEX IF NOT EXISTS idx_dispatch_outbox_status
    ON engine.dispatch_outbox (status, created_at);

-- Pool-rejoin sweep: runnable PENDING activities (deps satisfied) of RUNNING executions,
-- re-driven when the routing table gains a capability. Rare, but index it.
CREATE INDEX IF NOT EXISTS idx_activity_execution_runnable_pending
    ON engine.activity_execution (workflow_execution_id)
    WHERE status = 'PENDING' AND remaining_dependencies = 0;
