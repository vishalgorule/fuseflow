-- Phase 7 scale: index for the start-timeout scan.
--
-- findStartTimeouts (status='SCHEDULED' AND retry_due_at IS NULL AND updated_at < cutoff) had
-- no index — every poll walked the whole activity_execution table even when nothing was stuck.
-- This partial index bounds the scan to at-risk rows only (SCHEDULED activities not on the
-- retry clock, ordered by last update), so each poll reads just the genuinely stuck set. The
-- retry-due and execution-timeout scans already had partial indexes (V4).
CREATE INDEX idx_activity_execution_start_timeout
    ON engine.activity_execution (updated_at)
    WHERE status = 'SCHEDULED' AND retry_due_at IS NULL;
