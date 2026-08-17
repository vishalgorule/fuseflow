-- Phase 7: reliability — retries, timeouts, dead-lettering (FR-6, FR-7).
--
-- activity_execution gains:
--   * retry_due_at — the DB-polled due-time queue: a failed-but-retryable attempt sets this to
--     now + policy delay and stays SCHEDULED; the retry poller re-dispatches when due. NULL
--     means the activity is on the normal dispatch path, not a retry clock.
--   * error_type  — the failing exception class name (from the worker's FAILED signal), used to
--     match nonRetryableExceptions and reported on ActivityFailed/dead-letter messages.

ALTER TABLE engine.activity_execution ADD COLUMN retry_due_at TIMESTAMPTZ;
ALTER TABLE engine.activity_execution ADD COLUMN error_type TEXT;

-- Retry poller scan (due retries) + start-timeout scan (SCHEDULED, no retry clock).
CREATE INDEX idx_activity_execution_retry_due
    ON engine.activity_execution (retry_due_at)
    WHERE status = 'SCHEDULED' AND retry_due_at IS NOT NULL;

-- Execution-timeout scan (STARTED activities that never produced a result).
CREATE INDEX idx_activity_execution_started_at
    ON engine.activity_execution (updated_at)
    WHERE status = 'STARTED';
