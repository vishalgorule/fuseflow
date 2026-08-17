-- Phase 7: retry policies (FR-6) on workflow definitions and their tasks.
--
-- Both columns are optional JSONB: a workflow-level policy is the default for every task,
-- and a per-task policy overrides it. The engine resolves task policy -> workflow policy ->
-- engine-configured defaults at failure time. JSONB keeps the nested policy (maxAttempts,
-- fixedDelaySeconds, exponentialBackoff, backoffMultiplier, nonRetryableExceptions) opaque
-- to the definition service — it validates + round-trips the shared RetryPolicy DTO.

ALTER TABLE definition.workflow_definition ADD COLUMN retry_policy JSONB;

ALTER TABLE definition.workflow_task ADD COLUMN retry_policy JSONB;
