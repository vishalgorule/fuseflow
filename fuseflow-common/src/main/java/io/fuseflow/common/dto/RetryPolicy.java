package io.fuseflow.common.dto;

import java.util.List;

/**
 * Retry policy for activities (Phase 7, FR-6). Shared wire contract: the SDK's
 * {@code @Retry} annotation maps onto this, the definition service validates + persists it,
 * and the engine's retry manager resolves it (per-task policy overrides the workflow-level
 * policy, which overrides the engine's configured defaults).
 *
 * <p>All fields are nullable — a {@code null} field means "not specified, fall through to the
 * next level" (task → workflow → engine defaults), so a policy can override just one knob.
 *
 * @param maxAttempts            total attempts including the first (>= 1); null = default
 * @param fixedDelaySeconds      fixed delay between attempts; null = default
 * @param exponentialBackoff     when true the delay after attempt N is
 *                               {@code fixedDelaySeconds * backoffMultiplier^(N-1)}
 * @param backoffMultiplier      multiplier for exponential backoff (defaults to 2.0)
 * @param nonRetryableExceptions exception class names that must not be retried (exact match
 *                               or trailing {@code *} wildcard); null/empty = none
 */
public record RetryPolicy(
        Integer maxAttempts,
        Integer fixedDelaySeconds,
        Boolean exponentialBackoff,
        Double backoffMultiplier,
        List<String> nonRetryableExceptions) {

    /** Whether any knob is set (a fully-null policy means "no override"). */
    public boolean isEmpty() {
        return maxAttempts == null && fixedDelaySeconds == null
                && exponentialBackoff == null && backoffMultiplier == null
                && (nonRetryableExceptions == null || nonRetryableExceptions.isEmpty());
    }
}
