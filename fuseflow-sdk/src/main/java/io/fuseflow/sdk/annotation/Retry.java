package io.fuseflow.sdk.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Retry policy for activities (Phase 7, FR-6). Usable on {@link Workflow} (workflow-level
 * default for every task) and on {@link Step}/{@link Activity} (per-task override — wins over
 * the workflow policy). Maps 1:1 to the shared {@code RetryPolicy} wire record; unset knobs
 * fall through to the workflow policy and finally to the engine's configured defaults.
 *
 * <p>Sentinel values mean "not specified": {@code maxAttempts = 0}, {@code fixedDelaySeconds
 * = 0} and {@code backoffMultiplier = 0} are ignored (an all-default {@code @Retry} is a
 * no-op). {@code exponentialBackoff} is opt-in — leave it {@code false} (default) for fixed
 * delays, set it {@code true} for {@code fixedDelaySeconds * multiplier^(attempt-1)} delays.
 *
 * <pre>{@code
 * @Workflow(name = "payments", retry = @Retry(maxAttempts = 3, fixedDelaySeconds = 5))
 * public class PaymentsWorkflow {
 *     @Activity(id = "charge", retry = @Retry(maxAttempts = 1))   // no retry for this task
 *     public String charge(ActivityContext ctx) { ... }
 * }
 * }</pre>
 */
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Retry {

    /** Total attempts including the first (>= 1). 0 (default) = use the workflow/engine default. */
    int maxAttempts() default 0;

    /** Fixed delay between attempts, in seconds. 0 (default) = use the workflow/engine default. */
    int fixedDelaySeconds() default 0;

    /** Opt into exponential backoff ({@code fixedDelaySeconds * multiplier^(attempt-1)}). */
    boolean exponentialBackoff() default false;

    /** Backoff multiplier (defaults to 2.0). 0 (default) = use the workflow/engine default. */
    double backoffMultiplier() default 0;

    /** Exception class names that must never be retried (exact match or trailing {@code *}). */
    String[] nonRetryableExceptions() default {};
}
