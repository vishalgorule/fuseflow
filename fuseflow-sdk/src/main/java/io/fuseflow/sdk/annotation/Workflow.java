package io.fuseflow.sdk.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares a workflow definition <b>in code</b> (Phase 6, FR-1 extension). The SDK scans
 * {@code @Workflow} classes at startup, validates the DAG, and registers the definition with
 * the definition service (which remains the source of truth).
 *
 * <p>Steps map 1:1 to the JSON {@code Task(id, activity, dependsOn)} wire record; parallel
 * branches and fan-in joins fall out of the dependency graph. The class must be a Spring bean
 * (e.g. {@code @Component}) so the SDK scanner can see it. The workflow's {@code name} is
 * unique: re-registering the same name with an identical DAG is a no-op; a different DAG
 * replaces it (idempotent upsert by name).
 *
 * <p>Two declaration styles are supported (detected automatically):
 * <ul>
 *   <li><b>Self-contained (default):</b> the class's own {@link Activity} methods are the
 *       steps — {@code id()}/{@code dependsOn()} on the methods declare the DAG, the activity
 *       name is the method name, and the same methods are registered as executable
 *       activities. One class defines and implements the whole workflow.</li>
 *   <li><b>Metadata-only {@link Step}:</b> steps are pure declarations referencing activities
 *       by name; the activities live in other beans (or another deployable). Needed when a
 *       fleet instance must advertise only a subset of activities.</li>
 * </ul>
 *
 * <p>{@code pool} and {@code concurrency} are <b>worker</b> concerns (see
 * {@code fuseflow.worker.pool}/{@code fuseflow.worker.concurrency}) and deliberately do not
 * appear here — a workflow never owns a pool; the engine routes each task to any capable
 * worker pool at schedule time. Multi-versioning lands in Phase 8 ({@code @Workflow} carries
 * no {@code version}).
 *
 * <pre>{@code
 * @Component
 * @Workflow(name = "order-fulfillment", description = "Validate then ship")
 * public class OrderFulfillmentWorkflow {
 *     @Activity(id = "validate")
 *     public String validate(ActivityContext ctx) { ... }
 *
 *     @Activity(id = "ship", dependsOn = "validate")
 *     public String ship(ActivityContext ctx) { ... }
 * }
 * }</pre>
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Workflow {

    /** Unique workflow name — the upsert key at the definition service. */
    String name();

    /**
     * Semantic version label (Phase 8). Definitions are immutable version snapshots at the
     * definition service: {@code (name, version)} is the unique key and changing a DAG means
     * registering a new version. Re-registering the same name with an identical DAG on the
     * same version is a no-op; a <b>different</b> DAG on the same version fails loud at boot
     * (the registrar tells the operator to bump this attribute). Defaults to {@code "1"} so
     * pre-Phase 8 workflows keep working unchanged.
     */
    String version() default "1";

    /** Optional human-readable description. */
    String description() default "";

    /**
     * Optional workflow-level retry policy (Phase 7, FR-6) — the default for every task.
     * A per-task {@link Retry} on {@link Step}/{@link Activity} overrides it; unset knobs
     * fall through to the engine's configured defaults. An all-default {@code @Retry} is
     * treated as "no policy".
     */
    Retry retry() default @Retry;
}
