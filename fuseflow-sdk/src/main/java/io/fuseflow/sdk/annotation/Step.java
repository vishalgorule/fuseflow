package io.fuseflow.sdk.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * A single step of an {@link Workflow} (Phase 6) — a pure declaration referencing an
 * {@link Activity} by name, mapping 1:1 to the JSON {@code Task(id, activity, dependsOn)}
 * wire record. Repeatable on the {@code @Workflow} class; dependency edges are declared here
 * ({@code dependsOn} = ids of other steps in the same class), so parallel branches and
 * fan-in joins fall out of the dependency graph.
 *
 * <p>This is the <b>metadata-only</b> style: steps reference activities that live in other
 * beans. Prefer the self-contained style — {@code @Activity} methods on the {@code @Workflow}
 * class with {@code id()}/{@code dependsOn()} — unless a fleet instance must advertise only
 * a subset of activities (per-activity gating).
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Repeatable(Steps.class)
public @interface Step {

    /** Unique task id within the workflow (must be referenced by {@code dependsOn}). */
    String id();

    /** The activity to execute — must match an {@link Activity} declared in this project. */
    String activity();

    /** Ids of steps that must complete before this one runs. */
    String[] dependsOn() default {};

    /**
     * Optional per-task retry policy (Phase 7, FR-6) — overrides the {@link Workflow}-level
     * {@link Retry}; unset knobs fall through to it and then to the engine defaults.
     */
    Retry retry() default @Retry;
}
