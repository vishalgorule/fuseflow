package io.fuseflow.sdk.annotation;

import io.fuseflow.sdk.core.ActivityContext;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a bean method as an executable activity (Phase 4, FR-5). The SDK scans Spring beans at
 * startup, advertises the annotated names to the registry, and dispatches matching Kafka
 * messages to the method.
 *
 * <p>The annotated method must take exactly one {@link ActivityContext} parameter and may
 * return anything (a {@code String} is treated as raw JSON; other values are serialized to
 * JSON). Example:
 *
 * <pre>{@code
 * @Component
 * public class ImageWorker {
 *     @Activity("resizeImage")
 *     public Map<String, Object> resizeImage(ActivityContext ctx) { ... }
 * }
 * }</pre>
 *
 * <p><b>Name resolution (Phase 6):</b> when {@link #value()} is blank the activity name is the
 * method name (Temporal-style), so {@code @Activity} on {@code resizeImage(ActivityContext)}
 * advertises the {@code resizeImage} activity with no string at all.
 *
 * <p><b>Workflow steps (Phase 6, self-contained style):</b> on a {@link Workflow} class, the
 * annotated methods <b>are</b> the workflow's steps — {@link #id()} is the task id in the DAG
 * (defaults to the activity name) and {@link #dependsOn()} declares which steps must complete
 * first. The SDK builds the workflow DAG from these methods and registers the same methods as
 * executable activities, so one class defines a whole workflow:
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
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Activity {

    /**
     * The activity name workers advertise and the engine dispatches by. Blank (default) =
     * derive from the method name (Phase 6).
     */
    String value() default "";

    /**
     * Task id within the enclosing {@link Workflow}'s DAG (Phase 6, self-contained style).
     * Blank (default) = the activity name (i.e. the method name unless {@link #value()} is
     * set). Only meaningful on methods of a {@code @Workflow} class.
     */
    String id() default "";

    /**
     * Ids of steps that must complete before this step runs (Phase 6, self-contained style).
     * Only meaningful on methods of a {@code @Workflow} class.
     */
    String[] dependsOn() default {};
}
