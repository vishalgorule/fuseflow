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
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Activity {

    /** The activity name workers advertise and the engine dispatches by. */
    String value();
}
