package io.fuseflow.sdk.core;

/**
 * Executable unit behind a registered activity name. Implemented by the SDK's scanner wrapping
 * an {@link io.fuseflow.sdk.annotation.Activity} method; exceptions are converted to FAILED
 * results by the worker runtime.
 */
@FunctionalInterface
public interface ActivityHandler {

    /**
     * Executes the activity and returns its output. A {@code String} return is treated as raw
     * JSON; any other value is serialized to JSON by the SDK.
     */
    Object execute(ActivityContext context) throws Exception;
}
