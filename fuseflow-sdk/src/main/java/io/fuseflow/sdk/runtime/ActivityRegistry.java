package io.fuseflow.sdk.runtime;

import io.fuseflow.sdk.core.ActivityContext;
import io.fuseflow.sdk.core.ActivityHandler;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The worker's capability map: activity name → {@link ActivityHandler}. Populated by the
 * {@link ActivityScanner} at startup from {@code @Activity} beans; drives both registry
 * registration and Kafka dispatch filtering.
 */
public class ActivityRegistry {

    private final Map<String, ActivityHandler> handlers = new LinkedHashMap<>();

    /** Registers an activity handler; fails fast on duplicate names. */
    public void register(String name, ActivityHandler handler) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Activity name must not be blank");
        }
        if (handlers.putIfAbsent(name, handler) != null) {
            throw new IllegalStateException("Duplicate activity registration: '" + name + "'");
        }
    }

    /** All advertised activity names, in registration order. */
    public List<String> names() {
        return List.copyOf(handlers.keySet());
    }

    public boolean supports(String name) {
        return handlers.containsKey(name);
    }

    public Object execute(String name, ActivityContext context) throws Exception {
        ActivityHandler handler = handlers.get(name);
        if (handler == null) {
            throw new IllegalArgumentException("Unknown activity: '" + name + "'");
        }
        return handler.execute(context);
    }

}
