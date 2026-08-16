package io.fuseflow.sdk.runtime;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The workflows discovered by the {@link WorkflowScanner} at startup (Phase 6), keyed by name.
 * Consumed by the {@link WorkflowRegistrar} to upsert each definition at the definition
 * service. Mirrors the {@link ActivityRegistry} pattern: the scanner fills it, the runtime
 * consumes it.
 */
public class WorkflowRegistry {

    private final Map<String, WorkflowRegistration> registrations = new LinkedHashMap<>();

    /** Registers a scanned workflow; fails fast on duplicate names within one project. */
    public void register(WorkflowRegistration registration) {
        String name = registration.name();
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("@Workflow name must not be blank");
        }
        if (registrations.putIfAbsent(name, registration) != null) {
            throw new IllegalStateException("Duplicate @Workflow registration: '" + name + "'");
        }
    }

    /** All discovered workflows, in scan order. */
    public List<WorkflowRegistration> all() {
        return List.copyOf(registrations.values());
    }
}
