package io.fuseflow.sdk.runtime;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The workflows discovered by the {@link WorkflowScanner} at startup (Phase 6), keyed by
 * (name, semanticVersion) — two versions of the same workflow may legitimately coexist
 * (Phase 8). Consumed by the {@link WorkflowRegistrar} to register each definition at the
 * definition service. Mirrors the {@link ActivityRegistry} pattern: the scanner fills it,
 * the runtime consumes it.
 */
public class WorkflowRegistry {

    private final Map<String, WorkflowRegistration> registrations = new LinkedHashMap<>();

    /** Registers a scanned workflow; fails fast on duplicate (name, version) within one project. */
    public void register(WorkflowRegistration registration) {
        String name = registration.name();
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("@Workflow name must not be blank");
        }
        String version = registration.request().semanticVersion() == null
                || registration.request().semanticVersion().isBlank()
                ? "1" : registration.request().semanticVersion();
        String key = name + "\u0000" + version;
        if (registrations.putIfAbsent(key, registration) != null) {
            throw new IllegalStateException("Duplicate @Workflow registration: '" + name
                    + "' version '" + version + "'");
        }
    }

    /** All discovered workflows, in scan order. */
    public List<WorkflowRegistration> all() {
        return List.copyOf(registrations.values());
    }
}
