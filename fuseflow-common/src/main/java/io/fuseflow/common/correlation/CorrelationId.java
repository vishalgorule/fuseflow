package io.fuseflow.common.correlation;

import java.util.UUID;

/**
 * Correlation-ID holder. The id is generated at the edge (or read from the
 * {@code X-Correlation-Id} header), stored in a {@link ThreadLocal} and in the
 * SLF4J MDC so every log line in the request scope carries it.
 */
public final class CorrelationId {

    public static final String HEADER = "X-Correlation-Id";
    public static final String MDC_KEY = "correlationId";

    private static final ThreadLocal<String> CURRENT = new ThreadLocal<>();

    private CorrelationId() {
    }

    /** Returns the current correlation id, generating one if absent. */
    public static String getOrCreate() {
        String id = CURRENT.get();
        if (id == null || id.isBlank()) {
            id = UUID.randomUUID().toString();
            CURRENT.set(id);
        }
        return id;
    }

    public static String get() {
        return CURRENT.get();
    }

    public static void set(String id) {
        CURRENT.set(id);
    }

    public static void clear() {
        CURRENT.remove();
    }
}
