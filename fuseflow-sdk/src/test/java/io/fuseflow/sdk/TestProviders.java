package io.fuseflow.sdk;

import org.springframework.beans.factory.ObjectProvider;

/** Test helper: an {@link ObjectProvider} that never yields a bean (tracing/metrics absent). */
public final class TestProviders {

    private TestProviders() {
    }

    public static <T> ObjectProvider<T> emptyProvider() {
        return new ObjectProvider<>() {
            @Override
            public T getObject() {
                return null;
            }

            @Override
            public T getObject(Object... args) {
                return null;
            }
        };
    }
}
