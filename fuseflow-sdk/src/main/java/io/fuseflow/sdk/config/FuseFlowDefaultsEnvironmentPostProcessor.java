package io.fuseflow.sdk.config;

import org.springframework.boot.EnvironmentPostProcessor;
import org.springframework.boot.SpringApplication;
import org.springframework.core.Ordered;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.PropertySource;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * SDK-internal defaults for the Kafka transport (registered via
 * {@code META-INF/spring.factories} under {@code org.springframework.boot.EnvironmentPostProcessor}).
 *
 * <p>The worker package is configured with a <b>single endpoint</b> — the registry
 * ({@code fuseflow.registry.base-url}). Everything else is owned by the SDK:
 * <ul>
 *   <li>transport internals (String serializers, offset reset, internal queue names) are
 *       hardcoded below with no user surface, and</li>
 *   <li>the worker's pool dispatch queue is derived as
 *       {@code fuseflow.queue.pool-prefix}<b>.</b>{@code fuseflow.worker.pool} — one knob
 *       drives routing key, queue and consumer group, so they can never diverge, and</li>
 *   <li>the broker address is resolved lazily from the registry's {@code GET /api/v1/config}
 *       (see {@link BrokerConfigResolver}) the first time the transport asks for it — with
 *       <b>no fallback</b>: discovery retries at a fixed interval until the registry answers,
 *       because a worker is not functional without the registry (it must register there anyway).</li>
 * </ul>
 * All defaults are the lowest-precedence property sources, so user configuration always wins;
 * the lazy broker source sits just above the static defaults and only engages when the worker
 * runtime is enabled (mirroring {@link FuseFlowSdkAutoConfiguration}).
 */
public class FuseFlowDefaultsEnvironmentPostProcessor implements EnvironmentPostProcessor, Ordered {

    private static final String DEFAULTS_SOURCE = "fuseflow-sdk-defaults";
    private static final String BROKER_SOURCE = "fuseflow-sdk-broker";

    private static final String STRING_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    private static final String STRING_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";

    @Override
    public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
        Map<String, Object> defaults = new LinkedHashMap<>();
        defaults.put("spring.kafka.bootstrap-servers", "localhost:9092");
        defaults.put("spring.kafka.producer.key-serializer", STRING_SERIALIZER);
        defaults.put("spring.kafka.producer.value-serializer", STRING_SERIALIZER);
        defaults.put("spring.kafka.producer.properties.max.block.ms", "5000");
        defaults.put("spring.kafka.consumer.key-deserializer", STRING_DESERIALIZER);
        defaults.put("spring.kafka.consumer.value-deserializer", STRING_DESERIALIZER);
        defaults.put("spring.kafka.consumer.auto-offset-reset", "earliest");
        // Post-Phase 7 hardening: the pool listener runs inside a container-managed Kafka
        // transaction (the COMPLETED/FAILED result and the offset commit are atomic — a worker
        // crash mid-execution leaves the offset uncommitted, so the task is redelivered instead
        // of stalling until the execution timeout). Spring Kafka makes transactional.id =
        // <prefix> + n per producer, so the prefix MUST be unique per application instance — a
        // random suffix per JVM keeps fleet workers from fencing each other; users overriding
        // the property must keep it unique per instance too.
        defaults.put("spring.kafka.producer.transaction-id-prefix",
                "fuseflow-worker-" + UUID.randomUUID());
        defaults.put("fuseflow.queue.activity-results", "activity-results");
        // Pool dispatch queue prefix — the worker's queue is derived as
        // <pool-prefix>.<pool>; the engine ships the same default.
        defaults.put("fuseflow.queue.pool-prefix", "fuseflow-pool");
        environment.getPropertySources().addLast(new MapPropertySource(DEFAULTS_SOURCE, defaults));

        // Broker discovery only for worker runtimes; the registry is the single endpoint the
        // user configures. addBefore(DEFAULTS_SOURCE) lets the discovered broker override the
        // static localhost default while user config still wins over both.
        if (environment.getProperty("fuseflow.worker.enabled", Boolean.class, false)) {
            String registryBaseUrl = environment.getProperty("fuseflow.registry.base-url", "http://localhost:8083");
            environment.getPropertySources().addBefore(DEFAULTS_SOURCE, new LazyBrokerPropertySource(registryBaseUrl));
        }
    }

    @Override
    public int getOrder() {
        return Ordered.LOWEST_PRECEDENCE;
    }

    /** Resolves {@code spring.kafka.bootstrap-servers} from the registry once, then caches it. */
    private static final class LazyBrokerPropertySource extends PropertySource<String> {

        private final BrokerConfigResolver resolver = new BrokerConfigResolver();
        private final String registryBaseUrl;
        private volatile String cached;

        LazyBrokerPropertySource(String registryBaseUrl) {
            super(BROKER_SOURCE);
            this.registryBaseUrl = registryBaseUrl;
        }

        @Override
        public Object getProperty(String name) {
            if (!"spring.kafka.bootstrap-servers".equals(name)) {
                return null;
            }
            String value = cached;
            if (value == null) {
                value = resolver.resolve(registryBaseUrl);
                cached = value;
            }
            return value;
        }
    }
}
