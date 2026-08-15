package io.fuseflow.sdk.config;

import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.StandardEnvironment;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class FuseFlowDefaultsEnvironmentPostProcessorTest {

    private final FuseFlowDefaultsEnvironmentPostProcessor processor =
            new FuseFlowDefaultsEnvironmentPostProcessor();

    private StandardEnvironment environment() {
        StandardEnvironment environment = new StandardEnvironment();
        processor.postProcessEnvironment(environment, mock(SpringApplication.class));
        return environment;
    }

    @Test
    void providesKafkaTransportDefaults() {
        StandardEnvironment environment = environment();

        assertThat(environment.getProperty("spring.kafka.producer.key-serializer"))
                .isEqualTo("org.apache.kafka.common.serialization.StringSerializer");
        assertThat(environment.getProperty("spring.kafka.producer.value-serializer"))
                .isEqualTo("org.apache.kafka.common.serialization.StringSerializer");
        assertThat(environment.getProperty("spring.kafka.consumer.key-deserializer"))
                .isEqualTo("org.apache.kafka.common.serialization.StringDeserializer");
        assertThat(environment.getProperty("spring.kafka.consumer.value-deserializer"))
                .isEqualTo("org.apache.kafka.common.serialization.StringDeserializer");
        assertThat(environment.getProperty("spring.kafka.consumer.auto-offset-reset")).isEqualTo("earliest");
        assertThat(environment.getProperty("fuseflow.queue.activity-results")).isEqualTo("activity-results");
        assertThat(environment.getProperty("fuseflow.queue.pool-prefix")).isEqualTo("fuseflow-pool");
        assertThat(environment.getProperty("spring.kafka.bootstrap-servers")).isEqualTo("localhost:9092");
    }

    @Test
    void addsBrokerDiscoverySourceOnlyWhenWorkerEnabled() {
        StandardEnvironment environment = new StandardEnvironment();
        environment.getPropertySources().addFirst(new MapPropertySource("user-config",
                Map.of("fuseflow.worker.enabled", "true",
                        "fuseflow.registry.base-url", "http://registry:8083")));
        processor.postProcessEnvironment(environment, mock(SpringApplication.class));

        // The lazy broker source is registered when the worker is enabled. Reading the value is
        // deliberately not asserted here: resolution blocks (retrying at 5s, no fallback) until
        // the registry answers — see BrokerConfigResolverTest for that behavior.
        assertThat(environment.getPropertySources().contains("fuseflow-sdk-broker")).isTrue();
    }

    @Test
    void skipsBrokerDiscoverySourceWhenWorkerDisabled() {
        StandardEnvironment environment = environment();

        assertThat(environment.getPropertySources().contains("fuseflow-sdk-broker")).isFalse();
        assertThat(environment.getProperty("spring.kafka.bootstrap-servers")).isEqualTo("localhost:9092");
    }

    @Test
    void userConfigurationWinsOverDefaults() {
        StandardEnvironment environment = new StandardEnvironment();
        environment.getPropertySources().addFirst(new MapPropertySource("user-config",
                Map.of("spring.kafka.bootstrap-servers", "broker:9093")));
        processor.postProcessEnvironment(environment, mock(SpringApplication.class));

        assertThat(environment.getProperty("spring.kafka.bootstrap-servers")).isEqualTo("broker:9093");
    }
}
