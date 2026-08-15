package io.fuseflow.sample;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.env.Environment;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the sample workers application context boots and that the SDK's Kafka transport
 * defaults (shipped via the SDK's {@code EnvironmentPostProcessor}) are visible in the real
 * application environment — the worker package declares no {@code spring.kafka.*} config and no
 * broker address (that comes from registry discovery at runtime). The worker runtime is disabled
 * so the test needs no Kafka or registry; the full stack is exercised via the manual demo
 * (README) and the SDK's own unit tests.
 */
@SpringBootTest(properties = "fuseflow.worker.enabled=false")
class SampleWorkersApplicationTests {

    @Autowired
    Environment environment;

    @Test
    void contextLoads() {
    }

    @Test
    void sdkProvidesKafkaTransportDefaults() {
        assertThat(environment.getProperty("spring.kafka.producer.key-serializer"))
                .isEqualTo("org.apache.kafka.common.serialization.StringSerializer");
        assertThat(environment.getProperty("spring.kafka.producer.value-serializer"))
                .isEqualTo("org.apache.kafka.common.serialization.StringSerializer");
        assertThat(environment.getProperty("spring.kafka.consumer.auto-offset-reset")).isEqualTo("earliest");
        assertThat(environment.getProperty("fuseflow.queue.activity-results")).isEqualTo("activity-results");
        // Static fallback stands; with the worker disabled no registry discovery is attempted.
        assertThat(environment.getProperty("spring.kafka.bootstrap-servers")).isEqualTo("localhost:9092");
    }
}
