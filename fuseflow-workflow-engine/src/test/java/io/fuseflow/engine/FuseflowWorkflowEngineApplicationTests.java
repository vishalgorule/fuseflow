package io.fuseflow.engine;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Verifies the application context boots and Flyway migrates the
 * {@code engine} schema against a real PostgreSQL, with Kafka connectivity
 * wired via Testcontainers.
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest
class FuseflowWorkflowEngineApplicationTests {

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:18.4");

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer();

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @Test
    void contextLoads() {
    }
}
