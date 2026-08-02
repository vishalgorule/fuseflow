package io.fuseflow.definition;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Verifies the application context boots and Flyway migrates the
 * {@code definition} schema against a real PostgreSQL (Testcontainers).
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest
class FuseflowDefinitionServiceApplicationTests {

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:18.4");

    @Test
    void contextLoads() {
    }
}
