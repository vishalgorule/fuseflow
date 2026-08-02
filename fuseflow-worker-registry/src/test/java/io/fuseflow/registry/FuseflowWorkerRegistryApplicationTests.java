package io.fuseflow.registry;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Verifies the application context boots and Flyway migrates the
 * {@code registry} schema against a real PostgreSQL (Testcontainers).
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest
class FuseflowWorkerRegistryApplicationTests {

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:18.4");

    @Test
    void contextLoads() {
    }
}
