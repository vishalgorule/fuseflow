package io.fuseflow.registry;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.util.UUID;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * End-to-end tests for the Phase 3 offline detector against a real PostgreSQL, with short
 * heartbeat windows (degraded after 1s, offline after 2s, detector every 500ms): a worker
 * that stops heartbeating flips DEGRADED → OFFLINE, a heartbeat revives it, long-offline
 * workers are removed after the grace period, and the heartbeat log is purged past its
 * retention window.
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(properties = {
        "fuseflow.registry.detection.interval=500ms",
        "fuseflow.registry.heartbeat.degraded-after=1s",
        "fuseflow.registry.heartbeat.timeout=2s",
        "fuseflow.registry.heartbeat.retention=1s",
        "fuseflow.registry.heartbeat.offline-removal-after=5s",
        // No Kafka broker in this suite — worker-events publishing is off (Phase 4).
        "fuseflow.registry.events-enabled=false"
})
@AutoConfigureMockMvc
class OfflineDetectionIntegrationTest {

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:18.4");

    @Autowired
    MockMvc mockMvc;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    JdbcClient jdbcClient;

    // ---------------------------------------------------------------- helpers

    private String register(String host, String activitiesJson) throws Exception {
        String body = "{\"id\": \"" + UUID.randomUUID() + "\", \"host\": \"" + host
                + "\", \"activities\": " + activitiesJson + "}";
        String response = mockMvc.perform(post("/api/v1/workers")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        return objectMapper.readTree(response).get("id").asText();
    }

    private JsonNode getWorker(String id) throws Exception {
        String response = mockMvc.perform(get("/api/v1/workers/{id}", id))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        return objectMapper.readTree(response);
    }

    private void awaitStatusIn(String id, long timeoutMillis, String... expected) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            String status = getWorker(id).get("status").asText();
            for (String candidate : expected) {
                if (status.equals(candidate)) {
                    return;
                }
            }
            Thread.sleep(100);
        }
        throw new AssertionError("worker " + id + " never reached " + String.join("/", expected));
    }

    private void awaitDeleted(String id, long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            int status = mockMvc.perform(get("/api/v1/workers/{id}", id))
                    .andReturn().getResponse().getStatus();
            if (status == 404) {
                return;
            }
            Thread.sleep(200);
        }
        throw new AssertionError("worker " + id + " was not removed within " + timeoutMillis + "ms");
    }

    private void awaitHeartbeatCount(String id, int expected, long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            Integer count = jdbcClient.sql("SELECT COUNT(*) FROM registry.worker_heartbeat WHERE worker_id = :id")
                    .param("id", UUID.fromString(id))
                    .query((rs, rowNum) -> rs.getInt(1))
                    .optional().orElse(0);
            if (count == expected) {
                return;
            }
            Thread.sleep(200);
        }
        throw new AssertionError("heartbeat count for " + id + " never reached " + expected);
    }

    // ---------------------------------------------------------------- tests

    @Test
    void workerGoesDegradedThenOfflineAndHeartbeatRevives() throws Exception {
        String id = register("flaky", "[\"actA\"]");

        // No heartbeats: DEGRADED after ~1s (may already be OFFLINE by the first poll).
        awaitStatusIn(id, 6_000, "DEGRADED", "OFFLINE");
        // OFFLINE after the 2s heartbeat timeout.
        awaitStatusIn(id, 6_000, "OFFLINE");

        // A heartbeat revives the worker immediately.
        mockMvc.perform(post("/api/v1/workers/{id}/heartbeat", id))
                .andExpect(status().isNoContent());
        awaitStatusIn(id, 6_000, "ONLINE");
    }

    @Test
    void offlineWorkerIsRemovedAfterGracePeriod() throws Exception {
        String id = register("doomed", "[\"actA\"]");

        // Goes OFFLINE after ~2s; removed 5s after being marked OFFLINE (~7s total).
        awaitStatusIn(id, 6_000, "OFFLINE");
        awaitDeleted(id, 15_000);
    }

    @Test
    void staleHeartbeatsArePurgedWhileWorkerStaysOnline() throws Exception {
        String id = register("chatty", "[\"actA\"]");

        // Heartbeat faster than the detector so the worker never degrades or goes offline.
        long heartbeatDeadline = System.currentTimeMillis() + 3_500;
        while (System.currentTimeMillis() < heartbeatDeadline) {
            mockMvc.perform(post("/api/v1/workers/{id}/heartbeat", id))
                    .andExpect(status().isNoContent());
            Thread.sleep(200);
        }

        // Past the retention window (1s) + a couple of detector runs, all rows are purged...
        awaitHeartbeatCount(id, 0, 8_000);

        // ...while the worker itself is untouched by the heartbeat purge.
        getWorker(id);
    }
}
