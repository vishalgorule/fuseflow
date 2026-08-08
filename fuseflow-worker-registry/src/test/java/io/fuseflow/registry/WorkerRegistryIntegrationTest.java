package io.fuseflow.registry;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * End-to-end tests for the Phase 3 worker registry API against a real PostgreSQL (Testcontainers):
 * register / re-register / heartbeat / list / capability lookup / deregister / validation, plus
 * worker metadata surviving a service restart. The first test is intentionally ordered first and
 * {@code @DirtiesContext}: the worker it registers must still be readable in a freshly booted
 * application context ({@link #workerSurvivesServiceRestart}).
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest
@AutoConfigureMockMvc
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class WorkerRegistryIntegrationTest {

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:18.4");

    @Autowired
    MockMvc mockMvc;

    @Autowired
    ObjectMapper objectMapper;

    static final UUID WORKER_ID = UUID.fromString("20000000-0000-0000-0000-000000000001");

    /** Worker id created by the first test, read back by the restart-survival test. */
    private static String persistedWorkerId;

    // ---------------------------------------------------------------- helpers

    private static String registerBody(String id, String host, Integer capacity, String activitiesJson) {
        String capacityJson = capacity == null ? "" : "\"capacity\": " + capacity + ",";
        return "{\"id\": \"" + id + "\", \"host\": \"" + host + "\", " + capacityJson
                + "\"activities\": " + activitiesJson + "}";
    }

    private String registerAndExtractId(String id, String host, String activitiesJson) throws Exception {
        String response = mockMvc.perform(post("/api/v1/workers")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(registerBody(id, host, null, activitiesJson)))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        return objectMapper.readTree(response).get("id").asText();
    }

    private static JsonNode findWorker(JsonNode list, String id) {
        for (JsonNode node : list) {
            if (node.get("id").asText().equals(id)) {
                return node;
            }
        }
        return null;
    }

    // ---------------------------------------------------------------- tests

    @Test
    @Order(1)
    @DirtiesContext
    void registersWorkerAndHeartbeats() throws Exception {
        String body = registerBody(WORKER_ID.toString(), "worker-1", 4, "[\"resizeImage\", \"uploadImage\"]");
        mockMvc.perform(post("/api/v1/workers").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.id").value(WORKER_ID.toString()))
                .andExpect(jsonPath("$.host").value("worker-1"))
                .andExpect(jsonPath("$.capacity").value(4))
                .andExpect(jsonPath("$.status").value("ONLINE"))
                .andExpect(jsonPath("$.activities.length()").value(2))
                .andExpect(jsonPath("$.activities[0]").value("resizeImage"))
                .andExpect(jsonPath("$.lastHeartbeatAt").isNotEmpty());
        persistedWorkerId = WORKER_ID.toString();

        // Heartbeat refreshes liveness and can update capacity.
        mockMvc.perform(post("/api/v1/workers/{id}/heartbeat", persistedWorkerId)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"capacity\": 2}"))
                .andExpect(status().isNoContent());

        mockMvc.perform(get("/api/v1/workers/{id}", persistedWorkerId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.capacity").value(2))
                .andExpect(jsonPath("$.status").value("ONLINE"))
                .andExpect(jsonPath("$.lastHeartbeatAt").isNotEmpty());
    }

    @Test
    @Order(2)
    void workerSurvivesServiceRestart() throws Exception {
        // Runs in a freshly booted application context (test 1 dirtied the old one)
        // against the same PostgreSQL container.
        mockMvc.perform(get("/api/v1/workers/{id}", persistedWorkerId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.host").value("worker-1"))
                .andExpect(jsonPath("$.activities.length()").value(2));
    }

    @Test
    @Order(3)
    void reRegisteringAnExistingWorkerUpdatesIt() throws Exception {
        // Robust version check: read the current version, then assert it incremented by exactly 1.
        String before = mockMvc.perform(get("/api/v1/workers/{id}", persistedWorkerId))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        long versionBefore = objectMapper.readTree(before).get("version").asLong();

        String body = registerBody(persistedWorkerId, "worker-2", 8, "[\"downloadImage\", \"resizeImage\"]");
        mockMvc.perform(post("/api/v1/workers").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isOk()) // 200, not 201: this is an update
                .andExpect(jsonPath("$.host").value("worker-2"))
                .andExpect(jsonPath("$.capacity").value(8))
                .andExpect(jsonPath("$.status").value("ONLINE"))
                .andExpect(jsonPath("$.version").value(versionBefore + 1))
                .andExpect(jsonPath("$.activities.length()").value(2))
                .andExpect(jsonPath("$.activities[0]").value("downloadImage"));
    }

    @Test
    @Order(4)
    void listsWorkersAndDefaultsCapacityToOne() throws Exception {
        UUID other = UUID.fromString("20000000-0000-0000-0000-000000000002");
        mockMvc.perform(post("/api/v1/workers").contentType(MediaType.APPLICATION_JSON)
                        .content(registerBody(other.toString(), "worker-3", null, "[\"compressImage\"]")))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.capacity").value(1)); // omitted capacity defaults to 1

        String response = mockMvc.perform(get("/api/v1/workers"))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        JsonNode list = objectMapper.readTree(response);
        assertThat(list).isNotEmpty();
        assertThat(findWorker(list, persistedWorkerId)).isNotNull();
        assertThat(findWorker(list, other.toString())).isNotNull();
    }

    @Test
    @Order(5)
    void capabilityLookupReturnsWorkersSupportingActivity() throws Exception {
        UUID a = UUID.fromString("20000000-0000-0000-0000-000000000003");
        UUID b = UUID.fromString("20000000-0000-0000-0000-000000000004");
        registerAndExtractId(a.toString(), "wa", "[\"resizeImage\", \"compressImage\"]");
        registerAndExtractId(b.toString(), "wb", "[\"resizeImage\", \"uploadImage\"]");

        // resizeImage: the persisted worker (re-registered in test 3) + wa + wb.
        String resize = mockMvc.perform(get("/api/v1/workers/activities/resizeImage"))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        assertThat(objectMapper.readTree(resize)).hasSize(3);

        // compressImage: worker-3 (test 4) + wa.
        String compress = mockMvc.perform(get("/api/v1/workers/activities/compressImage"))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        assertThat(objectMapper.readTree(compress)).hasSize(2);
        assertThat(objectMapper.readTree(compress)).anyMatch(node -> node.get("id").asText().equals(a.toString()));

        String unknown = mockMvc.perform(get("/api/v1/workers/activities/noSuchActivity"))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        assertThat(objectMapper.readTree(unknown)).isEmpty();
    }

    @Test
    @Order(6)
    void heartbeatUnknownWorkerReturns404() throws Exception {
        mockMvc.perform(post("/api/v1/workers/{id}/heartbeat", UUID.randomUUID()))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.code").value("worker_not_found"));
    }

    @Test
    @Order(7)
    void deregistersWorker() throws Exception {
        UUID doomed = UUID.fromString("20000000-0000-0000-0000-000000000005");
        registerAndExtractId(doomed.toString(), "doomed", "[\"actA\"]");

        mockMvc.perform(delete("/api/v1/workers/{id}", doomed)).andExpect(status().isNoContent());
        mockMvc.perform(get("/api/v1/workers/{id}", doomed)).andExpect(status().isNotFound());
        mockMvc.perform(delete("/api/v1/workers/{id}", doomed)).andExpect(status().isNotFound());
        mockMvc.perform(post("/api/v1/workers/{id}/heartbeat", doomed)).andExpect(status().isNotFound());
    }

    @Test
    @Order(8)
    void rejectsInvalidRegistrations() throws Exception {
        // Missing host.
        mockMvc.perform(post("/api/v1/workers").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"id\": \"" + UUID.randomUUID() + "\", \"activities\": [\"actA\"]}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("invalid_worker_request"))
                .andExpect(jsonPath("$.errors[0].field").value("host"));
        // Empty activity list.
        mockMvc.perform(post("/api/v1/workers").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"id\": \"" + UUID.randomUUID() + "\", \"host\": \"h\", \"activities\": []}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[0].field").value("activities"));
        // Capacity below 1.
        mockMvc.perform(post("/api/v1/workers").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"id\": \"" + UUID.randomUUID() + "\", \"host\": \"h\", \"capacity\": 0, \"activities\": [\"actA\"]}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[0].field").value("capacity"));
        // Malformed body.
        mockMvc.perform(post("/api/v1/workers").contentType(MediaType.APPLICATION_JSON).content("{not json"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("bad_request"));
    }

    @Test
    @Order(9)
    void rejectsInvalidHeartbeatAndInvalidId() throws Exception {
        mockMvc.perform(post("/api/v1/workers/{id}/heartbeat", persistedWorkerId)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"capacity\": 0}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("invalid_heartbeat"));

        mockMvc.perform(get("/api/v1/workers/not-a-uuid"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("bad_request"));
    }
}
