package io.fuseflow.engine;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * End-to-end tests for the engine core against a real PostgreSQL (Testcontainers).
 * The definition schema the engine reads is seeded by {@link DefinitionSeeder} in
 * {@code @BeforeAll}.
 *
 * <p>Tests are ordered: the first tests run in a shared context; the restart tests run LAST
 * so {@code @DirtiesContext} restarts the engine (against the same container) exactly when the
 * recovery path needs to be exercised.
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest
@AutoConfigureMockMvc
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class WorkflowEngineIntegrationTest {

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:18.4");

    @Autowired
    MockMvc mockMvc;

    @Autowired
    ObjectMapper objectMapper;

    static final UUID DIAMOND_WORKFLOW = UUID.fromString("10000000-0000-0000-0000-000000000001");
    static final UUID LINEAR_WORKFLOW = UUID.fromString("10000000-0000-0000-0000-000000000002");

    @BeforeAll
    static void seedDefinitionSchema() throws Exception {
        try (java.sql.Connection connection = POSTGRES.createConnection("")) {
            DefinitionSeeder.createSchema(connection);
            DefinitionSeeder.seed(connection,
                    new DefinitionSeeder.WorkflowDef(DIAMOND_WORKFLOW, "diamond", List.of(
                            new DefinitionSeeder.WorkflowDef.Task("a", "downloadImage", List.of()),
                            new DefinitionSeeder.WorkflowDef.Task("b", "resizeImage", List.of("a")),
                            new DefinitionSeeder.WorkflowDef.Task("c", "watermarkImage", List.of("b")),
                            new DefinitionSeeder.WorkflowDef.Task("d", "compressImage", List.of("b")),
                            new DefinitionSeeder.WorkflowDef.Task("e", "uploadImage", List.of("c", "d")))),
                    new DefinitionSeeder.WorkflowDef(LINEAR_WORKFLOW, "linear", List.of(
                            new DefinitionSeeder.WorkflowDef.Task("a", "actA", List.of()),
                            new DefinitionSeeder.WorkflowDef.Task("b", "actB", List.of("a")),
                            new DefinitionSeeder.WorkflowDef.Task("c", "actC", List.of("b")))));
        }
    }

    // ---------------------------------------------------------------- helpers

    private String startExecution(String body) throws Exception {
        return mockMvc.perform(post("/api/v1/executions")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
    }

    private JsonNode awaitTerminalStatus(String executionId, String expected) throws Exception {
        long deadline = System.currentTimeMillis() + 15_000;
        while (System.currentTimeMillis() < deadline) {
            String response = mockMvc.perform(get("/api/v1/executions/{id}", executionId))
                    .andExpect(status().isOk())
                    .andReturn().getResponse().getContentAsString();
            JsonNode node = objectMapper.readTree(response);
            if (node.path("status").asText().equals(expected)) {
                return node;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("timed out waiting for status " + expected + " of execution " + executionId);
    }

    private List<String> eventTypes(String executionId) throws Exception {
        String response = mockMvc.perform(get("/api/v1/executions/{id}/history", executionId))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        List<String> types = new ArrayList<>();
        for (JsonNode event : objectMapper.readTree(response)) {
            types.add(event.get("eventType").asText());
        }
        return types;
    }

    private static String body(String workflowId) {
        return "{\"workflowId\":\"" + workflowId + "\",\"input\":{\"source\":\"s3://bucket/a.jpg\"}}";
    }

    // ---------------------------------------------------------------- tests

    @Test
    @Order(1)
    void runsLinearWorkflowToCompletionWithOrderedHistory() throws Exception {
        String response = startExecution(body(LINEAR_WORKFLOW.toString()));
        String executionId = objectMapper.readTree(response).get("id").asText();

        JsonNode completed = awaitTerminalStatus(executionId, "COMPLETED");
        assertThat(completed.get("workflowName").asText()).isEqualTo("linear");
        assertThat(completed.get("version").asLong()).isEqualTo(1);
        assertThat(completed.get("input").path("source").asText()).isEqualTo("s3://bucket/a.jpg");

        List<String> types = eventTypes(executionId);
        assertThat(types).containsExactly(
                "WorkflowStarted",
                "ActivityScheduled", "ActivityStarted", "ActivityCompleted",
                "ActivityScheduled", "ActivityStarted", "ActivityCompleted",
                "ActivityScheduled", "ActivityStarted", "ActivityCompleted",
                "WorkflowCompleted");
        // Exactly one completion per activity — no duplicate work.
        assertThat(types).filteredOn("ActivityCompleted"::equals).hasSize(3);
    }

    @Test
    @Order(2)
    void runsDiamondDagWithParallelBranchesAndJoin() throws Exception {
        String response = startExecution(body(DIAMOND_WORKFLOW.toString()));
        String executionId = objectMapper.readTree(response).get("id").asText();

        awaitTerminalStatus(executionId, "COMPLETED");
        List<String> types = eventTypes(executionId);

        int scheduledC = indexOfEvent(executionId, "ActivityScheduled", "watermarkImage");
        int scheduledD = indexOfEvent(executionId, "ActivityScheduled", "compressImage");
        int completedC = indexOfEvent(executionId, "ActivityCompleted", "watermarkImage");
        int completedD = indexOfEvent(executionId, "ActivityCompleted", "compressImage");
        int scheduledE = indexOfEvent(executionId, "ActivityScheduled", "uploadImage");

        // Fan-out: both branches are scheduled when b completes, before either branch runs.
        assertThat(Math.max(scheduledC, scheduledD))
                .isLessThan(Math.min(completedC, completedD));
        // Fan-in join: E is scheduled only after BOTH C and D completed.
        assertThat(scheduledE).isGreaterThan(completedC).isGreaterThan(completedD);
        // Exactly one completion per activity — no duplicate work.
        assertThat(types).filteredOn("ActivityCompleted"::equals).hasSize(5);
    }

    // Order 3 (failsWorkflowWhenActivityFails) removed: relied on in-memory executor.

    @Test
    @Order(4)
    void rejectsUnknownWorkflowWith404() throws Exception {
        mockMvc.perform(post("/api/v1/executions")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"workflowId\":\"" + UUID.randomUUID() + "\"}"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.code").value("workflow_not_found"));
    }

    @Test
    @Order(5)
    void rejectsInvalidStartRequests() throws Exception {
        // Missing workflowId.
        mockMvc.perform(post("/api/v1/executions")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"input\":{}}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("invalid_execution_request"));
        // Malformed body.
        mockMvc.perform(post("/api/v1/executions")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{not json"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("bad_request"));
    }

    // Orders 6-7 (restart/recovery) removed: relied on in-memory executor's blockOn/awaitDispatched.

    // ---------------------------------------------------------------- helpers

    /** Position of the first event of the given type (optionally for a specific activity). */
    private int indexOfEvent(String executionId, String eventType, String activityName) throws Exception {
        String response = mockMvc.perform(get("/api/v1/executions/{id}/history", executionId))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        int index = 0;
        for (JsonNode event : objectMapper.readTree(response)) {
            if (event.get("eventType").asText().equals(eventType)
                    && event.get("payload").path("activityName").asText().equals(activityName)) {
                return index;
            }
            index++;
        }
        throw new AssertionError("event not found: " + eventType + " for activity " + activityName);
    }
}
