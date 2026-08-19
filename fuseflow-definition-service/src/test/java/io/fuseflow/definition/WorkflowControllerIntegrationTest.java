package io.fuseflow.definition;

import tools.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.http.MediaType;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * End-to-end CRUD and validation tests for the workflow definition API against a
 * real PostgreSQL (Testcontainers). The first two tests are intentionally ordered:
 * the workflow registered by {@link #registersReadsAndListsWorkflow} must still be
 * readable in a freshly booted application context ({@link #workflowSurvivesServiceRestart}).
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest
@AutoConfigureMockMvc
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class WorkflowControllerIntegrationTest {

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:18.4");

    @Autowired
    MockMvc mockMvc;

    @Autowired
    ObjectMapper objectMapper;

    /** Workflow id created by the first test, read back by the restart-survival test. */
    private static String persistedWorkflowId;

    // ---------------------------------------------------------------- helpers

    private String register(String name, String tasksJson) throws Exception {
        String body = """
                {"name": "%s", "description": "it-desc", "tasks": %s}
                """.formatted(name, tasksJson);
        return postAndExtractId(body);
    }

    private String postAndExtractId(String body) throws Exception {
        String response = mockMvc.perform(post("/api/v1/workflows")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        return objectMapper.readTree(response).get("id").asText();
    }

    // ---------------------------------------------------------------- tests

    @Test
    @Order(1)
    @DirtiesContext
    void registersReadsAndListsWorkflow() throws Exception {
        String body = """
                {
                  "name": "image-processing",
                  "description": "Diamond DAG",
                  "tasks": [
                    {"id": "download", "activity": "downloadImage"},
                    {"id": "resize", "activity": "resizeImage", "dependsOn": ["download"]},
                    {"id": "upload", "activity": "uploadImage", "dependsOn": ["resize"]}
                  ]
                }
                """;
        String response = mockMvc.perform(post("/api/v1/workflows")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.name").value("image-processing"))
                .andExpect(jsonPath("$.version").value(0))
                .andExpect(jsonPath("$.tasks.length()").value(3))
                .andExpect(jsonPath("$.tasks[1].dependsOn[0]").value("download"))
                .andReturn().getResponse().getContentAsString();

        persistedWorkflowId = objectMapper.readTree(response).get("id").asText();

        mockMvc.perform(get("/api/v1/workflows/{id}", persistedWorkflowId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.name").value("image-processing"));

        mockMvc.perform(get("/api/v1/workflows"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(1))
                .andExpect(jsonPath("$[0].name").value("image-processing"));
    }

    @Test
    @Order(2)
    void workflowSurvivesServiceRestart() throws Exception {
        // Runs in a freshly booted application context (test 1 dirtied the old one)
        // against the same PostgreSQL container.
        mockMvc.perform(get("/api/v1/workflows/{id}", persistedWorkflowId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.name").value("image-processing"))
                .andExpect(jsonPath("$.tasks.length()").value(3));
    }

    @Test
    @Order(3)
    void rejectsCircularDependencyWithFieldError() throws Exception {
        String body = """
                {
                  "name": "cyclic",
                  "tasks": [
                    {"id": "a", "activity": "actA", "dependsOn": ["b"]},
                    {"id": "b", "activity": "actB", "dependsOn": ["a"]}
                  ]
                }
                """;
        mockMvc.perform(post("/api/v1/workflows").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("invalid_workflow"))
                .andExpect(jsonPath("$.errors[0].field").value("tasks"))
                .andExpect(jsonPath("$.errors[0].message").value(containsString("circular dependency")));
    }

    @Test
    @Order(4)
    void rejectsDuplicateTaskIds() throws Exception {
        String body = """
                {
                  "name": "duplicate-tasks",
                  "tasks": [
                    {"id": "a", "activity": "actA"},
                    {"id": "a", "activity": "actB"}
                  ]
                }
                """;
        mockMvc.perform(post("/api/v1/workflows").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("invalid_workflow"))
                .andExpect(jsonPath("$.errors[0].field").value("tasks[1].id"));
    }

    @Test
    @Order(5)
    void rejectsDanglingDependency() throws Exception {
        String body = """
                {
                  "name": "dangling",
                  "tasks": [
                    {"id": "a", "activity": "actA", "dependsOn": ["ghost"]}
                  ]
                }
                """;
        mockMvc.perform(post("/api/v1/workflows").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[0].field").value("tasks[0].dependsOn[0]"))
                .andExpect(jsonPath("$.errors[0].message").value(containsString("dependency 'ghost' is not defined")));
    }

    @Test
    @Order(6)
    void rejectsDuplicateNameWithConflict() throws Exception {
        register("duplicate-name", """
                [{"id": "a", "activity": "actA"}]
                """);
        mockMvc.perform(post("/api/v1/workflows")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"name": "duplicate-name", "tasks": [{"id": "b", "activity": "actB"}]}
                                """))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.code").value("workflow_name_conflict"));
    }

    @Test
    @Order(7)
    void rejectsMissingName() throws Exception {
        String body = """
                {"tasks": [{"id": "a", "activity": "actA"}]}
                """;
        mockMvc.perform(post("/api/v1/workflows").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[0].field").value("name"));
    }

    @Test
    @Order(8)
    void putOnExistingVersionIsRejectedAsImmutable() throws Exception {
        // Phase 8: definitions are immutable version snapshots — PUT (mutating a snapshot)
        // is always rejected with 409; changing a DAG means registering a new version.
        String id = register("updatable", """
                [{"id": "old", "activity": "oldActivity"}]
                """);

        String body = """
                {
                  "name": "updatable",
                  "tasks": [
                    {"id": "new", "activity": "newActivity", "dependsOn": ["dep"]},
                    {"id": "dep", "activity": "depActivity"}
                  ]
                }
                """;
        mockMvc.perform(put("/api/v1/workflows/{id}", id).contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.code").value("workflow_version_immutable"));

        // The original snapshot is untouched.
        mockMvc.perform(get("/api/v1/workflows/{id}", id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.version").value(0))
                .andExpect(jsonPath("$.tasks.length()").value(1));
    }

    @Test
    @Order(9)
    void updateUnknownWorkflowReturns404() throws Exception {
        mockMvc.perform(put("/api/v1/workflows/{id}", UUID.randomUUID())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"name": "x", "tasks": [{"id": "a", "activity": "actA"}]}
                                """))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.code").value("workflow_not_found"));
    }

    @Test
    @Order(10)
    void registersMultipleVersionsOfTheSameWorkflow() throws Exception {
        // Phase 8: (name, semanticVersion) is the unique key — registering a different DAG
        // under a NEW version succeeds and the versions coexist.
        String v1 = register("versioned", """
                [{"id": "a", "activity": "actA"}]
                """);

        String v2 = postAndExtractId("""
                {"name": "versioned", "semanticVersion": "2",
                 "tasks": [{"id": "b", "activity": "actB"}, {"id": "c", "activity": "actC", "dependsOn": ["b"]}]}
                """);

        assertThat(v2).isNotEqualTo(v1);
        // The same (name, version) pair cannot be registered twice.
        mockMvc.perform(post("/api/v1/workflows")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"name": "versioned", "semanticVersion": "2", "tasks": [{"id": "x", "activity": "actX"}]}
                                """))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.code").value("workflow_name_conflict"));

        // ?name=X returns every version (newest first); ?name=X&version=Y pins one.
        mockMvc.perform(get("/api/v1/workflows").param("name", "versioned"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(2));
        mockMvc.perform(get("/api/v1/workflows").param("name", "versioned").param("version", "2"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(1))
                .andExpect(jsonPath("$[0].semanticVersion").value("2"))
                .andExpect(jsonPath("$[0].tasks.length()").value(2));
        mockMvc.perform(get("/api/v1/workflows").param("name", "versioned").param("version", "9"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(0));
    }

    @Test
    @Order(11)
    void deletesWorkflow() throws Exception {
        String id = register("to-delete", """
                [{"id": "a", "activity": "actA"}]
                """);

        mockMvc.perform(delete("/api/v1/workflows/{id}", id)).andExpect(status().isNoContent());
        mockMvc.perform(get("/api/v1/workflows/{id}", id)).andExpect(status().isNotFound());
        mockMvc.perform(delete("/api/v1/workflows/{id}", id)).andExpect(status().isNotFound());
    }

    @Test
    @Order(12)
    void malformedBodyReturns400() throws Exception {
        mockMvc.perform(post("/api/v1/workflows")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{not json"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("bad_request"));
    }

    @Test
    @Order(13)
    void invalidWorkflowIdReturns400() throws Exception {
        mockMvc.perform(get("/api/v1/workflows/not-a-uuid"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("bad_request"));
    }

    @Test
    @Order(14)
    void persistsWorkflowAndTaskRetryPolicies() throws Exception {
        // Phase 7: retry policies round-trip at workflow and task level and survive restart.
        String body = """
                {
                  "name": "retry-policy",
                  "description": "retries",
                  "retryPolicy": {"maxAttempts": 4, "fixedDelaySeconds": 3, "exponentialBackoff": true, "backoffMultiplier": 2.0},
                  "tasks": [
                    {"id": "a", "activity": "actA", "retryPolicy": {"maxAttempts": 1}},
                    {"id": "b", "activity": "actB"}
                  ]
                }
                """;
        String id = postAndExtractId(body);
        mockMvc.perform(get("/api/v1/workflows/{id}", id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.retryPolicy.maxAttempts").value(4))
                .andExpect(jsonPath("$.retryPolicy.fixedDelaySeconds").value(3))
                .andExpect(jsonPath("$.retryPolicy.exponentialBackoff").value(true))
                .andExpect(jsonPath("$.tasks[0].retryPolicy.maxAttempts").value(1))
                .andExpect(jsonPath("$.tasks[1].retryPolicy").doesNotExist());
    }

    @Test
    @Order(15)
    void rejectsInvalidRetryPolicy() throws Exception {
        mockMvc.perform(post("/api/v1/workflows")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"name": "bad-policy", "retryPolicy": {"maxAttempts": 0},
                                 "tasks": [{"id": "a", "activity": "actA"}]}
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("invalid_workflow"))
                .andExpect(jsonPath("$.errors[0].field").value("retryPolicy.maxAttempts"));
    }

    @Test
    @Order(16)
    void looksUpByName() throws Exception {
        // Phase 6: ?name=X lookup backs the SDK's idempotent upsert. The image-processing
        // workflow registered by test 1 is still present (names are unique → 1 result).
        mockMvc.perform(get("/api/v1/workflows").param("name", "image-processing"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(1))
                .andExpect(jsonPath("$[0].id").value(persistedWorkflowId))
                .andExpect(jsonPath("$[0].tasks.length()").value(3));

        mockMvc.perform(get("/api/v1/workflows").param("name", "does-not-exist"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(0));
    }
}
