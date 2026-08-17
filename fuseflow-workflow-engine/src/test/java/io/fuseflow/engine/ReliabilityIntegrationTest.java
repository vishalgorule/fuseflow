package io.fuseflow.engine;

import io.fuseflow.common.dto.RetryPolicy;
import io.fuseflow.engine.dispatch.ActivityExecutor;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.http.MediaType;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
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
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Phase 7 end-to-end reliability tests (FR-6/FR-7): retries, timeouts and dead-lettering,
 * against real PostgreSQL with the deterministic in-memory {@link TestActivityExecutor}
 * (no Kafka — the retry/timeout machinery is DB-driven; the Kafka dead-letter publisher is
 * covered by its unit test). Short retry delays and poll intervals keep the suite fast.
 *
 * <p>Ordered + {@code @DirtiesContext} on the timeout test: that test leaves blocked executor
 * threads behind, so the context is discarded afterwards.
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest
@AutoConfigureMockMvc
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestPropertySource(properties = {
        "fuseflow.engine.dispatch-mode=in-memory",
        "fuseflow.engine.poll-interval=200ms",
        "fuseflow.engine.timeout.start=1s",
        "fuseflow.engine.timeout.execution=2s"
})
class ReliabilityIntegrationTest {

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:18.4");

    @Autowired
    MockMvc mockMvc;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    TestActivityExecutor activityExecutor;

    static final UUID FLAKY_WORKFLOW = UUID.fromString("20000000-0000-0000-0000-000000000001");
    static final UUID NON_RETRYABLE_WORKFLOW = UUID.fromString("20000000-0000-0000-0000-000000000002");
    static final UUID TIMEOUT_WORKFLOW = UUID.fromString("20000000-0000-0000-0000-000000000003");

    @TestConfiguration
    static class TestConfig {

        @Bean
        @Primary
        ActivityExecutor testActivityExecutor() {
            return new TestActivityExecutor();
        }
    }

    @BeforeAll
    static void seedDefinitionSchema() throws Exception {
        try (java.sql.Connection connection = POSTGRES.createConnection("")) {
            DefinitionSeeder.createSchema(connection);
            DefinitionSeeder.seed(connection,
                    // Task b fails once (flaky), then succeeds — the retry must re-dispatch it.
                    new DefinitionSeeder.WorkflowDef(FLAKY_WORKFLOW, "flaky", List.of(
                            new DefinitionSeeder.WorkflowDef.Task("a", "actA", List.of()),
                            new DefinitionSeeder.WorkflowDef.Task("b", "actB", List.of("a"),
                                    new RetryPolicy(3, 1, false, null, null)),
                            new DefinitionSeeder.WorkflowDef.Task("c", "actC", List.of("b"))),
                            null),
                    // Task b fails with a non-retryable error type — must fail immediately, no retries.
                    new DefinitionSeeder.WorkflowDef(NON_RETRYABLE_WORKFLOW, "non-retryable", List.of(
                            new DefinitionSeeder.WorkflowDef.Task("a", "actA", List.of()),
                            new DefinitionSeeder.WorkflowDef.Task("b", "actB", List.of("a"))),
                            new RetryPolicy(null, null, null, null,
                                    List.of("java.lang.IllegalStateException"))),
                    // Task b never completes — the execution timeout must convert it into a failed
                    // attempt, which is retried, then fails terminally when retries run out.
                    new DefinitionSeeder.WorkflowDef(TIMEOUT_WORKFLOW, "timeout", List.of(
                            new DefinitionSeeder.WorkflowDef.Task("a", "actA", List.of()),
                            new DefinitionSeeder.WorkflowDef.Task("b", "actB", List.of("a"),
                                    new RetryPolicy(2, 1, false, null, null)),
                            new DefinitionSeeder.WorkflowDef.Task("c", "actC", List.of("b"))),
                            null));
        }
    }

    @BeforeEach
    void resetExecutor() {
        activityExecutor.reset();
    }

    // ---------------------------------------------------------------- tests

    @Test
    @Order(1)
    void retriesFlakyActivityAndCompletes() throws Exception {
        activityExecutor.failNext("b", 1); // first attempt fails, retry succeeds

        String executionId = start(FLAKY_WORKFLOW);

        JsonNode completed = awaitTerminalStatus(executionId, "COMPLETED");
        assertThat(completed.get("status").asText()).isEqualTo("COMPLETED");

        List<String> types = eventTypes(executionId);
        // The failed attempt was retried, then the activity completed exactly once.
        assertThat(types).contains("ActivityRetryScheduled");
        assertThat(types).filteredOn("ActivityStarted"::equals).hasSize(4); // a, b(2 attempts), c
        assertThat(types).filteredOn("ActivityCompleted"::equals).hasSize(3);
        assertThat(types).filteredOn("ActivityFailed"::equals).hasSize(0);
        assertThat(types).filteredOn("WorkflowCompleted"::equals).hasSize(1);
    }

    @Test
    @Order(2)
    void failsImmediatelyForNonRetryableErrorType() throws Exception {
        activityExecutor.failWithType("b", "java.lang.IllegalStateException");

        String executionId = start(NON_RETRYABLE_WORKFLOW);

        JsonNode failed = awaitTerminalStatus(executionId, "FAILED");
        assertThat(failed.get("status").asText()).isEqualTo("FAILED");

        List<String> types = eventTypes(executionId);
        // No retry scheduled — the non-retryable classification fails the activity at once.
        assertThat(types).doesNotContain("ActivityRetryScheduled");
        assertThat(types).filteredOn("ActivityStarted"::equals).hasSize(2); // a, b — b once only
        assertThat(types).filteredOn("ActivityFailed"::equals).hasSize(1);
        assertThat(types).filteredOn("WorkflowFailed"::equals).hasSize(1);
    }

    @Test
    @Order(3)
    void failsTerminallyAfterRetriesExhausted() throws Exception {
        activityExecutor.fail("b"); // always fails

        String executionId = start(FLAKY_WORKFLOW);

        JsonNode failed = awaitTerminalStatus(executionId, "FAILED");
        assertThat(failed.get("status").asText()).isEqualTo("FAILED");

        List<String> types = eventTypes(executionId);
        // 3 attempts total (policy maxAttempts=3): two retries, then terminal failure.
        assertThat(types).filteredOn("ActivityRetryScheduled"::equals).hasSize(2);
        assertThat(types).filteredOn("ActivityStarted"::equals).hasSize(4); // a, b(3 attempts)
        assertThat(types).filteredOn("ActivityFailed"::equals).hasSize(1);
        assertThat(types).filteredOn("WorkflowFailed"::equals).hasSize(1);
    }

    @Test
    @Order(4)
    @DirtiesContext
    void convertsExecutionTimeoutIntoFailedAttemptAndFailsWhenRetriesExhausted() throws Exception {
        // 'b' is picked up and blocks forever — it will never produce a result. The execution
        // timeout (2s) must treat it as a failed attempt, the retry re-dispatches it, the
        // second timeout exhausts the retries (maxAttempts=2) and the workflow fails.
        activityExecutor.blockOn("b");

        String executionId = start(TIMEOUT_WORKFLOW);

        JsonNode failed = awaitTerminalStatus(executionId, "FAILED");
        assertThat(failed.get("status").asText()).isEqualTo("FAILED");

        List<String> types = eventTypes(executionId);
        assertThat(types).filteredOn("ActivityRetryScheduled"::equals).hasSize(1);
        assertThat(types).filteredOn("ActivityFailed"::equals).hasSize(1);
        assertThat(types).filteredOn("WorkflowFailed"::equals).hasSize(1);
    }

    // ---------------------------------------------------------------- helpers

    private String start(UUID workflowId) throws Exception {
        String response = mockMvc.perform(post("/api/v1/executions")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"workflowId\":\"" + workflowId + "\",\"input\":{\"x\":1}}"))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        return objectMapper.readTree(response).get("id").asText();
    }

    private JsonNode awaitTerminalStatus(String executionId, String expected) throws Exception {
        long deadline = System.currentTimeMillis() + 20_000;
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
}
