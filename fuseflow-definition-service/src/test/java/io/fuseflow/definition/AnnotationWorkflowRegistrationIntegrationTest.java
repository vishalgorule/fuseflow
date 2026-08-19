package io.fuseflow.definition;

import io.fuseflow.common.dto.WorkflowRequest;
import io.fuseflow.common.dto.WorkflowResponse;
import io.fuseflow.sdk.client.DefinitionClient;
import io.fuseflow.sdk.runtime.WorkflowRegistration;
import io.fuseflow.sdk.runtime.WorkflowRegistry;
import io.fuseflow.testfixtures.AnnotationWorkflowFixtures;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Phase 6 integration: an annotation-defined {@code @Workflow} (exactly what a worker project
 * declares in code) is scanned by the SDK's {@code WorkflowScanner}, validated with the shared
 * {@code DagValidator}, and registered through the SDK's {@link DefinitionClient} against the
 * live definition service REST API — then re-registration is idempotent by name (same DAG →
 * no-op, different DAG → replace). This is the same path a worker boots through
 * ({@code WorkflowRegistrar}); it proves the annotation → wire-contract → REST → DB journey
 * end-to-end.
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "fuseflow.workflow.enabled=false")
@AutoConfigureMockMvc
class AnnotationWorkflowRegistrationIntegrationTest {

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:18.4");

    @Autowired
    MockMvc mockMvc;

    @LocalServerPort
    int port;

    @Test
    void registersAnnotationDefinedWorkflowAndUpsertsByIdempotently() throws Exception {
        DefinitionClient client = new DefinitionClient("http://localhost:" + port);

        // 1. A worker project's @Workflow class is scanned into a wire request.
        WorkflowRequest request = scan().request();
        assertThat(request.name()).isEqualTo("annotation-diamond");
        assertThat(request.tasks()).hasSize(5);

        // 2. Registration persists it and it is readable by name.
        WorkflowResponse created = client.register(request);
        assertThat(created.id()).isNotNull();
        assertThat(created.tasks()).hasSize(5);

        mockMvc.perform(get("/api/v1/workflows").param("name", "annotation-diamond"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(1))
                .andExpect(jsonPath("$[0].tasks.length()").value(5))
                // Response tasks are sorted by id; find the watermark task by filter.
                .andExpect(jsonPath("$[0].tasks[?(@.id == 'watermark')].dependsOn[0]").value("resize"));

        // 3. Re-registering the identical definition is a no-op — version unchanged.
        WorkflowResponse again = client.register(request);
        assertThat(again.version()).isEqualTo(created.version());

        // 4. Phase 8: a changed DAG under the SAME version is an operator error — the client
        // fails loud (definitions are immutable snapshots; bump @Workflow.version() instead).
        WorkflowRequest changed = new WorkflowRequest("annotation-diamond", "simplified",
                List.of(new WorkflowRequest.Task("a", "downloadImage", null)));
        assertThatThrownBy(() -> client.register(changed))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("bump @Workflow.version()");

        // The original version-1 snapshot is untouched.
        mockMvc.perform(get("/api/v1/workflows").param("name", "annotation-diamond"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].tasks.length()").value(5));

        // 5. Registering the changed DAG under a NEW version succeeds and the versions coexist.
        WorkflowResponse updated = client.register(new WorkflowRequest("annotation-diamond", "2",
                "simplified", null, List.of(new WorkflowRequest.Task("a", "downloadImage", null))));
        assertThat(updated.semanticVersion()).isEqualTo("2");
        assertThat(updated.tasks()).hasSize(1);

        mockMvc.perform(get("/api/v1/workflows").param("name", "annotation-diamond"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(2));
    }

    // ---------------------------------------------------------------- helpers

    private WorkflowRegistration scan() {
        try (AnnotationConfigApplicationContext context =
                     new AnnotationConfigApplicationContext(AnnotationWorkflowFixtures.class)) {
            return context.getBean(WorkflowRegistry.class).all().get(0);
        }
    }
}
