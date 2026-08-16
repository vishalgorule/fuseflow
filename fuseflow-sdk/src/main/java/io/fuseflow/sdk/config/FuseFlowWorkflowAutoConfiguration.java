package io.fuseflow.sdk.config;

import io.fuseflow.common.validation.DagValidator;
import io.fuseflow.sdk.client.DefinitionClient;
import io.fuseflow.sdk.runtime.ActivityRegistry;
import io.fuseflow.sdk.runtime.WorkflowRegistrar;
import io.fuseflow.sdk.runtime.WorkflowRegistry;
import io.fuseflow.sdk.runtime.WorkflowScanner;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.web.client.RestClient;

/**
 * Wires annotation-based workflow registration (Phase 6, FR-1) into any application with the
 * SDK on its classpath. Enabled via {@code fuseflow.workflow.enabled=true} (on by default),
 * and needs <b>only</b> the definition service's REST API — no Kafka, no worker registry — so
 * a workflow-only deployable works with just this configuration.
 *
 * <p>Registered via {@code META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports}.
 */
@AutoConfiguration
@ConditionalOnProperty(name = "fuseflow.workflow.enabled", havingValue = "true", matchIfMissing = true)
@ConditionalOnClass(RestClient.class)
@EnableConfigurationProperties(WorkflowProperties.class)
public class FuseFlowWorkflowAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public DagValidator workflowDagValidator() {
        // Shared with the definition service — the SDK validates with the exact same rules.
        return new DagValidator();
    }

    @Bean
    @ConditionalOnMissingBean
    public WorkflowRegistry workflowRegistry() {
        return new WorkflowRegistry();
    }

    @Bean
    @ConditionalOnMissingBean
    public WorkflowScanner workflowScanner(ApplicationContext applicationContext,
                                           WorkflowRegistry workflowRegistry,
                                           DagValidator workflowDagValidator,
                                           ObjectProvider<ActivityRegistry> activityRegistry) {
        return new WorkflowScanner(applicationContext, workflowRegistry,
                workflowDagValidator, activityRegistry);
    }

    @Bean
    @ConditionalOnMissingBean
    public DefinitionClient definitionClient(
            @Value("${fuseflow.definition.base-url:http://localhost:8081}") String baseUrl) {
        return new DefinitionClient(baseUrl);
    }

    @Bean
    @ConditionalOnMissingBean
    public WorkflowRegistrar workflowRegistrar(WorkflowRegistry workflowRegistry,
                                               DefinitionClient definitionClient,
                                               WorkflowProperties workflowProperties) {
        return new WorkflowRegistrar(workflowRegistry, definitionClient, workflowProperties);
    }
}
