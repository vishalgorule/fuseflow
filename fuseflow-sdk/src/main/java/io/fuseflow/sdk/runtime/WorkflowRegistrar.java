package io.fuseflow.sdk.runtime;

import io.fuseflow.sdk.client.DefinitionClient;
import io.fuseflow.sdk.config.WorkflowProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;

import java.time.Duration;

/**
 * Registers every {@link WorkflowScanner}-discovered workflow with the definition service at
 * boot (Phase 6). Registration is best-effort and idempotent (upsert by name — see
 * {@link DefinitionClient}): with the definition service down, the registrar retries
 * {@code register-retries} times at {@code register-retry-delay}, then logs an error and
 * leaves the definition for a future deploy (the running worker does not depend on it).
 * Runs as an {@link ApplicationRunner}, after the scanners ({@code SmartInitializingSingleton}).
 */
public class WorkflowRegistrar implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(WorkflowRegistrar.class);

    private final WorkflowRegistry registry;
    private final DefinitionClient definitionClient;
    private final int registerRetries;
    private final Duration registerRetryDelay;

    public WorkflowRegistrar(WorkflowRegistry registry, DefinitionClient definitionClient,
                             WorkflowProperties properties) {
        this.registry = registry;
        this.definitionClient = definitionClient;
        this.registerRetries = properties.registerRetries() != null ? properties.registerRetries() : 10;
        this.registerRetryDelay = properties.registerRetryDelay() != null
                ? properties.registerRetryDelay() : Duration.ofSeconds(2);
    }

    @Override
    public void run(ApplicationArguments args) {
        for (WorkflowRegistration registration : registry.all()) {
            registerWithRetry(registration);
        }
    }

    // ---------------------------------------------------------------- internals

    private void registerWithRetry(WorkflowRegistration registration) {
        for (int attempt = 1; attempt <= Math.max(1, registerRetries); attempt++) {
            try {
                definitionClient.register(registration.request());
                return;
            } catch (Exception ex) {
                log.warn("Workflow '{}' registration attempt {}/{} failed: {}",
                        registration.name(), attempt, registerRetries, ex.getMessage());
                if (attempt < registerRetries) {
                    sleep(registerRetryDelay);
                }
            }
        }
        log.error("Workflow '{}' could not be registered after {} attempts — the definition "
                        + "service was unreachable or rejected it; fix and redeploy",
                registration.name(), registerRetries);
    }

    private static void sleep(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }
}
