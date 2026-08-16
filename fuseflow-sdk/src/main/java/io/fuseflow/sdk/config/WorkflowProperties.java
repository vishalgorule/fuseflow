package io.fuseflow.sdk.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Annotation-workflow registration configuration ({@code fuseflow.workflow.*}, Phase 6).
 * Registration is best-effort at boot (like worker registration): if the definition service
 * is down, the registrar retries {@code register-retries} times at {@code register-retry-delay}
 * before giving up and logging an error. The definition-service endpoint itself is
 * {@code fuseflow.definition.base-url} (default {@code http://localhost:8081}); enabling is
 * {@code fuseflow.workflow.enabled=true} (on by default when the SDK is present).
 *
 * @param registerRetries   registration attempts per workflow before giving up at boot
 *                          (default 10)
 * @param registerRetryDelay delay between registration attempts (default 2s)
 */
@ConfigurationProperties(prefix = "fuseflow.workflow")
public record WorkflowProperties(Integer registerRetries, Duration registerRetryDelay) {
}
