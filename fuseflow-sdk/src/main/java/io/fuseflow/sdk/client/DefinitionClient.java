package io.fuseflow.sdk.client;

import io.fuseflow.common.dto.RetryPolicy;
import io.fuseflow.common.dto.WorkflowRequest;
import io.fuseflow.common.dto.WorkflowResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Thin REST client for the workflow definition service (Phase 6, FR-1): idempotent
 * registration of annotation-defined workflows. The API contract (DTOs) is shared via
 * {@code fuseflow-common}.
 *
 * <p>Registration is an <b>upsert by (name, semanticVersion)</b> (Phase 8): definitions are
 * immutable version snapshots, so POST; on a name+version conflict (409), look the exact
 * version up and either do nothing (identical DAG) or <b>fail loud</b> (different DAG on the
 * same version — the operator must bump {@code @Workflow.version()} in the annotation). The
 * pre-Phase 8 replace-by-PUT path is gone: a changed DAG is a <em>new version</em>, never a
 * mutation of a snapshot that executions may already pin.
 */
public class DefinitionClient {

    private static final Logger log = LoggerFactory.getLogger(DefinitionClient.class);

    private final RestClient restClient;

    public DefinitionClient(String definitionBaseUrl) {
        this.restClient = RestClient.builder().baseUrl(definitionBaseUrl).build();
    }

    /**
     * Registers (or re-registers) a workflow definition, idempotently by (name, version).
     * Returns the resulting definition. A {@code 409} name+version conflict triggers a
     * version lookup followed by a no-op (same DAG) or a loud failure (different DAG — bump
     * the version in the annotation).
     */
    public WorkflowResponse register(WorkflowRequest request) {
        try {
            WorkflowResponse created = restClient.post()
                    .uri("/api/v1/workflows")
                    .body(request)
                    .retrieve()
                    .body(WorkflowResponse.class);
            log.info("Registered workflow '{}' version '{}' ({}) with {} task(s)",
                    request.name(), versionOf(request), created == null ? "?" : created.id(), request.tasks().size());
            return created;
        } catch (HttpClientErrorException.Conflict ex) {
            return resolveConflict(request);
        }
    }

    /** All versions of a workflow name, newest first; empty when none exist. */
    public List<WorkflowResponse> findByName(String name) {
        return restClient.get()
                .uri(uriBuilder -> uriBuilder.path("/api/v1/workflows")
                        .queryParam("name", name)
                        .build())
                .retrieve()
                .body(new ParameterizedTypeReference<>() {
                });
    }

    /** The exact {@code (name, version)} snapshot; empty when that version does not exist. */
    public List<WorkflowResponse> findByNameAndVersion(String name, String version) {
        return restClient.get()
                .uri(uriBuilder -> uriBuilder.path("/api/v1/workflows")
                        .queryParam("name", name)
                        .queryParam("version", version)
                        .build())
                .retrieve()
                .body(new ParameterizedTypeReference<>() {
                });
    }

    // ---------------------------------------------------------------- internals

    private WorkflowResponse resolveConflict(WorkflowRequest request) {
        String version = versionOf(request);
        WorkflowResponse existing = findByNameAndVersion(request.name(), version).stream().findFirst().orElse(null);
        if (existing != null && sameDag(request, existing)) {
            log.info("Workflow '{}' version '{}' already registered with an identical DAG — no-op",
                    request.name(), version);
            return existing;
        }
        if (existing == null) {
            // Name+version conflict raced with a delete; just retry the create once.
            log.warn("Workflow '{}' version '{}' conflicted but no definition found — retrying create",
                    request.name(), version);
            return restClient.post()
                    .uri("/api/v1/workflows")
                    .body(request)
                    .retrieve()
                    .body(WorkflowResponse.class);
        }
        // Phase 8: a different DAG on the same version is an operator error, not something the
        // SDK can paper over — definitions are immutable snapshots and executions may already
        // pin this version. The fix is to bump @Workflow.version() and re-deploy.
        throw new IllegalStateException("Workflow '" + request.name() + "' version '" + version
                + "' already exists with a different DAG. Definitions are immutable version "
                + "snapshots (Phase 8) — bump @Workflow.version() to \"" + nextVersion(version)
                + "\" (or higher) in the annotation and redeploy to register the changed DAG.");
    }

    private static String versionOf(WorkflowRequest request) {
        return request.semanticVersion() == null || request.semanticVersion().isBlank()
                ? "1" : request.semanticVersion();
    }

    /** A cheap suggestion for the next version label (vN → vN+1); operators can use any label. */
    private static String nextVersion(String version) {
        String trimmed = version.trim();
        if (trimmed.matches("v?\\d+")) {
            int base = Integer.parseInt(trimmed.replaceAll("[^0-9]", ""));
            return (trimmed.startsWith("v") ? "v" : "") + (base + 1);
        }
        return version + ".1";
    }

    /**
     * Structural comparison (ids/activities/dependency edges + retry policies; description is
     * ignored). A changed retry policy must trigger a loud failure, so policies are compared too.
     */
    private boolean sameDag(WorkflowRequest request, WorkflowResponse existing) {
        if (request.tasks() == null || existing.tasks() == null) {
            return request.tasks() == null && existing.tasks() == null;
        }
        if (request.tasks().size() != existing.tasks().size()
                || !samePolicy(request.retryPolicy(), existing.retryPolicy())) {
            return false;
        }
        Map<String, WorkflowRequest.Task> byId = request.tasks().stream()
                .collect(Collectors.toMap(WorkflowRequest.Task::id, Function.identity()));
        for (WorkflowResponse.Task task : existing.tasks()) {
            WorkflowRequest.Task candidate = byId.get(task.id());
            if (candidate == null || !candidate.activity().equals(task.activity())
                    || !sameSet(candidate.dependsOn(), task.dependsOn())
                    || !samePolicy(candidate.retryPolicy(), task.retryPolicy())) {
                return false;
            }
        }
        return true;
    }

    /** Policy equality: null and empty are equivalent; exception patterns compare as a set. */
    private static boolean samePolicy(RetryPolicy a, RetryPolicy b) {
        boolean aEmpty = a == null || a.isEmpty();
        boolean bEmpty = b == null || b.isEmpty();
        if (aEmpty || bEmpty) {
            return aEmpty && bEmpty;
        }
        return java.util.Objects.equals(a.maxAttempts(), b.maxAttempts())
                && java.util.Objects.equals(a.fixedDelaySeconds(), b.fixedDelaySeconds())
                && java.util.Objects.equals(a.exponentialBackoff(), b.exponentialBackoff())
                && java.util.Objects.equals(a.backoffMultiplier(), b.backoffMultiplier())
                && sameSet(a.nonRetryableExceptions(), b.nonRetryableExceptions());
    }

    private static boolean sameSet(List<String> a, List<String> b) {
        Set<String> setA = a == null ? Set.of() : new HashSet<>(a);
        Set<String> setB = b == null ? Set.of() : new HashSet<>(b);
        return setA.equals(setB);
    }
}
