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
 * <p>Registration is an <b>upsert by name</b>, matching the definition service's unique-name
 * model: POST; on a name conflict (409), look the workflow up by name and either do nothing
 * (identical DAG) or PUT-replace it (different DAG). Multi-instance deployments therefore
 * re-register without conflicts, and a changed {@code @Workflow} is picked up on the next
 * deploy.
 */
public class DefinitionClient {

    private static final Logger log = LoggerFactory.getLogger(DefinitionClient.class);

    private final RestClient restClient;

    public DefinitionClient(String definitionBaseUrl) {
        this.restClient = RestClient.builder().baseUrl(definitionBaseUrl).build();
    }

    /**
     * Registers (or re-registers) a workflow definition, idempotently by name. Returns the
     * resulting definition. A {@code 409} name conflict triggers a name lookup followed by
     * a no-op (same DAG) or a PUT (different DAG).
     */
    public WorkflowResponse register(WorkflowRequest request) {
        try {
            WorkflowResponse created = restClient.post()
                    .uri("/api/v1/workflows")
                    .body(request)
                    .retrieve()
                    .body(WorkflowResponse.class);
            log.info("Registered workflow '{}' ({}) with {} task(s)",
                    request.name(), created == null ? "?" : created.id(), request.tasks().size());
            return created;
        } catch (HttpClientErrorException.Conflict ex) {
            return resolveConflict(request);
        }
    }

    /** Lookup by unique name; empty when the definition does not exist. */
    public List<WorkflowResponse> findByName(String name) {
        return restClient.get()
                .uri(uriBuilder -> uriBuilder.path("/api/v1/workflows")
                        .queryParam("name", name)
                        .build())
                .retrieve()
                .body(new ParameterizedTypeReference<>() {
                });
    }

    // ---------------------------------------------------------------- internals

    private WorkflowResponse resolveConflict(WorkflowRequest request) {
        WorkflowResponse existing = findByName(request.name()).stream().findFirst().orElse(null);
        if (existing != null && sameDag(request, existing)) {
            log.info("Workflow '{}' already registered with an identical DAG — no-op", request.name());
            return existing;
        }
        if (existing == null) {
            // Name conflict raced with a delete; just retry the create once.
            log.warn("Workflow '{}' conflicted but no definition found by name — retrying create", request.name());
            return restClient.post()
                    .uri("/api/v1/workflows")
                    .body(request)
                    .retrieve()
                    .body(WorkflowResponse.class);
        }
        log.info("Workflow '{}' changed — replacing definition {}", request.name(), existing.id());
        return restClient.put()
                .uri("/api/v1/workflows/{id}", existing.id())
                .body(request)
                .retrieve()
                .body(WorkflowResponse.class);
    }

    /**
     * Structural comparison (ids/activities/dependency edges + retry policies; description is
     * ignored). A changed retry policy must trigger a replacement, so policies are compared too.
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
