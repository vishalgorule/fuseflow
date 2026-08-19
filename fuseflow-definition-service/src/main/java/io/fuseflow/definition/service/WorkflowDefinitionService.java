package io.fuseflow.definition.service;

import io.fuseflow.common.dto.ApiError;
import io.fuseflow.common.dto.RetryPolicy;
import io.fuseflow.common.dto.WorkflowRequest;
import io.fuseflow.common.dto.WorkflowResponse;
import io.fuseflow.common.exception.ApiException;
import io.fuseflow.common.validation.DagValidator;
import io.fuseflow.definition.model.TaskDependency;
import io.fuseflow.definition.model.WorkflowDefinition;
import io.fuseflow.definition.model.WorkflowTask;
import io.fuseflow.definition.repository.WorkflowDefinitionRepository;
import io.fuseflow.definition.repository.WorkflowTaskRepository;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Register, list and retrieve workflow definitions. Every create runs {@link DagValidator};
 * invalid DAGs are rejected with field-level errors.
 *
 * <p>Phase 8: definitions are <b>immutable version snapshots</b> — {@code (name, semanticVersion)}
 * is the unique key, and changing a DAG means registering a new version. {@code PUT} on an
 * existing snapshot is rejected (409); the SDK bumps {@code @Workflow.version()} instead.
 */
@Service
public class WorkflowDefinitionService {

    private static final String DEFAULT_VERSION = "1";

    private final WorkflowDefinitionRepository definitionRepository;
    private final WorkflowTaskRepository taskRepository;
    private final DagValidator dagValidator;
    private final ObjectMapper objectMapper;

    public WorkflowDefinitionService(WorkflowDefinitionRepository definitionRepository,
                                     WorkflowTaskRepository taskRepository,
                                     DagValidator dagValidator,
                                     ObjectMapper objectMapper) {
        this.definitionRepository = definitionRepository;
        this.taskRepository = taskRepository;
        this.dagValidator = dagValidator;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public WorkflowResponse create(WorkflowRequest request) {
        validate(request);
        UUID id = UUID.randomUUID();
        Instant now = Instant.now();
        String semanticVersion = normalizedVersion(request.semanticVersion());
        WorkflowDefinition definition = new WorkflowDefinition(id, request.name(), semanticVersion,
                request.description(), toJson(request.retryPolicy()), 0, now, now);
        try {
            definitionRepository.insert(definition);
        } catch (DuplicateKeyException ex) {
            // Unique (name, semantic_version) violated (possibly raced with a concurrent create).
            throw ApiException.conflict("workflow_name_conflict",
                    "A workflow named '" + request.name() + "' with version '" + semanticVersion
                            + "' already exists — register a new version instead");
        }
        taskRepository.replaceAll(id, toTasks(id, request.tasks()), toDependencies(id, request.tasks()));
        return get(id);
    }

    /**
     * Phase 8: versions are immutable snapshots — a {@code (name, version)} row is fixed once
     * created. Changing the DAG means registering a new version; {@code PUT} on an existing
     * snapshot is always rejected so callers cannot silently mutate a version in flight.
     */
    @Transactional
    public WorkflowResponse update(UUID id, WorkflowRequest request) {
        WorkflowDefinition existing = definitionRepository.findById(id)
                .orElseThrow(() -> ApiException.notFound("workflow_not_found", "Workflow '" + id + "' does not exist"));
        throw ApiException.conflict("workflow_version_immutable",
                "Workflow '" + existing.name() + "' version '" + existing.semanticVersion()
                        + "' is an immutable snapshot — register a new version (POST) to change its DAG");
    }

    @Transactional
    public void delete(UUID id) {
        if (!definitionRepository.delete(id)) {
            throw ApiException.notFound("workflow_not_found", "Workflow '" + id + "' does not exist");
        }
    }

    public List<WorkflowResponse> list() {
        List<WorkflowDefinition> definitions = definitionRepository.findAll();
        if (definitions.isEmpty()) {
            return List.of();
        }
        List<UUID> ids = definitions.stream().map(WorkflowDefinition::id).toList();
        Map<UUID, List<WorkflowTask>> tasksByWorkflow =
                groupByWorkflow(taskRepository.findTasksForWorkflows(ids), WorkflowTask::workflowId);
        Map<UUID, List<TaskDependency>> depsByWorkflow =
                groupByWorkflow(taskRepository.findDependenciesForWorkflows(ids), TaskDependency::workflowId);
        return definitions.stream()
                .map(def -> toResponse(def,
                        tasksByWorkflow.getOrDefault(def.id(), List.of()),
                        depsByWorkflow.getOrDefault(def.id(), List.of())))
                .toList();
    }

    public WorkflowResponse get(UUID id) {
        WorkflowDefinition definition = definitionRepository.findById(id)
                .orElseThrow(() -> ApiException.notFound("workflow_not_found", "Workflow '" + id + "' does not exist"));
        return toResponse(definition, taskRepository.findTasks(id), taskRepository.findDependencies(id));
    }

    /** All versions of a workflow name (Phase 8), newest first. */
    public List<WorkflowResponse> findByName(String name) {
        return definitionRepository.findAllByName(name).stream()
                .map(definition -> toResponse(definition,
                        taskRepository.findTasks(definition.id()),
                        taskRepository.findDependencies(definition.id())))
                .toList();
    }

    /** The exact {@code (name, semanticVersion)} snapshot (Phase 8). */
    public Optional<WorkflowResponse> findByNameAndVersion(String name, String semanticVersion) {
        return definitionRepository.findByNameAndVersion(name, normalizedVersion(semanticVersion))
                .map(definition -> toResponse(definition,
                        taskRepository.findTasks(definition.id()),
                        taskRepository.findDependencies(definition.id())));
    }

    // ------------------------------------------------------------------ helpers

    private void validate(WorkflowRequest request) {
        List<ApiError.FieldError> errors = dagValidator.validate(request);
        if (!errors.isEmpty()) {
            throw ApiException.badRequest("invalid_workflow", "Workflow definition is invalid", errors);
        }
    }

    /** {@code null}/{@code blank} → "1" so pre-Phase 8 callers keep working unchanged. */
    private static String normalizedVersion(String semanticVersion) {
        return (semanticVersion == null || semanticVersion.isBlank()) ? DEFAULT_VERSION : semanticVersion.trim();
    }

    private List<WorkflowTask> toTasks(UUID workflowId, List<WorkflowRequest.Task> tasks) {
        return tasks.stream()
                .map(task -> new WorkflowTask(workflowId, task.id(), task.activity(), toJson(task.retryPolicy())))
                .toList();
    }

    private List<TaskDependency> toDependencies(UUID workflowId, List<WorkflowRequest.Task> tasks) {
        // TaskDependency is a record, so set membership dedupes identical (task, dependsOn) pairs.
        Set<TaskDependency> dependencies = new LinkedHashSet<>();
        for (WorkflowRequest.Task task : tasks) {
            if (task.dependsOn() == null) {
                continue;
            }
            for (String dep : task.dependsOn()) {
                dependencies.add(new TaskDependency(workflowId, task.id(), dep));
            }
        }
        return List.copyOf(dependencies);
    }

    /** Assembles a response from the persisted task rows (deterministic ordering). */
    private WorkflowResponse toResponse(WorkflowDefinition definition, List<WorkflowTask> tasks,
                                        List<TaskDependency> dependencies) {
        Map<String, List<String>> depsByTask = dependencies.stream()
                .collect(Collectors.groupingBy(TaskDependency::taskId,
                        Collectors.mapping(TaskDependency::dependsOn, Collectors.toList())));
        List<WorkflowResponse.Task> responseTasks = tasks.stream()
                .sorted(Comparator.comparing(WorkflowTask::taskId))
                .map(task -> new WorkflowResponse.Task(task.taskId(), task.activityName(),
                        depsByTask.getOrDefault(task.taskId(), List.of()),
                        parsePolicy(task.retryPolicyJson())))
                .toList();
        return new WorkflowResponse(definition.id(), definition.name(), definition.semanticVersion(),
                definition.description(), parsePolicy(definition.retryPolicyJson()),
                responseTasks, definition.version(), definition.createdAt(), definition.updatedAt());
    }

    private String toJson(RetryPolicy policy) {
        if (policy == null || policy.isEmpty()) {
            return null;
        }
        try {
            return objectMapper.writeValueAsString(policy);
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to serialize retry policy", ex);
        }
    }

    private RetryPolicy parsePolicy(String json) {
        if (json == null || json.isBlank()) {
            return null;
        }
        try {
            return objectMapper.readValue(json, RetryPolicy.class);
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to deserialize retry policy", ex);
        }
    }

    private <T> Map<UUID, List<T>> groupByWorkflow(List<T> rows, Function<T, UUID> workflowIdFn) {
        Map<UUID, List<T>> grouped = new LinkedHashMap<>();
        for (T row : rows) {
            grouped.computeIfAbsent(workflowIdFn.apply(row), key -> new ArrayList<>()).add(row);
        }
        return grouped;
    }
}
