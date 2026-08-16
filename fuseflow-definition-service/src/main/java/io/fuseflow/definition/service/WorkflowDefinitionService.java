package io.fuseflow.definition.service;

import io.fuseflow.common.dto.ApiError;
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
 * Register, update, delete and retrieve workflow definitions. Every create/update
 * runs {@link DagValidator}; invalid DAGs are rejected with field-level errors.
 */
@Service
public class WorkflowDefinitionService {

    private final WorkflowDefinitionRepository definitionRepository;
    private final WorkflowTaskRepository taskRepository;
    private final DagValidator dagValidator;

    public WorkflowDefinitionService(WorkflowDefinitionRepository definitionRepository,
                                     WorkflowTaskRepository taskRepository,
                                     DagValidator dagValidator) {
        this.definitionRepository = definitionRepository;
        this.taskRepository = taskRepository;
        this.dagValidator = dagValidator;
    }

    @Transactional
    public WorkflowResponse create(WorkflowRequest request) {
        validate(request);
        UUID id = UUID.randomUUID();
        Instant now = Instant.now();
        WorkflowDefinition definition = new WorkflowDefinition(id, request.name(), request.description(), 0, now, now);
        try {
            definitionRepository.insert(definition);
        } catch (DuplicateKeyException ex) {
            // Unique name violated (possibly raced with a concurrent create).
            throw ApiException.conflict("workflow_name_conflict",
                    "A workflow named '" + request.name() + "' already exists");
        }
        taskRepository.replaceAll(id, toTasks(id, request.tasks()), toDependencies(id, request.tasks()));
        return get(id);
    }

    @Transactional
    public WorkflowResponse update(UUID id, WorkflowRequest request) {
        validate(request);
        WorkflowDefinition existing = definitionRepository.findById(id)
                .orElseThrow(() -> ApiException.notFound("workflow_not_found", "Workflow '" + id + "' does not exist"));
        if (definitionRepository.existsByNameExcluding(request.name(), id)) {
            throw ApiException.conflict("workflow_name_conflict",
                    "A workflow named '" + request.name() + "' already exists");
        }

        if (!definitionRepository.update(id, request.name(), request.description(), existing.version())) {
            throw ApiException.conflict("workflow_version_conflict",
                    "Workflow '" + id + "' was modified concurrently; retry");
        }
        taskRepository.replaceAll(id, toTasks(id, request.tasks()), toDependencies(id, request.tasks()));
        return get(id);
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

    /**
     * Lookup by unique name (Phase 6): enables the SDK's idempotent registration (same DAG →
     * no-op, different DAG → replace). Phase 8 extends this with {@code version} for
     * multi-versioned lookups.
     */
    public Optional<WorkflowResponse> findByName(String name) {
        return definitionRepository.findByName(name)
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

    private List<WorkflowTask> toTasks(UUID workflowId, List<WorkflowRequest.Task> tasks) {
        return tasks.stream()
                .map(task -> new WorkflowTask(workflowId, task.id(), task.activity()))
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
                        depsByTask.getOrDefault(task.taskId(), List.of())))
                .toList();
        return new WorkflowResponse(definition.id(), definition.name(), definition.description(),
                responseTasks, definition.version(), definition.createdAt(), definition.updatedAt());
    }

    private <T> Map<UUID, List<T>> groupByWorkflow(List<T> rows, Function<T, UUID> workflowIdFn) {
        Map<UUID, List<T>> grouped = new LinkedHashMap<>();
        for (T row : rows) {
            grouped.computeIfAbsent(workflowIdFn.apply(row), key -> new ArrayList<>()).add(row);
        }
        return grouped;
    }
}
