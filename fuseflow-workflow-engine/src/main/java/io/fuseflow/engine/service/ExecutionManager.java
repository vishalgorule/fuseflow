package io.fuseflow.engine.service;

import io.fuseflow.common.exception.ApiException;
import io.fuseflow.engine.definition.WorkflowDefinitionReader;
import io.fuseflow.engine.definition.WorkflowDefinitionSnapshot;
import io.fuseflow.engine.dto.EventResponse;
import io.fuseflow.engine.ha.EngineShards;
import io.fuseflow.engine.dto.ExecutionRequest;
import io.fuseflow.engine.dto.ExecutionResponse;
import io.fuseflow.engine.messaging.WorkflowEventPublisher;
import io.fuseflow.engine.model.ActivityExecution;
import io.fuseflow.engine.model.DagModel;
import io.fuseflow.engine.model.WorkflowEvent;
import io.fuseflow.engine.model.WorkflowExecution;
import io.fuseflow.engine.model.WorkflowStatus;
import io.fuseflow.engine.repository.ActivityExecutionRepository;
import io.fuseflow.engine.repository.EventStore;
import io.fuseflow.engine.repository.WorkflowExecutionRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Public facade of the engine (Phase 2, FR-2 start / FR-8 / FR-9):
 * <ul>
 *   <li>{@code start} — validate the definition exists, persist the execution + materialized
 *       dependency graph, append {@code WorkflowStarted}, schedule root activities.</li>
 *   <li>{@code get}/{@code list}/{@code history} — query endpoints for progress and the
 *       immutable event log.</li>
 * </ul>
 */
@Service
public class ExecutionManager {

    private final WorkflowDefinitionReader definitionReader;
    private final WorkflowExecutionRepository executionRepository;
    private final ActivityExecutionRepository activityRepository;
    private final EventStore eventStore;
    private final Scheduler scheduler;
    private final WorkflowEventPublisher workflowEventPublisher;
    private final EngineShards engineShards;
    private final ObjectMapper objectMapper;

    public ExecutionManager(WorkflowDefinitionReader definitionReader,
                            WorkflowExecutionRepository executionRepository,
                            ActivityExecutionRepository activityRepository,
                            EventStore eventStore,
                            Scheduler scheduler,
                            WorkflowEventPublisher workflowEventPublisher,
                            EngineShards engineShards,
                            ObjectMapper objectMapper) {
        this.definitionReader = definitionReader;
        this.executionRepository = executionRepository;
        this.activityRepository = activityRepository;
        this.eventStore = eventStore;
        this.scheduler = scheduler;
        this.workflowEventPublisher = workflowEventPublisher;
        this.engineShards = engineShards;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public ExecutionResponse start(ExecutionRequest request) {
        if (request.workflowId() == null) {
            throw ApiException.badRequest("invalid_execution_request", "workflowId is required");
        }
        WorkflowDefinitionSnapshot definition = definitionReader.find(request.workflowId())
                .orElseThrow(() -> ApiException.notFound("workflow_not_found",
                        "Workflow '" + request.workflowId() + "' does not exist"));

        UUID executionId = UUID.randomUUID();
        Instant now = Instant.now();
        String input = request.input() == null ? null : request.input().toString();
        // Phase 5 engine HA: pin the execution to its shard so any engine instance can scope
        // boot-time recovery to the shards it owns (shardOf is identical on every instance).
        WorkflowExecution execution = new WorkflowExecution(executionId, definition.id(), definition.name(),
                definition.version(), input, null, WorkflowStatus.RUNNING, 0, now, now, now, null,
                engineShards.shardOf(executionId));
        List<DagModel.DagTask> tasks = DagModel.from(definition);
        // Completion counter = number of DAG tasks (each seeded as one activity row); the last
        // terminal completion decrements it to 0 and completes the execution (Phase 7 scale).
        executionRepository.insert(execution, tasks.size());
        activityRepository.insertAll(executionId, tasks);
        Map<String, Object> startedPayload = Map.of(
                "workflowId", definition.id().toString(),
                "workflowName", definition.name(),
                "definitionVersion", definition.version());
        eventStore.append(executionId, "WorkflowStarted", startedPayload);
        workflowEventPublisher.publish(executionId, "WorkflowStarted", startedPayload);

        List<ActivityExecution> roots = activityRepository.findForExecution(executionId).stream()
                .filter(a -> a.remainingDependencies() == 0)
                .toList();
        scheduler.schedule(executionId, roots, input);

        return toResponse(executionRepository.findById(executionId).orElseThrow(),
                activityRepository.findForExecution(executionId));
    }

    public ExecutionResponse get(UUID id) {
        WorkflowExecution execution = executionRepository.findById(id)
                .orElseThrow(() -> ApiException.notFound("execution_not_found",
                        "Execution '" + id + "' does not exist"));
        return toResponse(execution, activityRepository.findForExecution(id));
    }

    public List<ExecutionResponse> list() {
        List<WorkflowExecution> executions = executionRepository.findAll();
        if (executions.isEmpty()) {
            return List.of();
        }
        // Batch-load activities for all executions in one query (avoids N+1, Phase 1 convention).
        List<UUID> ids = executions.stream().map(WorkflowExecution::id).toList();
        Map<UUID, List<ActivityExecution>> activitiesByExecution = activityRepository.findForExecutions(ids).stream()
                .collect(Collectors.groupingBy(ActivityExecution::workflowExecutionId,
                        LinkedHashMap::new, Collectors.toList()));
        return executions.stream()
                .map(execution -> toResponse(execution, activitiesByExecution.getOrDefault(execution.id(), List.of())))
                .toList();
    }

    public List<EventResponse> history(UUID id) {
        if (executionRepository.findById(id).isEmpty()) {
            throw ApiException.notFound("execution_not_found", "Execution '" + id + "' does not exist");
        }
        return eventStore.history(id).stream().map(this::toEventResponse).toList();
    }

    // ---------------------------------------------------------------- mapping

    private ExecutionResponse toResponse(WorkflowExecution execution, List<ActivityExecution> activities) {
        List<ExecutionResponse.ActivityResponse> activityResponses = activities.stream()
                .map(activity -> new ExecutionResponse.ActivityResponse(
                        activity.taskId(),
                        activity.activityName(),
                        activity.status().name(),
                        activity.attempt(),
                        parse(activity.output()),
                        activity.error(),
                        activity.updatedAt()))
                .toList();
        return new ExecutionResponse(
                execution.id(),
                execution.workflowId(),
                execution.workflowName(),
                parse(execution.input()),
                parse(execution.output()),
                execution.status().name(),
                execution.version(),
                execution.createdAt(),
                execution.updatedAt(),
                execution.startedAt(),
                execution.completedAt(),
                activityResponses);
    }

    private EventResponse toEventResponse(WorkflowEvent event) {
        return new EventResponse(event.id(), event.workflowExecutionId(), event.eventType(),
                parse(event.payload()), event.createdAt());
    }

    private JsonNode parse(String json) {
        if (json == null) {
            return null;
        }
        try {
            return objectMapper.readTree(json);
        } catch (Exception ex) {
            return objectMapper.getNodeFactory().textNode(json);
        }
    }
}
