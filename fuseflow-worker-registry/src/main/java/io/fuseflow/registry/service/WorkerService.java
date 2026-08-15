package io.fuseflow.registry.service;

import io.fuseflow.common.dto.ApiError;
import io.fuseflow.common.dto.WorkerRequest;
import io.fuseflow.common.dto.WorkerResponse;
import io.fuseflow.common.exception.ApiException;
import io.fuseflow.registry.dto.WorkerRegistration;
import io.fuseflow.registry.messaging.WorkerEventPublisher;
import io.fuseflow.registry.model.Worker;
import io.fuseflow.registry.model.WorkerActivity;
import io.fuseflow.registry.model.WorkerStatus;
import io.fuseflow.registry.repository.WorkerRepository;
import io.fuseflow.registry.validation.WorkerValidator;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Register, heartbeat, deregister and discover workers (Phase 3, FR-4 / FR-12).
 *
 * <p>Registration is an upsert keyed on the client-supplied worker id: a worker restarts and
 * re-registers on every boot, so an existing id updates host/activities and revives the worker
 * to ONLINE instead of conflicting. Heartbeats are lightweight "touch" writes; the scheduled
 * {@link OfflineDetector} derives liveness from {@code last_heartbeat_at}.
 */
@Service
public class WorkerService {

    private final WorkerRepository workerRepository;
    private final WorkerValidator workerValidator;
    private final WorkerEventPublisher workerEventPublisher;

    public WorkerService(WorkerRepository workerRepository, WorkerValidator workerValidator,
                         WorkerEventPublisher workerEventPublisher) {
        this.workerRepository = workerRepository;
        this.workerValidator = workerValidator;
        this.workerEventPublisher = workerEventPublisher;
    }

    @Transactional
    public WorkerRegistration register(WorkerRequest request) {
        List<ApiError.FieldError> errors = workerValidator.validate(request);
        if (!errors.isEmpty()) {
            throw ApiException.badRequest("invalid_worker_request", "Worker registration is invalid", errors);
        }
        String poolName = request.poolName() == null || request.poolName().isBlank()
                ? "default" : request.poolName();
        Instant now = Instant.now();

        Worker existing = workerRepository.findById(request.id()).orElse(null);
        if (existing == null) {
            workerRepository.insertWorker(new Worker(request.id(), request.host(),
                    WorkerStatus.ONLINE, now, 0, now, now, poolName, request.concurrency()));
            workerRepository.replaceActivities(request.id(), request.activities());
            workerEventPublisher.publish(request.id(), "worker_registered", eventPayload(request, poolName));
            return new WorkerRegistration(get(request.id()), true);
        }
        if (!workerRepository.updateOnRegister(request.id(), request.host(), poolName,
                request.concurrency(), existing.version())) {
            throw ApiException.conflict("worker_version_conflict",
                    "Worker '" + request.id() + "' was modified concurrently; retry");
        }
        workerRepository.replaceActivities(request.id(), request.activities());
        workerEventPublisher.publish(request.id(), "worker_registered", eventPayload(request, poolName));
        return new WorkerRegistration(get(request.id()), false);
    }

    @Transactional
    public void heartbeat(UUID id) {
        if (workerRepository.touchHeartbeat(id) == 0) {
            throw ApiException.notFound("worker_not_found", "Worker '" + id + "' does not exist");
        }
        workerRepository.appendHeartbeat(id);
    }

    @Transactional
    public void deregister(UUID id) {
        if (!workerRepository.deleteWorker(id)) {
            throw ApiException.notFound("worker_not_found", "Worker '" + id + "' does not exist");
        }
        workerEventPublisher.publish(id, "worker_deregistered", Map.of());
    }

    public List<WorkerResponse> list() {
        return toResponses(workerRepository.findAll());
    }

    public WorkerResponse get(UUID id) {
        Worker worker = workerRepository.findById(id)
                .orElseThrow(() -> ApiException.notFound("worker_not_found", "Worker '" + id + "' does not exist"));
        return toResponse(worker, workerRepository.findActivities(id).stream()
                .map(WorkerActivity::activityName)
                .toList());
    }

    /** Capability lookup (FR-12): every worker advertising the activity, with its health. */
    public List<WorkerResponse> findCapable(String activityName) {
        return toResponses(workerRepository.findByActivity(activityName));
    }

    private static Map<String, Object> eventPayload(WorkerRequest request, String poolName) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("host", request.host());
        payload.put("poolName", poolName);
        payload.put("concurrency", request.concurrency());
        payload.put("activities", request.activities());
        return payload;
    }

    // ---------------------------------------------------------------- mapping

    /** Assembles responses with activities batch-loaded in one query (avoids N+1). */
    private List<WorkerResponse> toResponses(List<Worker> workers) {
        if (workers.isEmpty()) {
            return List.of();
        }
        List<UUID> ids = workers.stream().map(Worker::id).toList();
        Map<UUID, List<String>> activitiesByWorker = groupActivities(workerRepository.findActivitiesForWorkers(ids));
        return workers.stream()
                .map(worker -> toResponse(worker, activitiesByWorker.getOrDefault(worker.id(), List.of())))
                .toList();
    }

    private Map<UUID, List<String>> groupActivities(List<WorkerActivity> rows) {
        return rows.stream()
                .collect(Collectors.groupingBy(WorkerActivity::workerId,
                        LinkedHashMap::new, Collectors.mapping(WorkerActivity::activityName, Collectors.toList())));
    }

    private WorkerResponse toResponse(Worker worker, List<String> activities) {
        return new WorkerResponse(
                worker.id(),
                worker.host(),
                worker.status().name(),
                List.copyOf(activities),
                worker.poolName(),
                worker.concurrency(),
                worker.lastHeartbeatAt(),
                worker.version(),
                worker.createdAt(),
                worker.updatedAt());
    }
}
