package io.fuseflow.registry.controller;

import io.fuseflow.common.dto.WorkerRequest;
import io.fuseflow.common.dto.WorkerResponse;
import io.fuseflow.registry.dto.WorkerRegistration;
import io.fuseflow.registry.service.WorkerService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;

/** REST API for worker registration, heartbeats and discovery (Phase 3, FR-4 / FR-12). */
@RestController
@RequestMapping("/api/v1/workers")
public class WorkerController {

    private final WorkerService workerService;

    public WorkerController(WorkerService workerService) {
        this.workerService = workerService;
    }

    @PostMapping
    public ResponseEntity<WorkerResponse> register(@RequestBody WorkerRequest request) {
        WorkerRegistration registration = workerService.register(request);
        return ResponseEntity.status(registration.created() ? HttpStatus.CREATED : HttpStatus.OK)
                .body(registration.worker());
    }

    @PostMapping("/{id}/heartbeat")
    public ResponseEntity<Void> heartbeat(@PathVariable UUID id) {
        workerService.heartbeat(id);
        return ResponseEntity.noContent().build();
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deregister(@PathVariable UUID id) {
        workerService.deregister(id);
        return ResponseEntity.noContent().build();
    }

    @GetMapping
    public List<WorkerResponse> list() {
        return workerService.list();
    }

    @GetMapping("/activities/{name}")
    public List<WorkerResponse> findCapable(@PathVariable String name) {
        return workerService.findCapable(name);
    }

    @GetMapping("/{id}")
    public WorkerResponse get(@PathVariable UUID id) {
        return workerService.get(id);
    }
}
