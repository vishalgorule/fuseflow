package io.fuseflow.engine.controller;

import io.fuseflow.engine.dto.EventResponse;
import io.fuseflow.engine.dto.ExecutionRequest;
import io.fuseflow.engine.dto.ExecutionResponse;
import io.fuseflow.engine.service.ExecutionManager;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;

/** REST API for workflow executions (Phase 2, FR-2 start / FR-9). */
@RestController
@RequestMapping("/api/v1/executions")
public class ExecutionController {

    private final ExecutionManager executionManager;

    public ExecutionController(ExecutionManager executionManager) {
        this.executionManager = executionManager;
    }

    @PostMapping
    public ResponseEntity<ExecutionResponse> start(@RequestBody ExecutionRequest request) {
        return ResponseEntity.status(HttpStatus.CREATED).body(executionManager.start(request));
    }

    @GetMapping
    public List<ExecutionResponse> list() {
        return executionManager.list();
    }

    @GetMapping("/{id}")
    public ExecutionResponse get(@PathVariable UUID id) {
        return executionManager.get(id);
    }

    @GetMapping("/{id}/history")
    public List<EventResponse> history(@PathVariable UUID id) {
        return executionManager.history(id);
    }

    // ---------------------------------------------------------------- lifecycle (Phase 8, FR-2)

    @PostMapping("/{id}/pause")
    public ExecutionResponse pause(@PathVariable UUID id) {
        return executionManager.pause(id);
    }

    @PostMapping("/{id}/resume")
    public ExecutionResponse resume(@PathVariable UUID id) {
        return executionManager.resume(id);
    }

    @PostMapping("/{id}/cancel")
    public ExecutionResponse cancel(@PathVariable UUID id) {
        return executionManager.cancel(id);
    }

    @PostMapping("/{id}/restart")
    public ResponseEntity<ExecutionResponse> restart(@PathVariable UUID id) {
        return ResponseEntity.status(HttpStatus.CREATED).body(executionManager.restart(id));
    }
}
