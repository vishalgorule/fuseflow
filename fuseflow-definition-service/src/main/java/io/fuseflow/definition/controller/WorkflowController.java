package io.fuseflow.definition.controller;

import io.fuseflow.definition.dto.WorkflowRequest;
import io.fuseflow.definition.dto.WorkflowResponse;
import io.fuseflow.definition.service.WorkflowDefinitionService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;

/** REST API for workflow definition management (Phase 1, FR-1). */
@RestController
@RequestMapping("/api/v1/workflows")
public class WorkflowController {

    private final WorkflowDefinitionService workflowService;

    public WorkflowController(WorkflowDefinitionService workflowService) {
        this.workflowService = workflowService;
    }

    @PostMapping
    public ResponseEntity<WorkflowResponse> register(@RequestBody WorkflowRequest request) {
        return ResponseEntity.status(HttpStatus.CREATED).body(workflowService.create(request));
    }

    @GetMapping
    public List<WorkflowResponse> list() {
        return workflowService.list();
    }

    @GetMapping("/{id}")
    public WorkflowResponse get(@PathVariable UUID id) {
        return workflowService.get(id);
    }

    @PutMapping("/{id}")
    public WorkflowResponse update(@PathVariable UUID id, @RequestBody WorkflowRequest request) {
        return workflowService.update(id, request);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> delete(@PathVariable UUID id) {
        workflowService.delete(id);
        return ResponseEntity.noContent().build();
    }
}
