package io.fuseflow.engine.controller;

import io.fuseflow.engine.retry.DeadLetterReader;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * Dead-letter inspection (Phase 7, plan §9 task 6): {@code GET /api/v1/dead-letters?limit=N}
 * returns the activities whose retries were exhausted or whose failure was non-retryable,
 * newest of the topic's earliest messages first.
 */
@RestController
@RequestMapping("/api/v1/dead-letters")
@ConditionalOnProperty(name = "fuseflow.engine.dispatch-mode", havingValue = "kafka", matchIfMissing = true)
public class DeadLetterController {

    private final DeadLetterReader deadLetterReader;

    public DeadLetterController(DeadLetterReader deadLetterReader) {
        this.deadLetterReader = deadLetterReader;
    }

    @GetMapping
    public List<Map<String, Object>> list(@RequestParam(defaultValue = "50") int limit) {
        return deadLetterReader.read(limit);
    }
}
