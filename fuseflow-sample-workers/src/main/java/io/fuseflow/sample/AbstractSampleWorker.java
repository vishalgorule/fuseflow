package io.fuseflow.sample;

import io.fuseflow.sdk.core.ActivityContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import tools.jackson.databind.ObjectMapper;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Shared simulated execution for the per-activity sample workers (Phase 5 heterogeneous pool
 * demo). Concrete components extend this and expose their single activity via {@code @Activity};
 * the base class itself has no {@code @Activity} method, so it is never registered as a worker.
 */
abstract class AbstractSampleWorker {

    private static final Logger log = LoggerFactory.getLogger(AbstractSampleWorker.class);

    protected final ObjectMapper objectMapper;
    private final long activityDurationMs;

    protected AbstractSampleWorker(ObjectMapper objectMapper,
                                   @Value("${fuseflow.sample.activity-duration-ms:200}") long activityDurationMs) {
        this.objectMapper = objectMapper;
        this.activityDurationMs = activityDurationMs;
    }

    protected Map<String, Object> run(String activity, ActivityContext ctx) {
        log.info("Executing {} for task {} of execution {} (attempt {})",
                activity, ctx.taskId(), ctx.executionId(), ctx.attempt());
        try {
            Thread.sleep(activityDurationMs);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
        Map<String, Object> output = new LinkedHashMap<>();
        output.put("activity", activity);
        output.put("taskId", ctx.taskId());
        output.put("message", "ok");
        output.put("input", parse(ctx.input()));
        return output;
    }

    private Object parse(String input) {
        if (input == null) {
            return null;
        }
        try {
            return objectMapper.readTree(input);
        } catch (Exception ex) {
            return input;
        }
    }
}
