package io.fuseflow.sample;

import io.fuseflow.sdk.annotation.Activity;
import io.fuseflow.sdk.core.ActivityContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Sample SDK worker (Phase 4): implements the five activities of the demo diamond DAG
 * (download → resize → watermark/compress → upload), each simulated with a short delay and a
 * small JSON output — the same shape the engine's history API shows. The SDK auto-configuration
 * scans this bean, registers the activities with the worker registry, and dispatches Kafka
 * messages here.
 */
@Component
public class ImageProcessingWorker {

    private static final Logger log = LoggerFactory.getLogger(ImageProcessingWorker.class);

    private final ObjectMapper objectMapper;

    public ImageProcessingWorker(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Activity("downloadImage")
    public Map<String, Object> downloadImage(ActivityContext ctx) {
        return run("downloadImage", ctx);
    }

    @Activity("resizeImage")
    public Map<String, Object> resizeImage(ActivityContext ctx) {
        return run("resizeImage", ctx);
    }

    @Activity("watermarkImage")
    public Map<String, Object> watermarkImage(ActivityContext ctx) {
        return run("watermarkImage", ctx);
    }

    @Activity("compressImage")
    public Map<String, Object> compressImage(ActivityContext ctx) {
        return run("compressImage", ctx);
    }

    @Activity("uploadImage")
    public Map<String, Object> uploadImage(ActivityContext ctx) {
        return run("uploadImage", ctx);
    }

    private Map<String, Object> run(String activity, ActivityContext ctx) {
        log.info("Executing {} for task {} of execution {} (attempt {})",
                activity, ctx.taskId(), ctx.executionId(), ctx.attempt());
        try {
            Thread.sleep(200);
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
