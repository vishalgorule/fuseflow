package io.fuseflow.sample;

import io.fuseflow.sdk.annotation.Activity;
import io.fuseflow.sdk.annotation.Workflow;
import io.fuseflow.sdk.core.ActivityContext;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.util.Map;

/**
 * The image-processing diamond (the same DAG the Phase 1/4 demo registers via the JSON API),
 * in the <b>self-contained</b> style (Phase 6): the {@code @Workflow} class's own
 * {@code @Activity} methods are the workflow's steps. The activity name is the method name,
 * {@code @Activity.id} is the task id in the DAG (defaults to the activity name) and
 * {@code @Activity.dependsOn} the edges — {@code download} → {@code resize} →
 * {@code watermark} + {@code compress} (parallel) → {@code upload} (fan-in join). The same
 * methods are registered as executable activities, so this bean both defines and implements
 * the workflow.
 *
 * <p>Gated as a whole by {@code fuseflow.sample.enable-image}: a fleet instance enables or
 * disables the whole workflow (its five activities) at once — workflow-level gating, as
 * opposed to the old per-activity worker classes (Phase 5 fleet heterogeneity is now expressed
 * per workflow; see {@code start-fleet-workers.sh}).
 */
@Component
@ConditionalOnProperty(name = "fuseflow.sample.enable-image", havingValue = "true", matchIfMissing = true)
@Workflow(name = "image-processing",
        description = "Diamond DAG defined with annotations (Phase 6)")
public class ImageProcessingWorkflow extends AbstractSampleWorker {

    public ImageProcessingWorkflow(ObjectMapper objectMapper) {
        super(objectMapper);
    }

    @Activity(id = "download")
    public Map<String, Object> downloadImage(ActivityContext ctx) {
        return run("downloadImage", ctx);
    }

    @Activity(id = "resize", dependsOn = "download")
    public Map<String, Object> resizeImage(ActivityContext ctx) {
        return run("resizeImage", ctx);
    }

    @Activity(id = "watermark", dependsOn = "resize")
    public Map<String, Object> watermarkImage(ActivityContext ctx) {
        return run("watermarkImage", ctx);
    }

    @Activity(id = "compress", dependsOn = "resize")
    public Map<String, Object> compressImage(ActivityContext ctx) {
        return run("compressImage", ctx);
    }

    @Activity(id = "upload", dependsOn = {"watermark", "compress"})
    public Map<String, Object> uploadImage(ActivityContext ctx) {
        return run("uploadImage", ctx);
    }
}
