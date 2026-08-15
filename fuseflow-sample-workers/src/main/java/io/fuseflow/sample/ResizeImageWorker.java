package io.fuseflow.sample;

import io.fuseflow.sdk.annotation.Activity;
import io.fuseflow.sdk.core.ActivityContext;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.util.Map;

/** The {@code resizeImage} activity — gated so fleet instances can advertise subsets (Phase 5). */
@Component
@ConditionalOnProperty(name = "fuseflow.sample.enable-resize", havingValue = "true", matchIfMissing = true)
public class ResizeImageWorker extends AbstractSampleWorker {

    public ResizeImageWorker(ObjectMapper objectMapper) {
        super(objectMapper);
    }

    @Activity("resizeImage")
    public Map<String, Object> resizeImage(ActivityContext ctx) {
        return run("resizeImage", ctx);
    }
}
