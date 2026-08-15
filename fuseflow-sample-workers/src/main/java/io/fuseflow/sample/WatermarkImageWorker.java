package io.fuseflow.sample;

import io.fuseflow.sdk.annotation.Activity;
import io.fuseflow.sdk.core.ActivityContext;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.util.Map;

/** The {@code watermarkImage} activity — gated so fleet instances can advertise subsets (Phase 5). */
@Component
@ConditionalOnProperty(name = "fuseflow.sample.enable-watermark", havingValue = "true", matchIfMissing = true)
public class WatermarkImageWorker extends AbstractSampleWorker {

    public WatermarkImageWorker(ObjectMapper objectMapper) {
        super(objectMapper);
    }

    @Activity("watermarkImage")
    public Map<String, Object> watermarkImage(ActivityContext ctx) {
        return run("watermarkImage", ctx);
    }
}
