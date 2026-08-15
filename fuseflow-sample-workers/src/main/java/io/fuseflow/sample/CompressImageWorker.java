package io.fuseflow.sample;

import io.fuseflow.sdk.annotation.Activity;
import io.fuseflow.sdk.core.ActivityContext;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.util.Map;

/** The {@code compressImage} activity — gated so fleet instances can advertise subsets (Phase 5). */
@Component
@ConditionalOnProperty(name = "fuseflow.sample.enable-compress", havingValue = "true", matchIfMissing = true)
public class CompressImageWorker extends AbstractSampleWorker {

    public CompressImageWorker(ObjectMapper objectMapper) {
        super(objectMapper);
    }

    @Activity("compressImage")
    public Map<String, Object> compressImage(ActivityContext ctx) {
        return run("compressImage", ctx);
    }
}
