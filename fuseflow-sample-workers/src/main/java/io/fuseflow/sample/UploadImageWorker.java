package io.fuseflow.sample;

import io.fuseflow.sdk.annotation.Activity;
import io.fuseflow.sdk.core.ActivityContext;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.util.Map;

/** The {@code uploadImage} activity — gated so fleet instances can advertise subsets (Phase 5). */
@Component
@ConditionalOnProperty(name = "fuseflow.sample.enable-upload", havingValue = "true", matchIfMissing = true)
public class UploadImageWorker extends AbstractSampleWorker {

    public UploadImageWorker(ObjectMapper objectMapper) {
        super(objectMapper);
    }

    @Activity("uploadImage")
    public Map<String, Object> uploadImage(ActivityContext ctx) {
        return run("uploadImage", ctx);
    }
}
