package io.fuseflow.sample;

import io.fuseflow.sdk.annotation.Activity;
import io.fuseflow.sdk.core.ActivityContext;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.util.Map;

/** The {@code downloadImage} activity — gated so fleet instances can advertise subsets (Phase 5). */
@Component
@ConditionalOnProperty(name = "fuseflow.sample.enable-download", havingValue = "true", matchIfMissing = true)
public class DownloadImageWorker extends AbstractSampleWorker {

    public DownloadImageWorker(ObjectMapper objectMapper) {
        super(objectMapper);
    }

    @Activity("downloadImage")
    public Map<String, Object> downloadImage(ActivityContext ctx) {
        return run("downloadImage", ctx);
    }
}
