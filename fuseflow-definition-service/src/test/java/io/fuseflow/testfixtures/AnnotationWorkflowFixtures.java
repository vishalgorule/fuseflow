package io.fuseflow.testfixtures;

import io.fuseflow.common.validation.DagValidator;
import io.fuseflow.sdk.annotation.Activity;
import io.fuseflow.sdk.annotation.Step;
import io.fuseflow.sdk.annotation.Workflow;
import io.fuseflow.sdk.core.ActivityContext;
import io.fuseflow.sdk.runtime.ActivityRegistry;
import io.fuseflow.sdk.runtime.WorkflowRegistry;
import io.fuseflow.sdk.runtime.WorkflowScanner;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Test fixtures for {@code AnnotationWorkflowRegistrationIntegrationTest}: a standalone Spring
 * context (exactly the shape of a worker project) that scans one annotation-defined workflow.
 * Lives in {@code io.fuseflow.testfixtures} so the definition service's component scan
 * ({@code io.fuseflow.definition}) never sees it.
 */
@Configuration
public class AnnotationWorkflowFixtures {

    @Bean
    public WorkflowRegistry workflowRegistry() {
        return new WorkflowRegistry();
    }

    @Bean
    public DagValidator dagValidator() {
        return new DagValidator();
    }

    @Bean
    public WorkflowScanner workflowScanner(ApplicationContext context, WorkflowRegistry registry,
                                           DagValidator dagValidator,
                                           ObjectProvider<ActivityRegistry> activityRegistry) {
        return new WorkflowScanner(context, registry, dagValidator, activityRegistry);
    }

    @Bean
    public AnnotationDiamond annotationDiamond() {
        return new AnnotationDiamond();
    }

    /**
     * The annotation-defined diamond (same shape as the JSON demo). {@link SampleActivities}
     * declares every referenced activity via {@code @Activity} in the same compilation — the
     * same way a real worker project does (and what the compile-time processor requires).
     */
    @Workflow(name = "annotation-diamond", description = "Phase 6 annotation-defined diamond")
    @Step(id = "download", activity = "downloadImage")
    @Step(id = "resize", activity = "resizeImage", dependsOn = "download")
    @Step(id = "watermark", activity = "watermarkImage", dependsOn = "resize")
    @Step(id = "compress", activity = "compressImage", dependsOn = "resize")
    @Step(id = "upload", activity = "uploadImage", dependsOn = {"watermark", "compress"})
    public static class AnnotationDiamond {
    }

    @SuppressWarnings("unused")
    public static class SampleActivities {
        @Activity("downloadImage")
        public String download(ActivityContext ctx) {
            return null;
        }

        @Activity("resizeImage")
        public String resize(ActivityContext ctx) {
            return null;
        }

        @Activity("watermarkImage")
        public String watermark(ActivityContext ctx) {
            return null;
        }

        @Activity("compressImage")
        public String compress(ActivityContext ctx) {
            return null;
        }

        @Activity("uploadImage")
        public String upload(ActivityContext ctx) {
            return null;
        }
    }
}
