package io.fuseflow.sdk.runtime;

import io.fuseflow.sdk.annotation.Activity;
import io.fuseflow.sdk.core.ActivityContext;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ActivityScannerTest {

    static class SampleWorker {

        @Activity("downloadImage")
        public Map<String, Object> download(ActivityContext ctx) {
            return Map.of("taskId", ctx.taskId());
        }

        @Activity("uploadImage")
        public String upload(ActivityContext ctx) {
            return "{\"ok\":true}";
        }
    }

    @Configuration
    static class TestConfig {

        @Bean
        SampleWorker sampleWorker() {
            return new SampleWorker();
        }

        @Bean
        ActivityRegistry activityRegistry() {
            return new ActivityRegistry();
        }

        @Bean
        ActivityScanner activityScanner(ApplicationContext context, ActivityRegistry registry) {
            return new ActivityScanner(context, registry);
        }
    }

    @Test
    void scansActivityMethodsIntoRegistry() throws Exception {
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(TestConfig.class)) {
            ActivityRegistry registry = context.getBean(ActivityRegistry.class);

            assertThat(registry.names()).containsExactly("downloadImage", "uploadImage");

            Object output = registry.execute("downloadImage",
                    new ActivityContext(UUID.randomUUID(), "a", "downloadImage", 1, null));
            assertThat(output).isEqualTo(Map.of("taskId", "a"));

            String json = (String) registry.execute("uploadImage",
                    new ActivityContext(UUID.randomUUID(), "b", "uploadImage", 1, null));
            assertThat(json).isEqualTo("{\"ok\":true}");
        }
    }

    @Test
    void derivesActivityNameFromMethodNameWhenValueBlank() throws Exception {
        // Phase 6: blank @Activity value = activity name is the method name (Temporal-style).
        try (AnnotationConfigApplicationContext context =
                     new AnnotationConfigApplicationContext(MethodNameConfig.class)) {
            ActivityRegistry registry = context.getBean(ActivityRegistry.class);
            assertThat(registry.names()).containsExactly("refundOrder");
            assertThat(registry.supports("refundOrder")).isTrue();
        }
    }

    @Test
    void rejectsMethodsWithWrongSignature() {
        // The scanner's failure surfaces during context refresh (propagated directly).
        assertThatThrownBy(() -> new AnnotationConfigApplicationContext(BadConfig.class))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("must take a single ActivityContext");
    }

    static class MethodNameWorker {

        @Activity
        public Map<String, Object> refundOrder(ActivityContext ctx) {
            return Map.of("refunded", true);
        }
    }

    @Configuration
    static class MethodNameConfig {

        @Bean
        MethodNameWorker methodNameWorker() {
            return new MethodNameWorker();
        }

        @Bean
        ActivityRegistry activityRegistry() {
            return new ActivityRegistry();
        }

        @Bean
        ActivityScanner activityScanner(ApplicationContext context, ActivityRegistry registry) {
            return new ActivityScanner(context, registry);
        }
    }

    @Configuration
    static class BadConfig {

        @Bean
        ActivityRegistry activityRegistry() {
            return new ActivityRegistry();
        }

        @Bean
        ActivityScanner activityScanner(ApplicationContext context, ActivityRegistry registry) {
            return new ActivityScanner(context, registry);
        }

        @SuppressWarnings("unused")
        @Bean
        Object badWorker() {
            return new Object() {
                @Activity("broken")
                public String broken() { // no ActivityContext parameter
                    return "x";
                }
            };
        }
    }
}
