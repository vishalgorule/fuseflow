package io.fuseflow.sdk.processor;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves the compile-time {@code @Workflow} validation: unknown activity references, cycles
 * and dangling dependencies fail the build with clear messages, while valid definitions
 * compile. Sources are compiled in-memory with {@code -proc:full} (JDK 25 disables implicit
 * annotation processing) and the SDK's processor discovered from the classpath.
 */
class FuseFlowWorkflowProcessorTest {

    @TempDir
    Path tempDir;

    private static final String VALID_SOURCE = """
            import io.fuseflow.sdk.annotation.Activity;
            import io.fuseflow.sdk.annotation.Step;
            import io.fuseflow.sdk.annotation.Workflow;
            import io.fuseflow.sdk.core.ActivityContext;

            class Activities {
                @Activity("actA") public String actA(ActivityContext ctx) { return null; }
                @Activity("actB") public String actB(ActivityContext ctx) { return null; }
            }

            @Workflow(name = "w", description = "desc")
            @Step(id = "a", activity = "actA")
            @Step(id = "b", activity = "actB", dependsOn = "a")
            class Wf {}
            """;

    private static final String UNKNOWN_ACTIVITY_SOURCE = """
            import io.fuseflow.sdk.annotation.Activity;
            import io.fuseflow.sdk.annotation.Step;
            import io.fuseflow.sdk.annotation.Workflow;
            import io.fuseflow.sdk.core.ActivityContext;

            class Activities {
                @Activity("actA") public String actA(ActivityContext ctx) { return null; }
            }

            @Workflow(name = "w")
            @Step(id = "a", activity = "actA")
            @Step(id = "b", activity = "actB")  // not declared above
            class Wf {}
            """;

    private static final String CYCLIC_SOURCE = """
            import io.fuseflow.sdk.annotation.Activity;
            import io.fuseflow.sdk.annotation.Step;
            import io.fuseflow.sdk.annotation.Workflow;
            import io.fuseflow.sdk.core.ActivityContext;

            class Activities {
                @Activity("actA") public String actA(ActivityContext ctx) { return null; }
                @Activity("actB") public String actB(ActivityContext ctx) { return null; }
            }

            @Workflow(name = "w")
            @Step(id = "a", activity = "actA", dependsOn = "b")
            @Step(id = "b", activity = "actB", dependsOn = "a")
            class Wf {}
            """;

    private static final String SELF_CONTAINED_SOURCE = """
            import io.fuseflow.sdk.annotation.Activity;
            import io.fuseflow.sdk.annotation.Workflow;
            import io.fuseflow.sdk.core.ActivityContext;
            import java.util.Map;

            @Workflow(name = "w", description = "desc")
            class Wf {
                @Activity(id = "validate")
                public Map<String, Object> validate(ActivityContext ctx) { return Map.of(); }
                @Activity(id = "ship", dependsOn = "validate")
                public Map<String, Object> ship(ActivityContext ctx) { return Map.of(); }
            }
            """;

    private static final String SELF_CONTAINED_DANGLING_SOURCE = """
            import io.fuseflow.sdk.annotation.Activity;
            import io.fuseflow.sdk.annotation.Workflow;
            import io.fuseflow.sdk.core.ActivityContext;
            import java.util.Map;

            @Workflow(name = "w")
            class Wf {
                @Activity(id = "a", dependsOn = "ghost")
                public Map<String, Object> a(ActivityContext ctx) { return Map.of(); }
            }
            """;

    private static final String SELF_CONTAINED_BAD_SIGNATURE_SOURCE = """
            import io.fuseflow.sdk.annotation.Activity;
            import io.fuseflow.sdk.annotation.Workflow;

            @Workflow(name = "w")
            class Wf {
                @Activity(id = "a")
                public String a(String input) { return input; }
            }
            """;

    @Test
    void compilesValidWorkflow() throws Exception {
        Compilation result = compile(Map.of("Wf.java", VALID_SOURCE));
        assertThat(result.success()).isTrue();
        assertThat(result.errorMessages()).isEmpty();
    }

    @Test
    void rejectsUnknownActivityAtCompileTime() throws Exception {
        Compilation result = compile(Map.of("Wf.java", UNKNOWN_ACTIVITY_SOURCE));
        assertThat(result.success()).isFalse();
        assertThat(result.errorMessages())
                .anyMatch(message -> message.contains("references activity 'actB'")
                        && message.contains("not declared via @Activity"));
    }

    @Test
    void rejectsCycleAtCompileTime() throws Exception {
        Compilation result = compile(Map.of("Wf.java", CYCLIC_SOURCE));
        assertThat(result.success()).isFalse();
        assertThat(result.errorMessages())
                .anyMatch(message -> message.contains("circular dependency detected"));
    }

    private static final String RETRY_POLICY_SOURCE = """
            import io.fuseflow.sdk.annotation.Activity;
            import io.fuseflow.sdk.annotation.Retry;
            import io.fuseflow.sdk.annotation.Step;
            import io.fuseflow.sdk.annotation.Workflow;
            import io.fuseflow.sdk.core.ActivityContext;

            class Activities {
                @Activity("actA") public String actA(ActivityContext ctx) { return null; }
                @Activity("actB") public String actB(ActivityContext ctx) { return null; }
            }

            @Workflow(name = "w", retry = @Retry(maxAttempts = 3, fixedDelaySeconds = 5, exponentialBackoff = true))
            @Step(id = "a", activity = "actA")
            @Step(id = "b", activity = "actB", dependsOn = "a", retry = @Retry(maxAttempts = 1))
            class Wf {}
            """;

    @Test
    void compilesWorkflowWithRetryPolicies() throws Exception {
        // Phase 7: @Retry on the workflow and on individual steps is valid annotation syntax
        // and must not trip the processor.
        Compilation result = compile(Map.of("Wf.java", RETRY_POLICY_SOURCE));
        assertThat(result.success()).isTrue();
        assertThat(result.errorMessages()).isEmpty();
    }

    @Test
    void compilesSelfContainedWorkflow() throws Exception {
        Compilation result = compile(Map.of("Wf.java", SELF_CONTAINED_SOURCE));
        assertThat(result.success()).isTrue();
        assertThat(result.errorMessages()).isEmpty();
    }

    @Test
    void rejectsDanglingDependencyInSelfContainedWorkflowAtCompileTime() throws Exception {
        Compilation result = compile(Map.of("Wf.java", SELF_CONTAINED_DANGLING_SOURCE));
        assertThat(result.success()).isFalse();
        assertThat(result.errorMessages())
                .anyMatch(message -> message.contains("dependency 'ghost' is not defined"));
    }

    @Test
    void rejectsBadActivitySignatureInSelfContainedWorkflowAtCompileTime() throws Exception {
        Compilation result = compile(Map.of("Wf.java", SELF_CONTAINED_BAD_SIGNATURE_SOURCE));
        assertThat(result.success()).isFalse();
        assertThat(result.errorMessages())
                .anyMatch(message -> message.contains("must take a single ActivityContext"));
    }

    // ---------------------------------------------------------------- helpers

    private Compilation compile(Map<String, String> sources) throws Exception {
        Path sourceDir = Files.createDirectories(tempDir.resolve("src"));
        Path outputDir = Files.createDirectories(tempDir.resolve("out"));
        List<Path> files = new ArrayList<>();
        for (Map.Entry<String, String> entry : sources.entrySet()) {
            Path file = sourceDir.resolve(entry.getKey());
            Files.writeString(file, entry.getValue());
            files.add(file);
        }

        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();
        try (StandardJavaFileManager fileManager =
                     compiler.getStandardFileManager(diagnostics, null, null)) {
            Iterable<? extends JavaFileObject> units = fileManager.getJavaFileObjectsFromPaths(files);
            List<String> options = List.of(
                    "-proc:full",
                    "-classpath", System.getProperty("java.class.path"),
                    "-d", outputDir.toString());
            Boolean success = compiler.getTask(null, fileManager, diagnostics, options, null, units).call();
            List<String> errors = diagnostics.getDiagnostics().stream()
                    .filter(d -> d.getKind() == Diagnostic.Kind.ERROR)
                    .map(d -> d.getMessage(null))
                    .toList();
            return new Compilation(Boolean.TRUE.equals(success), errors);
        }
    }

    private record Compilation(boolean success, List<String> errorMessages) {
    }
}
