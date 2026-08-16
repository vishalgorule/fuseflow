package io.fuseflow.sdk.client;

import com.sun.net.httpserver.HttpServer;
import io.fuseflow.common.dto.WorkflowRequest;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class DefinitionClientTest {

    private static final WorkflowRequest DIAMOND = new WorkflowRequest("image-processing", "desc",
            List.of(
                    new WorkflowRequest.Task("download", "downloadImage", null),
                    new WorkflowRequest.Task("resize", "resizeImage", List.of("download")),
                    new WorkflowRequest.Task("upload", "uploadImage", List.of("resize"))));

    private static final String EXISTING_RESPONSE = """
            {"id":"11111111-1111-1111-1111-111111111111","name":"image-processing","description":"old",
             "tasks":[{"id":"download","activity":"downloadImage","dependsOn":[]},
                      {"id":"resize","activity":"resizeImage","dependsOn":["download"]},
                      {"id":"upload","activity":"uploadImage","dependsOn":["resize"]}],
             "version":2,"createdAt":"2026-08-01T00:00:00Z","updatedAt":"2026-08-01T00:00:00Z"}
            """;

    private static final String UPDATED_RESPONSE = """
            {"id":"11111111-1111-1111-1111-111111111111","name":"image-processing","description":"desc",
             "tasks":[{"id":"download","activity":"downloadImage","dependsOn":[]},
                      {"id":"resize","activity":"resizeImage","dependsOn":["download"]},
                      {"id":"upload","activity":"uploadImage","dependsOn":["resize"]}],
             "version":3,"createdAt":"2026-08-01T00:00:00Z","updatedAt":"2026-08-02T00:00:00Z"}
            """;

    /** Request log: "METHOD path". */
    private final List<String> requests = new ArrayList<>();

    @Test
    void registersNewWorkflowWithPost() throws Exception {
        HttpServer server = startServer(exchange -> {
            requests.add(exchange.getRequestMethod() + " " + exchange.getRequestURI().getPath());
            respond(exchange, 201, EXISTING_RESPONSE);
        });
        try {
            DefinitionClient client = clientFor(server);
            var response = client.register(DIAMOND);
            assertThat(response.id()).isNotNull();
            assertThat(response.name()).isEqualTo("image-processing");
            assertThat(requests).containsExactly("POST /api/v1/workflows");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void identicalDagIsANoOpOnNameConflict() throws Exception {
        AtomicReference<Boolean> putCalled = new AtomicReference<>(false);
        HttpServer server = startServer(exchange -> {
            requests.add(exchange.getRequestMethod() + " " + exchange.getRequestURI().getPath());
            if ("POST".equals(exchange.getRequestMethod())) {
                respond(exchange, 409, "{}"); // name conflict
            } else if ("GET".equals(exchange.getRequestMethod())) {
                respond(exchange, 200, "[" + EXISTING_RESPONSE + "]");
            } else if ("PUT".equals(exchange.getRequestMethod())) {
                putCalled.set(true);
                respond(exchange, 200, UPDATED_RESPONSE);
            }
        });
        try {
            DefinitionClient client = clientFor(server);
            var response = client.register(DIAMOND);
            // No-op: the existing definition (identical DAG) is returned as-is, no PUT.
            assertThat(response.version()).isEqualTo(2);
            assertThat(putCalled.get()).isFalse();
            assertThat(requests).contains("POST /api/v1/workflows", "GET /api/v1/workflows");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void changedDagReplacesExistingDefinitionOnNameConflict() throws Exception {
        // The persisted definition differs (resize depends on ghost) → the client PUTs.
        String changedExisting = EXISTING_RESPONSE.replace("\"dependsOn\":[\"resize\"]",
                "\"dependsOn\":[\"ghost\"]");
        HttpServer server = startServer(exchange -> {
            requests.add(exchange.getRequestMethod() + " " + exchange.getRequestURI().getPath());
            if ("POST".equals(exchange.getRequestMethod())) {
                respond(exchange, 409, "{}");
            } else if ("GET".equals(exchange.getRequestMethod())) {
                respond(exchange, 200, "[" + changedExisting + "]");
            } else if ("PUT".equals(exchange.getRequestMethod())) {
                respond(exchange, 200, UPDATED_RESPONSE);
            }
        });
        try {
            DefinitionClient client = clientFor(server);
            var response = client.register(DIAMOND);
            assertThat(response.version()).isEqualTo(3);
            assertThat(requests).contains(
                    "POST /api/v1/workflows",
                    "GET /api/v1/workflows",
                    "PUT /api/v1/workflows/11111111-1111-1111-1111-111111111111");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void retriesCreateWhenConflictHasNoExistingDefinition() throws Exception {
        AtomicReference<Integer> postCount = new AtomicReference<>(0);
        HttpServer server = startServer(exchange -> {
            requests.add(exchange.getRequestMethod() + " " + exchange.getRequestURI().getPath());
            if ("POST".equals(exchange.getRequestMethod())) {
                if (postCount.updateAndGet(n -> n + 1) == 1) {
                    respond(exchange, 409, "{}");
                } else {
                    respond(exchange, 201, EXISTING_RESPONSE);
                }
            } else if ("GET".equals(exchange.getRequestMethod())) {
                respond(exchange, 200, "[]");
            }
        });
        try {
            DefinitionClient client = clientFor(server);
            var response = client.register(DIAMOND);
            assertThat(response.id()).isNotNull();
            assertThat(requests).containsExactly("POST /api/v1/workflows", "GET /api/v1/workflows",
                    "POST /api/v1/workflows");
        } finally {
            server.stop(0);
        }
    }

    // ---------------------------------------------------------------- helpers

    private static DefinitionClient clientFor(HttpServer server) {
        return new DefinitionClient("http://127.0.0.1:" + server.getAddress().getPort());
    }

    private HttpServer startServer(Handler handler) throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            try {
                handler.handle(exchange);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        });
        server.start();
        return server;
    }

    private static void respond(com.sun.net.httpserver.HttpExchange exchange, int status, String body) {
        try {
            byte[] bytes = body.getBytes();
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(status, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @FunctionalInterface
    interface Handler {
        void handle(com.sun.net.httpserver.HttpExchange exchange) throws Exception;
    }
}
