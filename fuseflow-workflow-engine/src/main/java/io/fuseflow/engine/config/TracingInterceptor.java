package io.fuseflow.engine.config;

import io.fuseflow.common.correlation.CorrelationId;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.MDC;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.UUID;

/**
 * Phase 9 observability: HTTP tracing interceptor.
 * 
 * <p>Extracts or generates a correlation ID from the {@code X-Correlation-Id} header and
 * places it in the SLF4J MDC so every log line produced during this request includes it.
 * Also adds the execution ID to MDC when present as a path variable for log correlation.
 * 
 * <p>This runs early (highest priority) so all subsequent filters and controllers benefit.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class TracingInterceptor extends OncePerRequestFilter {

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
                                    FilterChain filterChain) throws ServletException, IOException {
        try {
            // Extract or generate correlation ID
            String correlationId = request.getHeader(CorrelationId.HEADER);
            if (correlationId == null || correlationId.isBlank()) {
                correlationId = UUID.randomUUID().toString();
            }
            
            // Set in MDC for structured logging
            MDC.put("correlationId", correlationId);
            CorrelationId.set(correlationId);
            
            // Extract execution ID from path if present (e.g., /api/v1/executions/{id})
            String requestURI = request.getRequestURI();
            if (requestURI.contains("/executions/")) {
                String[] parts = requestURI.split("/");
                for (int i = 0; i < parts.length - 1; i++) {
                    if ("executions".equals(parts[i]) && i + 1 < parts.length) {
                        String executionId = parts[i + 1];
                        // Only set if it looks like a UUID
                        if (executionId.matches("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")) {
                            MDC.put("executionId", executionId);
                        }
                        break;
                    }
                }
            }
            
            // Propagate correlation ID in response header
            response.setHeader(CorrelationId.HEADER, correlationId);
            
            filterChain.doFilter(request, response);
        } finally {
            MDC.clear();
        }
    }
    
    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        // Don't filter actuator endpoints to avoid noise
        String path = request.getRequestURI();
        return path.startsWith("/actuator");
    }
}
