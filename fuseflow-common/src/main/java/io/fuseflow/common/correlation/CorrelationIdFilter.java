package io.fuseflow.common.correlation;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.MDC;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

/**
 * Servlet filter that ensures every inbound request has a correlation id:
 * reads {@code X-Correlation-Id} if present, otherwise generates a UUID,
 * stores it in {@link CorrelationId} and the MDC, and echoes it back on the
 * response header so downstream services can propagate it.
 */
public class CorrelationIdFilter extends OncePerRequestFilter {

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {
        String inbound = request.getHeader(CorrelationId.HEADER);
        String correlationId = (inbound != null && !inbound.isBlank()) ? inbound : java.util.UUID.randomUUID().toString();

        CorrelationId.set(correlationId);
        MDC.put(CorrelationId.MDC_KEY, correlationId);
        response.setHeader(CorrelationId.HEADER, correlationId);
        try {
            filterChain.doFilter(request, response);
        } finally {
            MDC.remove(CorrelationId.MDC_KEY);
            CorrelationId.clear();
        }
    }
}
