package io.fuseflow.common.correlation;

import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class CorrelationIdFilterTest {

    private final CorrelationIdFilter filter = new CorrelationIdFilter();

    @AfterEach
    void cleanUp() {
        CorrelationId.clear();
        MDC.clear();
    }

    /** Runs the filter and captures the correlation id + MDC as seen inside the chain (i.e. mid-request). */
    private String runAndCapture(MockHttpServletRequest request, MockHttpServletResponse response) throws Exception {
        AtomicReference<String> captured = new AtomicReference<>();
        AtomicReference<String> capturedMdc = new AtomicReference<>();
        MockFilterChain chain = new MockFilterChain() {
            @Override
            public void doFilter(ServletRequest req, ServletResponse res) {
                captured.set(CorrelationId.get());
                capturedMdc.set(MDC.get(CorrelationId.MDC_KEY));
            }
        };
        filter.doFilter(request, response, chain);
        assertThat(capturedMdc.get()).isEqualTo(captured.get());
        return captured.get();
    }

    @Test
    void propagatesInboundCorrelationId() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.addHeader(CorrelationId.HEADER, "inbound-123");
        MockHttpServletResponse response = new MockHttpServletResponse();

        String captured = runAndCapture(request, response);

        assertThat(captured).isEqualTo("inbound-123");
        assertThat(response.getHeader(CorrelationId.HEADER)).isEqualTo("inbound-123");
    }

    @Test
    void generatesCorrelationIdWhenAbsent() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest();
        MockHttpServletResponse response = new MockHttpServletResponse();

        String captured = runAndCapture(request, response);

        assertThat(captured).isNotBlank();
        assertThat(response.getHeader(CorrelationId.HEADER)).isEqualTo(captured);
    }

    @Test
    void clearsMdcAndThreadLocalAfterRequest() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.addHeader(CorrelationId.HEADER, "cleanup-456");

        runAndCapture(request, new MockHttpServletResponse());

        assertThat(MDC.get(CorrelationId.MDC_KEY)).isNull();
        assertThat(CorrelationId.get()).isNull();
    }
}
