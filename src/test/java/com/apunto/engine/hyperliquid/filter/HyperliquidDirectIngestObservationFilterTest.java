package com.apunto.engine.hyperliquid.filter;

import com.apunto.engine.hyperliquid.metric.HyperliquidIngestTransportMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Proxy;

import static org.junit.jupiter.api.Assertions.assertEquals;

class HyperliquidDirectIngestObservationFilterTest {

    @Test
    void directPostRecordsReceivedAndFinalHttpResult() throws ServletException, IOException {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        HyperliquidDirectIngestObservationFilter filter = new HyperliquidDirectIngestObservationFilter(
                new HyperliquidIngestTransportMetrics(registry));
        HttpServletRequest request = proxy(HttpServletRequest.class, "POST",
                "/internal/v1/hyperliquid/deltas", 0);
        HttpServletResponse response = proxy(HttpServletResponse.class, null, null, 202);
        FilterChain chain = (ignoredRequest, ignoredResponse) -> { };

        filter.doFilter(request, response, chain);

        assertEquals(1.0d, registry.get("signals.hyperliquid.direct_ingest.received.total")
                .counter().count());
        assertEquals(1.0d, registry.get("signals.hyperliquid.direct_ingest.http.total")
                .tag("result", "accepted")
                .tag("status", "202")
                .counter().count());
    }

    @SuppressWarnings("unchecked")
    private <T> T proxy(Class<T> type, String method, String uri, int status) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type},
                (ignored, invoked, args) -> switch (invoked.getName()) {
                    case "getMethod" -> method;
                    case "getRequestURI" -> uri;
                    case "getStatus" -> status;
                    default -> defaultValue(invoked.getReturnType());
                });
    }

    private Object defaultValue(Class<?> type) {
        if (!type.isPrimitive()) {
            return null;
        }
        if (type == boolean.class) {
            return false;
        }
        if (type == char.class) {
            return '\0';
        }
        return 0;
    }
}
