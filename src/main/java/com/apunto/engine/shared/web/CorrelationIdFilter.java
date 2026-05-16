package com.apunto.engine.shared.web;

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

@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class CorrelationIdFilter extends OncePerRequestFilter {

    public static final String TRACE_ID_HEADER = "X-Trace-Id";
    private static final int MAX_TRACE_ID_LENGTH = 128;

    @Override
    protected void doFilterInternal(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain filterChain
    ) throws ServletException, IOException {
        String traceId = resolveTraceId(request);
        MDC.put("traceId", traceId);
        MDC.put("httpMethod", request.getMethod());
        MDC.put("httpPath", safe(request.getRequestURI()));
        response.setHeader(TRACE_ID_HEADER, traceId);
        try {
            filterChain.doFilter(request, response);
        } finally {
            MDC.remove("httpPath");
            MDC.remove("httpMethod");
            MDC.remove("traceId");
        }
    }

    private String resolveTraceId(HttpServletRequest request) {
        String incoming = request.getHeader(TRACE_ID_HEADER);
        if (incoming == null || incoming.isBlank()) {
            incoming = request.getHeader("X-Correlation-Id");
        }
        if (incoming == null || incoming.isBlank()) {
            return UUID.randomUUID().toString();
        }
        String clean = safe(incoming.trim());
        return clean.length() > MAX_TRACE_ID_LENGTH ? clean.substring(0, MAX_TRACE_ID_LENGTH) : clean;
    }

    private String safe(String value) {
        if (value == null) {
            return "NA";
        }
        return value
                .replace('\n', ' ')
                .replace('\r', ' ')
                .replace('\t', ' ')
                .replace('"', '\'');
    }
}
