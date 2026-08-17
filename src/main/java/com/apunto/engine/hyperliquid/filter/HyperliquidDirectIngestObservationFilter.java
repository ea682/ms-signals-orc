package com.apunto.engine.hyperliquid.filter;

import com.apunto.engine.hyperliquid.metric.HyperliquidIngestTransportMetrics;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

@Component
@RequiredArgsConstructor
public class HyperliquidDirectIngestObservationFilter extends OncePerRequestFilter {

    private static final String DIRECT_PATH = "/internal/v1/hyperliquid/deltas";

    private final HyperliquidIngestTransportMetrics metrics;

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        return !"POST".equalsIgnoreCase(request.getMethod())
                || !DIRECT_PATH.equals(request.getRequestURI());
    }

    @Override
    protected void doFilterInternal(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain filterChain
    ) throws ServletException, IOException {
        metrics.directReceived();
        boolean completed = false;
        try {
            filterChain.doFilter(request, response);
            completed = true;
        } finally {
            metrics.directCompleted(completed ? response.getStatus() : 500);
        }
    }
}
