package com.apunto.engine.hyperliquid.metric;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class HyperliquidIngestTransportMetrics {

    private final MeterRegistry registry;

    public void directReceived() {
        registry.counter("signals.hyperliquid.direct_ingest.received.total").increment();
    }

    public void directCompleted(int status) {
        registry.counter(
                "signals.hyperliquid.direct_ingest.http.total",
                "result", status >= 200 && status < 300 ? "accepted" : "rejected",
                "status", Integer.toString(status)
        ).increment();
    }

    public void kafkaReceived() {
        registry.counter("signals.hyperliquid.kafka_ingest.received.total").increment();
    }

    public void kafkaAccepted(boolean duplicate) {
        registry.counter(
                "signals.hyperliquid.kafka_ingest.accepted.total",
                "result", duplicate ? "duplicate" : "accepted"
        ).increment();
    }

    public void kafkaFailed(String reason) {
        registry.counter(
                "signals.hyperliquid.kafka_ingest.failed.total",
                "reason", reason
        ).increment();
    }
}
