package com.apunto.engine.hyperliquid;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertTrue;

class SignalsHotHandoffObservabilityArchitectureTest {

    @Test
    void hotHandoffAlertsReferenceMetricsImplementedBySignals() throws Exception {
        String alerts = Files.readString(Path.of(
                "src/main/resources/monitoring/signals-hot-handoff-alerts.yml"));
        String backlogSource = Files.readString(Path.of(
                "src/main/java/com/apunto/engine/outbox/jobs/MetricHandoffObservability.java"));
        String publisherSource = Files.readString(Path.of(
                "src/main/java/com/apunto/engine/outbox/jobs/MetricMovementOutboxPublisher.java"));
        String ingestSource = Files.readString(Path.of(
                "src/main/java/com/apunto/engine/hyperliquid/service/impl/HyperliquidDirectDeltaIngestServiceImpl.java"));

        assertContract(alerts, backlogSource,
                "signals_metric_outbox_pending",
                "signals.metric_outbox.pending");
        assertContract(alerts, backlogSource,
                "signals_metric_outbox_oldest_pending_age_seconds",
                "signals.metric_outbox.oldest.pending.age.seconds");
        assertContract(alerts, backlogSource,
                "signals_hyperliquid_direct_ingest_processing_stale",
                "signals.hyperliquid.direct_ingest.processing.stale");
        assertContract(alerts, publisherSource,
                "signals_metric_outbox_publish_total",
                "signals.metric_outbox.publish");
        assertContract(alerts, ingestSource,
                "signals_hyperliquid_user_fill_ingress_total",
                "signals.hyperliquid.user_fill.ingress.total");
        assertContract(alerts, ingestSource,
                "signals_hyperliquid_user_fill_durable_total",
                "signals.hyperliquid.user_fill.durable.total");
        assertContract(alerts, Files.readString(Path.of(
                        "src/main/java/com/apunto/engine/service/impl/OperationMovementEventServiceImpl.java")),
                "signals_hyperliquid_user_fill_economic_total",
                "signals.hyperliquid.user_fill.economic.total");
        assertContract(alerts, Files.readString(Path.of(
                        "src/main/java/com/apunto/engine/hyperliquid/service/impl/HyperliquidDirectIngestIdempotencyGuard.java")),
                "duplicate_noop_total",
                "duplicate_noop_total");
        assertTrue(alerts.contains("SignalsUpButEconomicHandoffStalled"));
        assertTrue(alerts.contains("SignalsUserFillWithoutEligibleEconomics"));
        assertTrue(alerts.contains("SOURCE_LEDGER_DIVERGENCE"));
        assertTrue(alerts.contains("signals_movement_event_published_total"));
    }

    private void assertContract(
            String alerts,
            String source,
            String prometheusMetric,
            String micrometerMetric
    ) {
        assertTrue(alerts.contains(prometheusMetric),
                () -> "missing alert metric " + prometheusMetric);
        assertTrue(source.contains(micrometerMetric),
                () -> "missing implemented metric " + micrometerMetric);
    }
}
