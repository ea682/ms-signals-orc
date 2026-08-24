package com.apunto.engine.outbox.jobs;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Component
public class MetricHandoffObservability {

    private static final String STALE_PROCESSING_SQL = """
            SELECT count(*)
            FROM futuros_operaciones.hyperliquid_direct_ingest_dedupe
            WHERE status = 'PROCESSING'
              AND lease_until IS NOT NULL
              AND lease_until < now()
            """;

    private static final String OUTBOX_BACKLOG_SQL = """
            SELECT count(*) AS pending_count,
                   COALESCE(
                       floor(extract(epoch FROM (now() - min(created_at))))::bigint,
                       0
                   ) AS oldest_age_seconds
            FROM futuros_operaciones.metric_event_outbox
            WHERE published_at IS NULL
            """;

    private final JdbcTemplate jdbcTemplate;
    private final MeterRegistry meterRegistry;
    private final AtomicLong staleProcessing = new AtomicLong();
    private final AtomicLong pendingOutbox = new AtomicLong();
    private final AtomicLong oldestPendingAgeSeconds = new AtomicLong();
    private final AtomicLong lastSuccessfulRefreshEpochSeconds = new AtomicLong();

    @Value("${metric.outbox.observability.enabled:${metric.outbox.publisher.enabled:false}}")
    private boolean enabled;

    public MetricHandoffObservability(
            JdbcTemplate jdbcTemplate,
            MeterRegistry meterRegistry
    ) {
        this.jdbcTemplate = jdbcTemplate;
        this.meterRegistry = meterRegistry;
        registerMetrics();
    }

    @Scheduled(fixedDelayString = "${metric.outbox.observability.poll-ms:10000}")
    public void refresh() {
        if (!enabled) {
            return;
        }
        try {
            Long stale = jdbcTemplate.queryForObject(
                    STALE_PROCESSING_SQL,
                    Long.class);
            Map<String, Object> backlog = jdbcTemplate.queryForMap(
                    OUTBOX_BACKLOG_SQL);
            staleProcessing.set(nonNegative(stale));
            pendingOutbox.set(nonNegative(backlog.get("pending_count")));
            oldestPendingAgeSeconds.set(nonNegative(
                    backlog.get("oldest_age_seconds")));
            lastSuccessfulRefreshEpochSeconds.set(
                    Instant.now().getEpochSecond());
            meterRegistry.counter(
                    "signals.metric_outbox.observation.refresh",
                    "result", "success").increment();
        } catch (DataAccessException ex) {
            meterRegistry.counter(
                    "signals.metric_outbox.observation.refresh",
                    "result", "failure").increment();
            log.warn("event=metric_outbox.observation_failed errClass={} errMsg=\"{}\"",
                    ex.getClass().getSimpleName(), safe(ex.getMessage()));
        }
    }

    private void registerMetrics() {
        Gauge.builder(
                        "signals.hyperliquid.direct_ingest.processing.stale",
                        staleProcessing,
                        AtomicLong::get)
                .description("Expired durable Hyperliquid PROCESSING claims")
                .register(meterRegistry);
        Gauge.builder(
                        "signals.metric_outbox.pending",
                        pendingOutbox,
                        AtomicLong::get)
                .description("Durable metric outbox rows awaiting publication")
                .register(meterRegistry);
        Gauge.builder(
                        "signals.metric_outbox.oldest.pending.age.seconds",
                        oldestPendingAgeSeconds,
                        AtomicLong::get)
                .description("Age in seconds of the oldest unpublished outbox row")
                .register(meterRegistry);
        Gauge.builder(
                        "signals.metric_outbox.observation.last.success.epoch.seconds",
                        lastSuccessfulRefreshEpochSeconds,
                        AtomicLong::get)
                .description("Epoch second of the latest successful handoff observation")
                .register(meterRegistry);
    }

    private long nonNegative(Object value) {
        if (value instanceof Number number) {
            return Math.max(0L, number.longValue());
        }
        return 0L;
    }

    private String safe(String value) {
        return value == null
                ? "null"
                : value.replace('\n', '_').replace('\r', '_');
    }
}
