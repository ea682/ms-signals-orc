package com.apunto.engine.service.copy.observability;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Component
public class CopyDispatchOperationalMetrics {

    private final JdbcTemplate jdbcTemplate;
    private final AtomicLong pending = new AtomicLong();
    private final AtomicLong ambiguous = new AtomicLong();
    private final AtomicLong persistencePending = new AtomicLong();
    private final AtomicLong manualReview = new AtomicLong();
    private final AtomicLong oldestPendingAge = new AtomicLong();
    private final AtomicReference<Double> reservedMargin = new AtomicReference<>(0.0);
    private final AtomicLong openOrReservedPositions = new AtomicLong();
    private final AtomicLong reconciliationBacklog = new AtomicLong();

    @Value("${copy.observability.dispatch-gauges.enabled:true}")
    private boolean enabled;

    public CopyDispatchOperationalMetrics(JdbcTemplate jdbcTemplate, MeterRegistry registry) {
        this.jdbcTemplate = jdbcTemplate;
        Gauge.builder("pending_intents", pending, AtomicLong::doubleValue).register(registry);
        Gauge.builder("ambiguous_intents", ambiguous, AtomicLong::doubleValue).register(registry);
        Gauge.builder("persistence_pending_intents", persistencePending, AtomicLong::doubleValue).register(registry);
        Gauge.builder("manual_review_intents", manualReview, AtomicLong::doubleValue).register(registry);
        Gauge.builder("oldest_pending_age", oldestPendingAge, AtomicLong::doubleValue).register(registry);
        Gauge.builder("reserved_margin", reservedMargin, value -> value.get()).register(registry);
        Gauge.builder("open_or_reserved_positions", openOrReservedPositions, AtomicLong::doubleValue).register(registry);
        Gauge.builder("reconciliation_backlog", reconciliationBacklog, AtomicLong::doubleValue).register(registry);
    }

    @Scheduled(initialDelayString = "${copy.observability.dispatch-gauges.initial-delay-ms:15000}",
            fixedDelayString = "${copy.observability.dispatch-gauges.refresh-ms:15000}")
    public void refresh() {
        if (!enabled) return;
        try {
            Map<String, Object> row = jdbcTemplate.queryForMap("""
                    with intents as (
                        select
                            count(*) filter (where reservation_status = 'PENDING') as pending_intents,
                            count(*) filter (where status = 'RECONCILING') as ambiguous_intents,
                            count(*) filter (where status = 'PERSISTENCE_PENDING') as persistence_pending_intents,
                            count(*) filter (where status = 'MANUAL_REVIEW') as manual_review_intents,
                            coalesce(extract(epoch from (now() - min(created_at)
                                filter (where reservation_status = 'PENDING'))), 0) as oldest_pending_age,
                            coalesce(sum(requested_margin_usd)
                                filter (where reservation_status = 'PENDING'), 0) as reserved_margin,
                            coalesce(sum(reserved_position_count)
                                filter (where reservation_status = 'PENDING'), 0) as reserved_positions,
                            count(*) filter (where status in ('RECONCILING', 'PERSISTENCE_PENDING', 'NEW', 'PARTIALLY_FILLED')) as reconciliation_backlog
                        from futuros_operaciones.copy_dispatch_intent
                    ), active as (
                        select count(*) as open_positions
                        from futuros_operaciones.copy_operation
                        where is_active = true and coalesce(is_shadow, false) = false
                    )
                    select intents.*, active.open_positions
                    from intents cross join active
                    """);
            pending.set(longValue(row.get("pending_intents")));
            ambiguous.set(longValue(row.get("ambiguous_intents")));
            persistencePending.set(longValue(row.get("persistence_pending_intents")));
            manualReview.set(longValue(row.get("manual_review_intents")));
            oldestPendingAge.set(longValue(row.get("oldest_pending_age")));
            reservedMargin.set(decimalValue(row.get("reserved_margin")).doubleValue());
            openOrReservedPositions.set(longValue(row.get("open_positions")) + longValue(row.get("reserved_positions")));
            reconciliationBacklog.set(longValue(row.get("reconciliation_backlog")));
        } catch (RuntimeException ex) {
            log.warn("event=copy.dispatch.metrics.refresh_failed reasonCode=DISPATCH_GAUGE_QUERY_FAILED decision=KEEP_LAST_VALUE errClass={} errMsg=\"{}\"",
                    ex.getClass().getSimpleName(), safe(ex.getMessage()));
        }
    }

    private long longValue(Object value) {
        return value instanceof Number number ? number.longValue() : 0L;
    }

    private BigDecimal decimalValue(Object value) {
        if (value instanceof BigDecimal decimal) return decimal;
        if (value instanceof Number number) return BigDecimal.valueOf(number.doubleValue());
        return BigDecimal.ZERO;
    }

    private String safe(String value) {
        if (value == null) return "";
        String clean = value.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ').replace('"', '\'');
        return clean.length() > 500 ? clean.substring(0, 500) : clean;
    }
}
