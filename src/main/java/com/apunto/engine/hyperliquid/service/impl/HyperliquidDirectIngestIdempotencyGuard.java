package com.apunto.engine.hyperliquid.service.impl;

import com.apunto.engine.hyperliquid.config.HyperliquidDirectIngestProperties;
import com.apunto.engine.hyperliquid.dto.HyperliquidMappedDelta;
import com.apunto.engine.hyperliquid.exception.HyperliquidDirectIngestDedupeException;
import com.apunto.engine.shared.util.CopyLogAdvice;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.Map;

@Slf4j
@Service
public class HyperliquidDirectIngestIdempotencyGuard {

    private static final String STATUS_PROCESSING = "PROCESSING";
    private static final String STATUS_PROCESSED = "PROCESSED";
    private static final String STATUS_FAILED = "FAILED";
    private static final String STATUS_REJECTED = "REJECTED";

    private static final String ACQUIRE_SQL = """
            WITH acquired AS (
                INSERT INTO futuros_operaciones.hyperliquid_direct_ingest_dedupe (
                    idempotency_key,
                    dedupe_key,
                    position_key,
                    wallet,
                    symbol,
                    side,
                    delta_type,
                    source_ts_ms,
                    status,
                    attempt_count,
                    lease_until,
                    first_seen_at,
                    last_seen_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'PROCESSING', 1, now() + (? * interval '1 millisecond'), now(), now())
                ON CONFLICT (idempotency_key) DO UPDATE SET
                    dedupe_key = EXCLUDED.dedupe_key,
                    position_key = EXCLUDED.position_key,
                    wallet = EXCLUDED.wallet,
                    symbol = EXCLUDED.symbol,
                    side = EXCLUDED.side,
                    delta_type = EXCLUDED.delta_type,
                    source_ts_ms = EXCLUDED.source_ts_ms,
                    status = EXCLUDED.status,
                    attempt_count = futuros_operaciones.hyperliquid_direct_ingest_dedupe.attempt_count + 1,
                    lease_until = EXCLUDED.lease_until,
                    last_seen_at = now(),
                    last_reason_code = 'lease_reacquired_after_stale_or_failed'
                WHERE futuros_operaciones.hyperliquid_direct_ingest_dedupe.status IN ('FAILED', 'REJECTED')
                   OR futuros_operaciones.hyperliquid_direct_ingest_dedupe.lease_until < now()
                RETURNING idempotency_key
            )
            SELECT count(*) FROM acquired
            """;

    private static final String DUPLICATE_SQL = """
            UPDATE futuros_operaciones.hyperliquid_direct_ingest_dedupe
            SET duplicate_count = duplicate_count + 1,
                last_seen_at = now(),
                last_reason_code = 'duplicate_suppressed'
            WHERE idempotency_key = ?
            """;

    private static final String MARK_PROCESSED_SQL = """
            UPDATE futuros_operaciones.hyperliquid_direct_ingest_dedupe
            SET status = 'PROCESSED',
                processed_at = now(),
                lease_until = NULL,
                last_seen_at = now(),
                last_reason_code = ?
            WHERE idempotency_key = ?
            """;

    private static final String MARK_FAILED_SQL = """
            UPDATE futuros_operaciones.hyperliquid_direct_ingest_dedupe
            SET status = ?,
                failed_at = now(),
                lease_until = NULL,
                last_seen_at = now(),
                last_reason_code = ?,
                last_error_class = ?,
                last_error_message = ?
            WHERE idempotency_key = ?
            """;

    private final HyperliquidDirectIngestProperties properties;
    private final JdbcTemplate jdbcTemplate;
    private final MeterRegistry meterRegistry;

    public HyperliquidDirectIngestIdempotencyGuard(
            HyperliquidDirectIngestProperties properties,
            JdbcTemplate jdbcTemplate,
            MeterRegistry meterRegistry
    ) {
        this.properties = properties;
        this.jdbcTemplate = jdbcTemplate;
        this.meterRegistry = meterRegistry;
    }

    public boolean tryAcquire(HyperliquidMappedDelta mappedDelta, String dedupeKey) {
        if (!properties.isDistributedDedupeEnabled()) {
            return true;
        }
        String idempotencyKey = requireIdempotencyKey(mappedDelta);
        try {
            Long acquired = jdbcTemplate.queryForObject(
                    ACQUIRE_SQL,
                    Long.class,
                    idempotencyKey,
                    safe(dedupeKey),
                    safe(mappedDelta.positionKey()),
                    safe(mappedDelta.wallet()),
                    safe(mappedDelta.symbol()),
                    safe(mappedDelta.side()),
                    safe(mappedDelta.deltaType()),
                    sourceTs(mappedDelta),
                    Math.max(1000L, properties.getDedupeLeaseTtlMs())
            );
            boolean allowed = acquired != null && acquired > 0L;
            if (!allowed) {
                markDuplicate(idempotencyKey, mappedDelta, dedupeKey);
            } else {
                meterRegistry.counter("signals.hyperliquid.direct_ingest.distributed_dedupe.total", "result", "acquired").increment();
            }
            return allowed;
        } catch (DataAccessException ex) {
            meterRegistry.counter("signals.hyperliquid.direct_ingest.distributed_dedupe.total", "result", "error").increment();
            if (properties.isFailOpenOnDedupeError()) {
                log.error("event=hyperliquid.direct_ingest.dedupe_guard_unavailable reasonCode=dedupe_guard_unavailable policy=fail_open copyImpact=duplicate_risk idempotencyKey={} positionKey={} wallet={} symbol={} side={} deltaType={} errClass={} errMsg=\"{}\" humanMessage=no_pude_validar_idempotencia_distribuida_y_deje_pasar_el_evento_por_configuracion {}",
                        safe(idempotencyKey), safe(mappedDelta.positionKey()), safe(mappedDelta.wallet()), safe(mappedDelta.symbol()), safe(mappedDelta.side()), safe(mappedDelta.deltaType()),
                        ex.getClass().getSimpleName(), safeLog(ex.getMessage()),
                        CopyLogAdvice.fields("dedupe_guard_unavailable", CopyLogAdvice.context(null, null, null, null, null, null, null, "direct_ingest_dedupe")));
                return true;
            }
            throw new HyperliquidDirectIngestDedupeException(
                    "No se pudo validar idempotencia distribuida de Hyperliquid direct ingest",
                    ex,
                    Map.of(
                            "reason", "dedupe_guard_unavailable",
                            "idempotencyKey", idempotencyKey,
                            "positionKey", safe(mappedDelta.positionKey()),
                            "wallet", safe(mappedDelta.wallet()),
                            "symbol", safe(mappedDelta.symbol()),
                            "side", safe(mappedDelta.side()),
                            "deltaType", safe(mappedDelta.deltaType())
                    )
            );
        }
    }

    public void markProcessed(HyperliquidMappedDelta mappedDelta, String reasonCode) {
        if (!properties.isDistributedDedupeEnabled() || mappedDelta == null || mappedDelta.idempotencyKey() == null) {
            return;
        }
        try {
            jdbcTemplate.update(MARK_PROCESSED_SQL, safeReason(reasonCode, "processed"), mappedDelta.idempotencyKey());
        } catch (DataAccessException ex) {
            log.error("event=hyperliquid.direct_ingest.dedupe_mark_processed_failed reasonCode=dedupe_mark_processed_failed copyImpact=copy_already_decided idempotencyKey={} wallet={} symbol={} side={} deltaType={} errClass={} errMsg=\"{}\" humanMessage=la_copia_ya_fue_procesada_pero_no_pude_marcar_el_guard_como_procesado",
                    safe(mappedDelta.idempotencyKey()), safe(mappedDelta.wallet()), safe(mappedDelta.symbol()), safe(mappedDelta.side()), safe(mappedDelta.deltaType()),
                    ex.getClass().getSimpleName(), safeLog(ex.getMessage()));
        }
    }

    public void markFailed(HyperliquidMappedDelta mappedDelta, String reasonCode, Throwable ex) {
        markTerminal(mappedDelta, STATUS_FAILED, safeReason(reasonCode, "failed"), ex);
    }

    public void markRejected(HyperliquidMappedDelta mappedDelta, String reasonCode, Throwable ex) {
        markTerminal(mappedDelta, STATUS_REJECTED, safeReason(reasonCode, "rejected"), ex);
    }

    private void markTerminal(HyperliquidMappedDelta mappedDelta, String status, String reasonCode, Throwable ex) {
        if (!properties.isDistributedDedupeEnabled() || mappedDelta == null || mappedDelta.idempotencyKey() == null) {
            return;
        }
        try {
            jdbcTemplate.update(
                    MARK_FAILED_SQL,
                    status,
                    reasonCode,
                    ex == null ? null : safe(ex.getClass().getSimpleName()),
                    ex == null ? null : safeLog(ex.getMessage()),
                    mappedDelta.idempotencyKey()
            );
        } catch (DataAccessException dbEx) {
            log.error("event=hyperliquid.direct_ingest.dedupe_mark_terminal_failed reasonCode=dedupe_mark_terminal_failed status={} copyImpact=copy_state_uncertain idempotencyKey={} wallet={} symbol={} side={} deltaType={} errClass={} errMsg=\"{}\" humanMessage=no_pude_marcar_el_guard_de_idempotencia_como_terminal",
                    status, safe(mappedDelta.idempotencyKey()), safe(mappedDelta.wallet()), safe(mappedDelta.symbol()), safe(mappedDelta.side()), safe(mappedDelta.deltaType()),
                    dbEx.getClass().getSimpleName(), safeLog(dbEx.getMessage()));
        }
    }

    private void markDuplicate(String idempotencyKey, HyperliquidMappedDelta mappedDelta, String dedupeKey) {
        try {
            jdbcTemplate.update(DUPLICATE_SQL, idempotencyKey);
        } catch (DataAccessException ex) {
            log.warn("event=hyperliquid.direct_ingest.duplicate_count_update_failed idempotencyKey={} dedupeKey={} errClass={} errMsg=\"{}\"",
                    safe(idempotencyKey), safe(dedupeKey), ex.getClass().getSimpleName(), safeLog(ex.getMessage()));
        }
        meterRegistry.counter("signals.hyperliquid.direct_ingest.distributed_dedupe.total", "result", "duplicate").increment();
        log.info("event=hyperliquid.direct_ingest.distributed_duplicate reasonCode=distributed_duplicate_suppressed reasonAlias=duplicate_from_other_instance friendlyReason=evento_ya_tomado_por_otra_instancia explanation=otra_instancia_de_signals_ya_tomo_esta_idempotencyKey_y_esta_no_debe_copiar copyImpact=no_copy_order idempotencyKey={} dedupeKey={} positionKey={} wallet={} symbol={} side={} deltaType={} {}",
                safe(idempotencyKey), safe(dedupeKey), safe(mappedDelta.positionKey()), safe(mappedDelta.wallet()), safe(mappedDelta.symbol()), safe(mappedDelta.side()), safe(mappedDelta.deltaType()),
                CopyLogAdvice.fields("distributed_duplicate_suppressed", CopyLogAdvice.context(null, null, 0, 1, null, null, null, "direct_ingest_dedupe")));
    }

    private String requireIdempotencyKey(HyperliquidMappedDelta mappedDelta) {
        if (mappedDelta == null || mappedDelta.idempotencyKey() == null || mappedDelta.idempotencyKey().isBlank()) {
            throw new HyperliquidDirectIngestDedupeException(
                    "Hyperliquid direct ingest requiere idempotencyKey para dedupe distribuido",
                    null,
                    Map.of("reason", "idempotency_key_missing")
            );
        }
        return mappedDelta.idempotencyKey().trim();
    }

    private Long sourceTs(HyperliquidMappedDelta mappedDelta) {
        if (mappedDelta == null || mappedDelta.request() == null || mappedDelta.request().sourceTs() == null || mappedDelta.request().sourceTs() <= 0) {
            return null;
        }
        return mappedDelta.request().sourceTs();
    }

    private String safeReason(String reasonCode, String fallback) {
        if (reasonCode == null || reasonCode.isBlank()) {
            return fallback;
        }
        return safe(reasonCode);
    }

    private String safe(String value) {
        if (value == null || value.isBlank()) {
            return "NA";
        }
        String clean = value.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ').replace('"', '\'').trim();
        return clean.length() > 600 ? clean.substring(0, 600) : clean;
    }

    private String safeLog(String value) {
        if (value == null || value.isBlank()) {
            return "NA";
        }
        String clean = value.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ').replace('"', '\'').trim();
        return clean.length() > 1000 ? clean.substring(0, 1000) : clean;
    }
}
