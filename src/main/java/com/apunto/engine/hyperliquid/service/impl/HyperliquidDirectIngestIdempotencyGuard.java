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

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
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
                    payload_fingerprint,
                    status,
                    attempt_count,
                    lease_until,
                    first_seen_at,
                    last_seen_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'PROCESSING', 1, now() + (? * interval '1 millisecond'), now(), now())
                ON CONFLICT (idempotency_key) DO UPDATE SET
                    dedupe_key = EXCLUDED.dedupe_key,
                    position_key = EXCLUDED.position_key,
                    wallet = EXCLUDED.wallet,
                    symbol = EXCLUDED.symbol,
                    side = EXCLUDED.side,
                    delta_type = EXCLUDED.delta_type,
                    source_ts_ms = EXCLUDED.source_ts_ms,
                    payload_fingerprint = COALESCE(futuros_operaciones.hyperliquid_direct_ingest_dedupe.payload_fingerprint, EXCLUDED.payload_fingerprint),
                    status = EXCLUDED.status,
                    attempt_count = futuros_operaciones.hyperliquid_direct_ingest_dedupe.attempt_count + 1,
                    lease_until = EXCLUDED.lease_until,
                    last_seen_at = now(),
                    last_reason_code = 'lease_reacquired_after_stale_or_failed'
                WHERE (futuros_operaciones.hyperliquid_direct_ingest_dedupe.status IN ('FAILED', 'REJECTED')
                   OR futuros_operaciones.hyperliquid_direct_ingest_dedupe.lease_until < now())
                  AND futuros_operaciones.hyperliquid_direct_ingest_dedupe.payload_fingerprint = EXCLUDED.payload_fingerprint
                RETURNING attempt_count
            )
            SELECT COALESCE(max(attempt_count), 0) FROM acquired
            """;

    private static final String EXISTING_CLAIM_SQL = """
            SELECT payload_fingerprint, status, lease_until < now() AS lease_expired
            FROM futuros_operaciones.hyperliquid_direct_ingest_dedupe
            WHERE idempotency_key = ?
            """;

    private static final String DUPLICATE_SQL = """
            UPDATE futuros_operaciones.hyperliquid_direct_ingest_dedupe
            SET duplicate_count = duplicate_count + 1,
                last_seen_at = now(),
                payload_fingerprint = COALESCE(payload_fingerprint, ?),
                last_reason_code = 'duplicate_suppressed'
            WHERE idempotency_key = ?
            """;

    private static final String PAYLOAD_CONFLICT_SQL = """
            UPDATE futuros_operaciones.hyperliquid_direct_ingest_dedupe
            SET duplicate_count = duplicate_count + 1,
                last_seen_at = now(),
                last_reason_code = 'IDEMPOTENCY_KEY_PAYLOAD_CONFLICT'
            WHERE idempotency_key = ?
            """;

    private static final String PAYLOAD_UNVERIFIED_SQL = """
            UPDATE futuros_operaciones.hyperliquid_direct_ingest_dedupe
            SET duplicate_count = duplicate_count + 1,
                last_seen_at = now(),
                last_reason_code = 'DISTRIBUTED_DUPLICATE_PAYLOAD_UNVERIFIED'
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

    private record ExistingClaim(String payloadFingerprint, String status, boolean leaseExpired) {
    }

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
        String payloadFingerprint = payloadFingerprint(mappedDelta, dedupeKey);
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
                    payloadFingerprint,
                    Math.max(1000L, properties.getDedupeLeaseTtlMs())
            );
            boolean allowed = acquired != null && acquired > 0L;
            if (!allowed) {
                ExistingClaim existing = existingClaim(idempotencyKey);
                if (existing == null) {
                    throw payloadConflictException(idempotencyKey, mappedDelta,
                            "IDEMPOTENCY_CLAIM_DISAPPEARED", "NA", payloadFingerprint);
                }
                if (existing.payloadFingerprint() == null || existing.payloadFingerprint().isBlank()) {
                    markPayloadUnverified(idempotencyKey, mappedDelta, dedupeKey, existing);
                    return false;
                }
                if (existing.payloadFingerprint() != null
                        && !existing.payloadFingerprint().isBlank()
                        && !existing.payloadFingerprint().equals(payloadFingerprint)) {
                    markPayloadConflict(idempotencyKey, mappedDelta, dedupeKey, existing, payloadFingerprint);
                }
                markDuplicate(idempotencyKey, mappedDelta, dedupeKey, payloadFingerprint);
            } else {
                String result = acquired > 1L ? "reacquired" : "acquired";
                recordDedupeMetric(result);
                if (acquired > 1L) {
                    log.info("event=hyperliquid.direct_ingest.lease_reacquired reasonCode=DISTRIBUTED_DEDUPE_LEASE_REACQUIRED decision=PROCESS expected=true shouldAlert=false retryable=false copyImpact=SAFE_RETRY_SAME_PAYLOAD idempotencyKey={} positionKey={} wallet={} symbol={} side={} deltaType={} attempt={}",
                            safe(idempotencyKey), safe(mappedDelta.positionKey()), safe(mappedDelta.wallet()), safe(mappedDelta.symbol()),
                            safe(mappedDelta.side()), safe(mappedDelta.deltaType()), acquired);
                }
            }
            return allowed;
        } catch (DataAccessException ex) {
            recordDedupeMetric("error");
            if (properties.isFailOpenOnDedupeError()) {
                log.error("event=hyperliquid.direct_ingest.dedupe_guard_unavailable reasonCode=dedupe_guard_unavailable policy=fail_open copyImpact=duplicate_risk idempotencyKey={} positionKey={} wallet={} symbol={} side={} deltaType={} errClass={} errMsg=\"{}\" {}",
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

    private void markDuplicate(
            String idempotencyKey,
            HyperliquidMappedDelta mappedDelta,
            String dedupeKey,
            String payloadFingerprint
    ) {
        try {
            jdbcTemplate.update(DUPLICATE_SQL, payloadFingerprint, idempotencyKey);
        } catch (DataAccessException ex) {
            log.warn("event=hyperliquid.direct_ingest.duplicate_count_update_failed idempotencyKey={} dedupeKey={} errClass={} errMsg=\"{}\"",
                    safe(idempotencyKey), safe(dedupeKey), ex.getClass().getSimpleName(), safeLog(ex.getMessage()));
        }
        recordDedupeMetric("duplicate");
        log.info("event=hyperliquid.direct_ingest.distributed_duplicate reasonCode=DISTRIBUTED_DUPLICATE_SUPPRESSED reasonAlias=duplicate_claimed_by_other_instance decision=NOOP expected=true copyImpact=NO_DUPLICATE_ORDER idempotencyKey={} dedupeKey={} positionKey={} wallet={} symbol={} side={} deltaType={} {}",
                safe(idempotencyKey), safe(dedupeKey), safe(mappedDelta.positionKey()), safe(mappedDelta.wallet()), safe(mappedDelta.symbol()), safe(mappedDelta.side()), safe(mappedDelta.deltaType()),
                CopyLogAdvice.fields("distributed_duplicate_suppressed", CopyLogAdvice.context(null, null, 0, 1, null, null, null, "direct_ingest_dedupe")));
    }

    private ExistingClaim existingClaim(String idempotencyKey) {
        List<ExistingClaim> claims = jdbcTemplate.query(
                EXISTING_CLAIM_SQL,
                (rs, rowNum) -> new ExistingClaim(
                        rs.getString("payload_fingerprint"),
                        rs.getString("status"),
                        rs.getBoolean("lease_expired")
                ),
                idempotencyKey
        );
        return claims.isEmpty() ? null : claims.getFirst();
    }

    private void markPayloadConflict(
            String idempotencyKey,
            HyperliquidMappedDelta mappedDelta,
            String dedupeKey,
            ExistingClaim existing,
            String incomingFingerprint
    ) {
        try {
            jdbcTemplate.update(PAYLOAD_CONFLICT_SQL, idempotencyKey);
        } catch (DataAccessException auditFailure) {
            log.error("event=hyperliquid.direct_ingest.idempotency_payload_conflict_audit_failed reasonCode=IDEMPOTENCY_KEY_PAYLOAD_CONFLICT_AUDIT_FAILED decision=BLOCK_STILL_ENFORCED shouldAlert=true idempotencyKey={} errorClass={} errorMessage=\"{}\"",
                    safe(idempotencyKey), auditFailure.getClass().getSimpleName(), safeLog(auditFailure.getMessage()));
        }
        recordDedupeMetric("payload_conflict");
        log.error("event=hyperliquid.direct_ingest.idempotency_payload_conflict reasonCode=IDEMPOTENCY_KEY_PAYLOAD_CONFLICT reasonAlias=same_key_different_payload decision=BLOCK expected=false shouldAlert=true retryable=false copyImpact=ORDER_NOT_SENT idempotencyKey={} dedupeKey={} positionKey={} wallet={} symbol={} side={} deltaType={} existingStatus={} leaseExpired={} recommendedAction=INVESTIGATE_KEY_GENERATION",
                safe(idempotencyKey), safe(dedupeKey), safe(mappedDelta.positionKey()), safe(mappedDelta.wallet()),
                safe(mappedDelta.symbol()), safe(mappedDelta.side()), safe(mappedDelta.deltaType()), safe(existing.status()), existing.leaseExpired());
        throw payloadConflictException(
                idempotencyKey,
                mappedDelta,
                "IDEMPOTENCY_KEY_PAYLOAD_CONFLICT",
                existing.payloadFingerprint(),
                incomingFingerprint
        );
    }

    private void markPayloadUnverified(
            String idempotencyKey,
            HyperliquidMappedDelta mappedDelta,
            String dedupeKey,
            ExistingClaim existing
    ) {
        try {
            jdbcTemplate.update(PAYLOAD_UNVERIFIED_SQL, idempotencyKey);
        } catch (DataAccessException auditFailure) {
            log.error("event=hyperliquid.direct_ingest.payload_unverified_audit_failed reasonCode=DISTRIBUTED_DUPLICATE_PAYLOAD_UNVERIFIED_AUDIT_FAILED decision=NOOP_STILL_ENFORCED shouldAlert=true idempotencyKey={} errorClass={} errorMessage=\"{}\"",
                    safe(idempotencyKey), auditFailure.getClass().getSimpleName(), safeLog(auditFailure.getMessage()));
        }
        recordDedupeMetric("payload_unverified");
        log.warn("event=hyperliquid.direct_ingest.distributed_duplicate_unverified reasonCode=DISTRIBUTED_DUPLICATE_PAYLOAD_UNVERIFIED reasonAlias=legacy_claim_without_fingerprint decision=NOOP expected=false shouldAlert=true retryable=false copyImpact=NO_ORDER_SENT idempotencyKey={} dedupeKey={} positionKey={} wallet={} symbol={} side={} deltaType={} existingStatus={} leaseExpired={} recommendedAction=REVIEW_OR_BACKFILL_LEGACY_DEDUPE_ROW",
                safe(idempotencyKey), safe(dedupeKey), safe(mappedDelta.positionKey()), safe(mappedDelta.wallet()),
                safe(mappedDelta.symbol()), safe(mappedDelta.side()), safe(mappedDelta.deltaType()),
                safe(existing.status()), existing.leaseExpired());
    }

    private HyperliquidDirectIngestDedupeException payloadConflictException(
            String idempotencyKey,
            HyperliquidMappedDelta mappedDelta,
            String reason,
            String existingFingerprint,
            String incomingFingerprint
    ) {
        return new HyperliquidDirectIngestDedupeException(
                "Conflicto entre idempotencyKey y payload de Hyperliquid direct ingest",
                null,
                Map.of(
                        "reason", reason,
                        "idempotencyKey", safe(idempotencyKey),
                        "positionKey", safe(mappedDelta.positionKey()),
                        "wallet", safe(mappedDelta.wallet()),
                        "symbol", safe(mappedDelta.symbol()),
                        "side", safe(mappedDelta.side()),
                        "deltaType", safe(mappedDelta.deltaType()),
                        "existingFingerprint", safe(existingFingerprint),
                        "incomingFingerprint", safe(incomingFingerprint)
                )
        );
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

    private void recordDedupeMetric(String result) {
        String safeResult = result == null || result.isBlank() ? "unknown" : result;
        meterRegistry.counter("signals.hyperliquid.direct_ingest.distributed_dedupe.total", "result", safeResult).increment();
        meterRegistry.counter("distributed_duplicate_total", "result", safeResult).increment();
    }

    private Long sourceTs(HyperliquidMappedDelta mappedDelta) {
        if (mappedDelta == null || mappedDelta.request() == null || mappedDelta.request().sourceTs() == null || mappedDelta.request().sourceTs() <= 0) {
            return null;
        }
        return mappedDelta.request().sourceTs();
    }

    private String payloadFingerprint(HyperliquidMappedDelta mappedDelta, String dedupeKey) {
        var request = mappedDelta.request();
        String canonical = String.join("|",
                canonicalText(dedupeKey),
                canonicalText(mappedDelta.positionKey()),
                canonicalText(mappedDelta.wallet()),
                canonicalText(mappedDelta.symbol()),
                canonicalText(mappedDelta.side()),
                canonicalText(mappedDelta.deltaType()),
                canonicalValue(sourceTs(mappedDelta)),
                canonicalText(request == null ? null : request.eventId()),
                canonicalText(request == null ? null : request.eventType()),
                canonicalText(request == null ? null : request.status()),
                canonicalDecimal(request == null ? null : request.sizeQty()),
                canonicalDecimal(request == null ? null : request.signedSizeQty()),
                canonicalDecimal(request == null ? null : request.notionalUsd()),
                canonicalDecimal(request == null ? null : request.marginUsedUsd()),
                canonicalDecimal(request == null ? null : request.entryPrice()),
                canonicalDecimal(request == null ? null : request.markPrice()),
                canonicalDecimal(request == null ? null : request.leverage()),
                canonicalValue(request == null ? null : request.walletVersion()),
                canonicalValue(request == null ? null : request.snapshotVersion()),
                canonicalText(request == null ? null : request.externalId())
        );
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(canonical.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SHA-256 no disponible para idempotencia", ex);
        }
    }

    private static String canonicalText(String value) {
        if (value == null) return "<null>";
        return value.trim().toUpperCase(Locale.ROOT);
    }

    private static String canonicalDecimal(BigDecimal value) {
        return value == null ? "<null>" : value.stripTrailingZeros().toPlainString();
    }

    private static String canonicalValue(Object value) {
        return value == null ? "<null>" : value.toString();
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
