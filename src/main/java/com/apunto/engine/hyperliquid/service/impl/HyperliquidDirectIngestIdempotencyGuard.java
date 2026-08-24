package com.apunto.engine.hyperliquid.service.impl;

import com.apunto.engine.hyperliquid.config.HyperliquidDirectIngestProperties;
import com.apunto.engine.hyperliquid.dto.HyperliquidMappedDelta;
import com.apunto.engine.hyperliquid.exception.HyperliquidDirectIngestDedupeException;
import com.apunto.engine.shared.util.CopyLogAdvice;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeSet;

@Slf4j
@Service
public class HyperliquidDirectIngestIdempotencyGuard {

    private static final String FINGERPRINT_ALGORITHM =
            "hyperliquid-economic-v2";
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
                    first_payload,
                    fingerprint_algorithm,
                    status,
                    attempt_count,
                    lease_until,
                    first_seen_at,
                    last_seen_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?,
                          'PROCESSING', 1,
                          now() + (? * interval '1 millisecond'), now(), now())
                ON CONFLICT (idempotency_key) DO UPDATE SET
                    dedupe_key = EXCLUDED.dedupe_key,
                    position_key = EXCLUDED.position_key,
                    wallet = EXCLUDED.wallet,
                    symbol = EXCLUDED.symbol,
                    side = EXCLUDED.side,
                    delta_type = EXCLUDED.delta_type,
                    source_ts_ms = EXCLUDED.source_ts_ms,
                   payload_fingerprint = COALESCE(futuros_operaciones.hyperliquid_direct_ingest_dedupe.payload_fingerprint, EXCLUDED.payload_fingerprint),
                    first_payload = COALESCE(
                        futuros_operaciones.hyperliquid_direct_ingest_dedupe.first_payload,
                        EXCLUDED.first_payload),
                    fingerprint_algorithm = COALESCE(
                        futuros_operaciones.hyperliquid_direct_ingest_dedupe.fingerprint_algorithm,
                        EXCLUDED.fingerprint_algorithm),
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
            SELECT payload_fingerprint, status, lease_until < now() AS lease_expired,
                   wallet, symbol, source_ts_ms,
                   first_payload::text AS first_payload_json
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

    private static final String REPLICA_PAYLOAD_DIVERGENCE_SQL = """
            UPDATE futuros_operaciones.hyperliquid_direct_ingest_dedupe
            SET duplicate_count = duplicate_count + 1,
                last_seen_at = now(),
                last_reason_code = 'REPLICA_DERIVED_PAYLOAD_DIVERGENCE'
            WHERE idempotency_key = ?
            """;

    private static final String INSERT_REPLICA_CONFLICT_SQL = """
            INSERT INTO futuros_operaciones.hyperliquid_replica_payload_conflict (
                idempotency_key,
                existing_fingerprint,
                incoming_fingerprint,
                fingerprint_algorithm,
                first_payload,
                incoming_payload,
                differing_fields,
                resolution_status,
                first_detected_at,
                last_detected_at,
                observation_count
            ) VALUES (?, ?, ?, ?, ?::jsonb, ?::jsonb, ?::jsonb, ?,
                      now(), now(), 1)
            ON CONFLICT (idempotency_key, incoming_fingerprint)
            DO UPDATE SET
                last_detected_at = now(),
                observation_count =
                    futuros_operaciones.hyperliquid_replica_payload_conflict.observation_count + 1,
                incoming_payload = EXCLUDED.incoming_payload,
                differing_fields = EXCLUDED.differing_fields,
                resolution_status = CASE
                    WHEN futuros_operaciones.hyperliquid_replica_payload_conflict.resolution_status
                         = 'RESOLVED'
                    THEN 'RESOLVED'
                    ELSE EXCLUDED.resolution_status
                END
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
    private final ObjectMapper objectMapper;

    private record ExistingClaim(
            String payloadFingerprint,
            String status,
            boolean leaseExpired,
            String wallet,
            String symbol,
            Long sourceTs,
            String firstPayloadJson
    ) {
    }

    public HyperliquidDirectIngestIdempotencyGuard(
            HyperliquidDirectIngestProperties properties,
            JdbcTemplate jdbcTemplate,
            MeterRegistry meterRegistry
    ) {
        this(
                properties,
                jdbcTemplate,
                meterRegistry,
                new ObjectMapper().findAndRegisterModules());
    }

    @Autowired
    public HyperliquidDirectIngestIdempotencyGuard(
            HyperliquidDirectIngestProperties properties,
            JdbcTemplate jdbcTemplate,
            MeterRegistry meterRegistry,
            ObjectMapper objectMapper
    ) {
        this.properties = properties;
        this.jdbcTemplate = jdbcTemplate;
        this.meterRegistry = meterRegistry;
        this.objectMapper = objectMapper;
    }

    public boolean tryAcquire(HyperliquidMappedDelta mappedDelta, String dedupeKey) {
        return acquire(mappedDelta, dedupeKey).acquired();
    }

    public AcquireDecision acquire(
            HyperliquidMappedDelta mappedDelta,
            String dedupeKey
    ) {
        if (!properties.isDistributedDedupeEnabled()) {
            return AcquireDecision.ACQUIRED;
        }
        String idempotencyKey = requireIdempotencyKey(mappedDelta);
        String payloadFingerprint = payloadFingerprint(mappedDelta, dedupeKey);
        String payloadEvidence = payloadEvidence(mappedDelta, dedupeKey);
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
                    payloadEvidence,
                    FINGERPRINT_ALGORITHM,
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
                    return duplicateDecision(existing);
                }
                if (existing.payloadFingerprint() != null
                        && !existing.payloadFingerprint().isBlank()
                        && !existing.payloadFingerprint().equals(payloadFingerprint)) {
                    if (isAuthoritativeUserFill(mappedDelta)
                            && existingIsAuthoritativeUserFill(existing)) {
                        markPayloadConflict(
                                idempotencyKey, mappedDelta, dedupeKey,
                                existing, payloadFingerprint);
                    }
                    if (sameImmutableSourceIdentity(existing, mappedDelta)) {
                        markReplicaPayloadDivergence(
                                idempotencyKey, mappedDelta, dedupeKey, existing,
                                payloadFingerprint, payloadEvidence);
                        return duplicateDecision(existing);
                    }
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
            return allowed
                    ? AcquireDecision.ACQUIRED
                    : duplicateDecision(existingClaim(idempotencyKey));
        } catch (DataAccessException ex) {
            recordDedupeMetric("error");
            if (properties.isFailOpenOnDedupeError()) {
                log.error("event=hyperliquid.direct_ingest.dedupe_guard_unavailable reasonCode=dedupe_guard_unavailable policy=fail_open copyImpact=duplicate_risk idempotencyKey={} positionKey={} wallet={} symbol={} side={} deltaType={} errClass={} errMsg=\"{}\" {}",
                        safe(idempotencyKey), safe(mappedDelta.positionKey()), safe(mappedDelta.wallet()), safe(mappedDelta.symbol()), safe(mappedDelta.side()), safe(mappedDelta.deltaType()),
                        ex.getClass().getSimpleName(), safeLog(ex.getMessage()),
                        CopyLogAdvice.fields("dedupe_guard_unavailable", CopyLogAdvice.context(null, null, null, null, null, null, null, "direct_ingest_dedupe")));
                return AcquireDecision.ACQUIRED;
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

    private AcquireDecision duplicateDecision(ExistingClaim existing) {
        return existing != null && STATUS_PROCESSED.equals(existing.status())
                ? AcquireDecision.DUPLICATE_COMPLETED
                : AcquireDecision.DUPLICATE_IN_FLIGHT;
    }

    public enum AcquireDecision {
        ACQUIRED,
        DUPLICATE_COMPLETED,
        DUPLICATE_IN_FLIGHT;

        public boolean acquired() {
            return this == ACQUIRED;
        }

        public boolean completed() {
            return this == DUPLICATE_COMPLETED;
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
        meterRegistry.counter(
                "duplicate_noop_total",
                "delta_type", safeMetricTag(mappedDelta.deltaType())
        ).increment();
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
                        rs.getBoolean("lease_expired"),
                        rs.getString("wallet"),
                        rs.getString("symbol"),
                        rs.getObject("source_ts_ms", Long.class),
                        rs.getString("first_payload_json")
                ),
                idempotencyKey
        );
        return claims.isEmpty() ? null : claims.getFirst();
    }

    private boolean sameImmutableSourceIdentity(ExistingClaim existing,
                                                HyperliquidMappedDelta incoming) {
        return existing != null
                && canonicalText(existing.wallet()).equals(canonicalText(incoming.wallet()))
                && canonicalText(existing.symbol()).equals(canonicalText(incoming.symbol()))
                && java.util.Objects.equals(existing.sourceTs(), sourceTs(incoming));
    }

    private boolean isAuthoritativeUserFill(HyperliquidMappedDelta mappedDelta) {
        if (mappedDelta == null || mappedDelta.request() == null) {
            return false;
        }
        var request = mappedDelta.request();
        return "USER_FILL".equalsIgnoreCase(request.economicEventKind())
                && Boolean.FALSE.equals(request.sourceEstimated())
                && request.sourceSequence() != null
                && request.sourceSequence() > 0L;
    }

    private boolean existingIsAuthoritativeUserFill(ExistingClaim existing) {
        if (existing == null || existing.firstPayloadJson() == null
                || existing.firstPayloadJson().isBlank()) {
            return false;
        }
        try {
            JsonNode payload = objectMapper.readTree(existing.firstPayloadJson());
            return "USER_FILL".equalsIgnoreCase(
                    payload.path("economicEventKind").asText())
                    && !payload.path("sourceEstimated").asBoolean(true);
        } catch (JsonProcessingException invalidStoredEvidence) {
            return false;
        }
    }

    private void markReplicaPayloadDivergence(
            String idempotencyKey,
            HyperliquidMappedDelta mappedDelta,
            String dedupeKey,
            ExistingClaim existing,
            String incomingFingerprint,
            String incomingPayload
    ) {
        persistReplicaConflict(
                idempotencyKey,
                existing,
                incomingFingerprint,
                incomingPayload,
                isAuthoritativeUserFill(mappedDelta)
                        ? "AUTHORITATIVE_VARIANT_OBSERVED"
                        : "UNRESOLVED");
        try {
            jdbcTemplate.update(REPLICA_PAYLOAD_DIVERGENCE_SQL, idempotencyKey);
        } catch (DataAccessException auditFailure) {
            log.error("event=hyperliquid.direct_ingest.replica_payload_divergence_audit_failed reasonCode=REPLICA_DERIVED_PAYLOAD_DIVERGENCE_AUDIT_FAILED decision=NOOP_STILL_ENFORCED shouldAlert=true idempotencyKey={} errorClass={} errorMessage=\"{}\"",
                    safe(idempotencyKey), auditFailure.getClass().getSimpleName(), safeLog(auditFailure.getMessage()));
        }
        recordDedupeMetric("replica_payload_divergence");
        meterRegistry.counter(
                "replica_payload_divergence_total",
                "delta_type", safeMetricTag(mappedDelta.deltaType())
        ).increment();
        meterRegistry.counter(
                "replica_payload_conflict_total",
                "delta_type", safeMetricTag(mappedDelta.deltaType())
        ).increment();
        log.warn("event=hyperliquid.direct_ingest.replica_payload_divergence reasonCode=REPLICA_DERIVED_PAYLOAD_DIVERGENCE decision=NOOP_HTTP_ACK expected=false shouldAlert=true retryable=false copyImpact=NO_DUPLICATE_ORDER idempotencyKey={} dedupeKey={} positionKey={} wallet={} symbol={} side={} deltaType={} sourceTs={} existingStatus={} leaseExpired={} existingFingerprint={} incomingFingerprint={} recommendedAction=COMPARE_HYPERLIQUID_REPLICA_LOCAL_STATE",
                safe(idempotencyKey), safe(dedupeKey), safe(mappedDelta.positionKey()),
                safe(mappedDelta.wallet()), safe(mappedDelta.symbol()), safe(mappedDelta.side()),
                safe(mappedDelta.deltaType()), sourceTs(mappedDelta), safe(existing.status()),
                existing.leaseExpired(), safe(existing.payloadFingerprint()), safe(incomingFingerprint));
    }

    private void markPayloadConflict(
            String idempotencyKey,
            HyperliquidMappedDelta mappedDelta,
            String dedupeKey,
            ExistingClaim existing,
            String incomingFingerprint
    ) {
        persistReplicaConflict(
                idempotencyKey,
                existing,
                incomingFingerprint,
                payloadEvidence(mappedDelta, dedupeKey),
                "ESCALATED");
        try {
            jdbcTemplate.update(PAYLOAD_CONFLICT_SQL, idempotencyKey);
        } catch (DataAccessException auditFailure) {
            log.error("event=hyperliquid.direct_ingest.idempotency_payload_conflict_audit_failed reasonCode=IDEMPOTENCY_KEY_PAYLOAD_CONFLICT_AUDIT_FAILED decision=BLOCK_STILL_ENFORCED shouldAlert=true idempotencyKey={} errorClass={} errorMessage=\"{}\"",
                    safe(idempotencyKey), auditFailure.getClass().getSimpleName(), safeLog(auditFailure.getMessage()));
        }
        recordDedupeMetric("payload_conflict");
        meterRegistry.counter(
                "authoritative_identity_conflict_total",
                "delta_type", safeMetricTag(mappedDelta.deltaType())
        ).increment();
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

    private void persistReplicaConflict(
            String idempotencyKey,
            ExistingClaim existing,
            String incomingFingerprint,
            String incomingPayload,
            String resolutionStatus
    ) {
        String existingPayload = existing.firstPayloadJson() == null
                || existing.firstPayloadJson().isBlank()
                ? null
                : existing.firstPayloadJson();
        String differingFields = differingFields(
                existingPayload, incomingPayload);
        try {
            jdbcTemplate.update(
                    INSERT_REPLICA_CONFLICT_SQL,
                    idempotencyKey,
                    existing.payloadFingerprint(),
                    incomingFingerprint,
                    FINGERPRINT_ALGORITHM,
                    existingPayload,
                    incomingPayload,
                    differingFields,
                    resolutionStatus);
        } catch (DataAccessException auditFailure) {
            log.error("event=hyperliquid.direct_ingest.replica_conflict_evidence_failed reasonCode=REPLICA_CONFLICT_EVIDENCE_PERSIST_FAILED decision=NOOP_STILL_ENFORCED shouldAlert=true idempotencyKey={} errorClass={} errorMessage=\"{}\"",
                    safe(idempotencyKey),
                    auditFailure.getClass().getSimpleName(),
                    safeLog(auditFailure.getMessage()));
        }
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
                canonicalText(mappedDelta.idempotencyKey()),
                canonicalText(mappedDelta.wallet()),
                canonicalText(mappedDelta.symbol()),
                canonicalText(mappedDelta.side()),
                canonicalText(mappedDelta.deltaType()),
                canonicalValue(sourceTs(mappedDelta)),
                canonicalText(request == null ? null : request.eventType()),
                canonicalText(request == null ? null : request.status()),
                canonicalDecimal(request == null ? null : request.sizeQty()),
                canonicalDecimal(request == null ? null : request.signedSizeQty()),
                canonicalDecimal(request == null ? null : request.notionalUsd()),
                canonicalDecimal(request == null ? null : request.marginUsedUsd()),
                canonicalDecimal(request == null ? null : request.entryPrice()),
                canonicalDecimal(request == null ? null : request.markPrice()),
                canonicalDecimal(request == null ? null : request.leverage()),
                canonicalDecimal(request == null ? null : request.rawNotionalUsd()),
                canonicalDecimal(request == null ? null : request.positionNotionalUsd()),
                canonicalDecimal(request == null ? null : request.closedNotionalUsd()),
                canonicalDecimal(request == null ? null : request.closedMarginUsedUsd()),
                canonicalDecimal(request == null ? null : request.effectiveCloseQty()),
                canonicalDecimal(request == null ? null : request.effectiveEntryPrice()),
                canonicalDecimal(request == null ? null : request.effectiveExitPrice()),
                canonicalDecimal(request == null ? null : request.effectiveRealizedPnlUsd()),
                canonicalText(request == null ? null : request.economicEventKind()),
                canonicalValue(request == null ? null : request.economicEventVersion()),
                canonicalValue(request == null ? null : request.sourceSequence()),
                canonicalDecimal(request == null ? null : request.sourceFeeUsd()),
                canonicalDecimal(request == null ? null : request.fundingPnlUsd()),
                canonicalText(request == null ? null : request.executionPriceBasis()),
                canonicalText(request == null ? null : request.notionalBasis()),
                canonicalValue(request == null ? null : request.sourceEstimated()),
                canonicalDecimal(request == null ? null : request.sourcePreviousPositionQuantity()),
                canonicalDecimal(request == null ? null : request.sourceResultingPositionQuantity()),
                canonicalDecimal(request == null ? null : request.sourceExecutionQuantity()),
                canonicalDecimal(request == null ? null : request.sourceSignedExecutionQuantity()),
                canonicalText(request == null ? null : request.sourceDeliveryMode()),
                canonicalValue(request == null ? null : request.sourceRecoveredAt())
        );
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(canonical.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SHA-256 no disponible para idempotencia", ex);
        }
    }

    private String payloadEvidence(
            HyperliquidMappedDelta mappedDelta,
            String dedupeKey
    ) {
        var request = mappedDelta.request();
        Map<String, Object> evidence = new LinkedHashMap<>();
        evidence.put("fingerprintAlgorithm", FINGERPRINT_ALGORITHM);
        evidence.put("dedupeKey", dedupeKey);
        evidence.put("positionKey", mappedDelta.positionKey());
        evidence.put("wallet", mappedDelta.wallet());
        evidence.put("symbol", mappedDelta.symbol());
        evidence.put("side", mappedDelta.side());
        evidence.put("deltaType", mappedDelta.deltaType());
        evidence.put("sourceTs", sourceTs(mappedDelta));
        evidence.put("eventType", request == null ? null : request.eventType());
        evidence.put("status", request == null ? null : request.status());
        evidence.put("sizeQty", request == null ? null : request.sizeQty());
        evidence.put("signedSizeQty",
                request == null ? null : request.signedSizeQty());
        evidence.put("notionalUsd",
                request == null ? null : request.notionalUsd());
        evidence.put("marginUsedUsd",
                request == null ? null : request.marginUsedUsd());
        evidence.put("entryPrice",
                request == null ? null : request.entryPrice());
        evidence.put("markPrice",
                request == null ? null : request.markPrice());
        evidence.put("leverage",
                request == null ? null : request.leverage());
        evidence.put("externalId",
                request == null ? null : request.externalId());
        evidence.put("economicEventKind",
                request == null ? null : request.economicEventKind());
        evidence.put("sourceSequence",
                request == null ? null : request.sourceSequence());
        evidence.put("sourceEstimated",
                request == null ? null : request.sourceEstimated());
        evidence.put("sourcePreviousPositionQuantity",
                request == null ? null : request.sourcePreviousPositionQuantity());
        evidence.put("sourceResultingPositionQuantity",
                request == null ? null : request.sourceResultingPositionQuantity());
        evidence.put("sourceExecutionQuantity",
                request == null ? null : request.sourceExecutionQuantity());
        evidence.put("sourceSignedExecutionQuantity",
                request == null ? null : request.sourceSignedExecutionQuantity());
        evidence.put("sourceDeliveryMode",
                request == null ? null : request.sourceDeliveryMode());
        evidence.put("sourceRecoveredAt",
                request == null ? null : request.sourceRecoveredAt());
        evidence.put("walletVersion",
                request == null ? null : request.walletVersion());
        evidence.put("snapshotVersion",
                request == null ? null : request.snapshotVersion());
        try {
            return objectMapper.writeValueAsString(evidence);
        } catch (JsonProcessingException serializationFailure) {
            throw new IllegalStateException(
                    "Could not serialize sanitized Hyperliquid payload evidence",
                    serializationFailure);
        }
    }

    private String differingFields(
            String existingPayload,
            String incomingPayload
    ) {
        if (existingPayload == null || existingPayload.isBlank()) {
            return "[\"firstPayloadUnavailable\"]";
        }
        try {
            JsonNode existing = objectMapper.readTree(existingPayload);
            JsonNode incoming = objectMapper.readTree(incomingPayload);
            TreeSet<String> names = new TreeSet<>();
            existing.fieldNames().forEachRemaining(names::add);
            incoming.fieldNames().forEachRemaining(names::add);
            List<String> differences = new ArrayList<>();
            for (String name : names) {
                if (!java.util.Objects.equals(
                        existing.get(name), incoming.get(name))) {
                    differences.add(name);
                }
            }
            return objectMapper.writeValueAsString(differences);
        } catch (JsonProcessingException invalidEvidence) {
            return "[\"evidenceParseFailed\"]";
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

    private static String safeMetricTag(String value) {
        return value == null || value.isBlank()
                ? "unknown"
                : value.trim().toLowerCase(Locale.ROOT);
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
