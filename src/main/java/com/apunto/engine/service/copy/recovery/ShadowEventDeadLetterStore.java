package com.apunto.engine.service.copy.recovery;

import com.apunto.engine.hyperliquid.dto.HyperliquidMappedDelta;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.LinkedHashMap;
import java.util.Map;

@Component
@Slf4j
public class ShadowEventDeadLetterStore {

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;
    private final MeterRegistry meterRegistry;

    public ShadowEventDeadLetterStore(JdbcTemplate jdbcTemplate,
                                      ObjectMapper objectMapper,
                                      MeterRegistry meterRegistry) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
        this.meterRegistry = meterRegistry;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void recordRecoverable(HyperliquidMappedDelta mappedDelta, Throwable failure, int attempts) {
        if (mappedDelta == null) return;
        String idempotencyKey = firstNonBlank(mappedDelta.idempotencyKey(), mappedDelta.positionKey());
        if (idempotencyKey == null) {
            throw new IllegalArgumentException("shadow dead-letter requires an idempotency key");
        }
        String sourceEventId = mappedDelta.event() == null ? null : mappedDelta.event().getSourceEventId();
        String errorCode = errorCode(failure);
        String detail = safe(failure == null ? null : failure.getMessage(), 1000);
        String payload = payload(mappedDelta);

        jdbcTemplate.update("""
                INSERT INTO futuros_operaciones.shadow_event_dead_letter (
                    idempotency_key, source_event_id, position_key, wallet_id, symbol,
                    position_side, delta_type, payload_json, error_code, error_detail,
                    attempt_count, status, first_failed_at, last_failed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, cast(? as jsonb), ?, ?, ?, 'RECOVERABLE', clock_timestamp(), clock_timestamp())
                ON CONFLICT (idempotency_key) DO UPDATE SET
                    source_event_id = excluded.source_event_id,
                    position_key = excluded.position_key,
                    wallet_id = excluded.wallet_id,
                    symbol = excluded.symbol,
                    position_side = excluded.position_side,
                    delta_type = excluded.delta_type,
                    payload_json = excluded.payload_json,
                    error_code = excluded.error_code,
                    error_detail = excluded.error_detail,
                    attempt_count = greatest(futuros_operaciones.shadow_event_dead_letter.attempt_count, excluded.attempt_count),
                    status = 'RECOVERABLE',
                    last_failed_at = clock_timestamp(),
                    resolved_at = null
                """,
                safe(idempotencyKey, 600), safe(sourceEventId, 600), safe(mappedDelta.positionKey(), 600),
                safe(mappedDelta.wallet(), 180), safe(mappedDelta.symbol(), 40), safe(mappedDelta.side(), 12),
                safe(mappedDelta.deltaType(), 40), payload, errorCode, detail, Math.max(1, attempts));
        meterRegistry.counter("copy.shadow.deadletter.total", "result", "recoverable").increment();
        log.error("event=shadow_dead_letter_recorded idempotencyKey={} sourceEventId={} positionKey={} walletId={} symbol={} side={} deltaType={} attempt={} result=RECOVERABLE reasonCode={} liveImpact=LIVE_NOT_BLOCKED",
                safe(idempotencyKey, 100), safe(sourceEventId, 100), safe(mappedDelta.positionKey(), 100),
                safe(mappedDelta.wallet(), 100), safe(mappedDelta.symbol(), 40), safe(mappedDelta.side(), 12),
                safe(mappedDelta.deltaType(), 40), Math.max(1, attempts), errorCode);
    }

    private String payload(HyperliquidMappedDelta mappedDelta) {
        try {
            return objectMapper.writeValueAsString(mappedDelta);
        } catch (JsonProcessingException serializationFailure) {
            Map<String, Object> fallback = new LinkedHashMap<>();
            fallback.put("idempotencyKey", mappedDelta.idempotencyKey());
            fallback.put("positionKey", mappedDelta.positionKey());
            fallback.put("wallet", mappedDelta.wallet());
            fallback.put("symbol", mappedDelta.symbol());
            fallback.put("side", mappedDelta.side());
            fallback.put("deltaType", mappedDelta.deltaType());
            fallback.put("serializationError", serializationFailure.getClass().getSimpleName());
            try {
                return objectMapper.writeValueAsString(fallback);
            } catch (JsonProcessingException impossibleFallback) {
                return "{\"serializationError\":\"payload_unavailable\"}";
            }
        }
    }

    private static String errorCode(Throwable failure) {
        if (failure == null) return "SHADOW_WORKER_FAILED";
        Throwable cursor = failure;
        while (cursor != null) {
            if (cursor instanceof java.sql.SQLException sql && sql.getSQLState() != null) {
                return safe("SQLSTATE_" + sql.getSQLState(), 80);
            }
            cursor = cursor.getCause();
        }
        return safe(failure.getClass().getSimpleName(), 80);
    }

    private static String firstNonBlank(String... values) {
        if (values == null) return null;
        for (String value : values) if (value != null && !value.isBlank()) return value;
        return null;
    }

    private static String safe(String value, int maxLength) {
        if (value == null) return null;
        String clean = value.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ').trim();
        return clean.length() <= maxLength ? clean : clean.substring(0, maxLength);
    }
}
