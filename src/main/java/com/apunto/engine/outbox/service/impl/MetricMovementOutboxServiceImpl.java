package com.apunto.engine.outbox.service.impl;

import com.apunto.engine.entity.OperationMovementEventEntity;
import com.apunto.engine.outbox.dto.MetricMovementPersistedEvent;
import com.apunto.engine.outbox.service.MetricMovementOutboxService;
import com.apunto.engine.outbox.exception.MetricOutboxHashException;
import com.apunto.engine.outbox.exception.MetricOutboxSerializationException;
import com.apunto.engine.shared.util.LogFmt;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Locale;

@Slf4j
@Service
@RequiredArgsConstructor
public class MetricMovementOutboxServiceImpl implements MetricMovementOutboxService {

    private static final String EVENT_TYPE = "operation-movement-persisted-v1";
    private static final String EVENT_VERSION = "1";

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    @Value("${metric.outbox.enabled:false}")
    private boolean enabled;

    @Override
    public void enqueue(OperationMovementEventEntity entity) {
        if (!enabled || entity == null) {
            return;
        }
        if (!StringUtils.hasText(entity.getMovementKey()) || !StringUtils.hasText(entity.getIdWalletOrigin())) {
            log.warn("event=metric_outbox.skip reason=payload_incomplete movementKey={} wallet={}",
                    safe(entity.getMovementKey()), safe(entity.getIdWalletOrigin()));
            return;
        }
        MetricMovementPersistedEvent event = toEvent(entity);
        String payload = serialize(event);
        insertOutbox(entity, payload);
    }

    private void insertOutbox(OperationMovementEventEntity entity, String payload) {
        String wallet = normalizeWallet(entity.getIdWalletOrigin());
        try {
            jdbcTemplate.update(
                    """
                    INSERT INTO futuros_operaciones.metric_event_outbox(
                        event_type, aggregate_key, kafka_key, payload
                    ) VALUES (?, ?, ?, ?::jsonb)
                    """,
                    EVENT_TYPE,
                    entity.getMovementKey(),
                    wallet,
                    payload
            );
            log.debug("event=metric_outbox.enqueued movementKey={} wallet={} topicEvent={}",
                    safe(entity.getMovementKey()), safe(wallet), EVENT_TYPE);
        } catch (DataAccessException ex) {
            log.error("event=metric_outbox.enqueue_failed movementKey={} wallet={} errClass={} errMsg=\"{}\" {}",
                    safe(entity.getMovementKey()), safe(wallet), ex.getClass().getSimpleName(), safe(ex.getMessage()),
                    LogFmt.kv("component", "metric_outbox"), ex);
            throw ex;
        }
    }

    private MetricMovementPersistedEvent toEvent(OperationMovementEventEntity entity) {
        return new MetricMovementPersistedEvent(
                EVENT_VERSION,
                entity.getMovementKey(),
                movementHash(entity.getMovementKey()),
                entity.getPositionKey(),
                normalizeWallet(entity.getIdWalletOrigin()),
                upper(entity.getParsymbol()),
                upper(entity.getTypeOperation()),
                upper(entity.getEventType()),
                upper(entity.getDeltaType()),
                upper(entity.getStatus()),
                sourceForLog(entity.getSource()),
                sourceCategory(entity.getSource()),
                metricEligible(entity.getSource()),
                metricDecisionUse(entity.getSource()),
                entity.getEventTime(),
                entity.getDateCreation(),
                entity.getPreviousSizeQty(),
                entity.getResultingSizeQty(),
                entity.getDeltaSizeQty(),
                entity.getSizeQty(),
                entity.getNotionalUsd(),
                entity.getMarginUsedUsd(),
                entity.getEntryPrice(),
                entity.getExitPrice(),
                entity.getMarkPrice(),
                entity.getRealizedPnlUsd(),
                entity.getLeverage(),
                entity.getRawNotionalUsd(),
                entity.getPositionNotionalUsd(),
                entity.getClosedNotionalUsd(),
                entity.getClosedMarginUsedUsd(),
                entity.getEffectiveCloseQty(),
                entity.getEffectiveEntryPrice(),
                entity.getEffectiveExitPrice(),
                entity.getEffectiveRealizedPnlUsd(),
                entity.getNormalizationStatus(),
                entity.getNormalizationReason(),
                entity.getCopySubmittedTasks(),
                entity.getCopyBusinessSkipped(),
                entity.getCopyFallbackJobs(),
                entity.getCopyFallbackUsed()
        );
    }

    private String serialize(MetricMovementPersistedEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException ex) {
            throw new MetricOutboxSerializationException("No se pudo serializar operation-movement-persisted-v1", ex);
        }
    }

    private String movementHash(String movementKey) {
        if (!StringUtils.hasText(movementKey)) {
            return hashHex("missing-movement-key");
        }
        String prefix = "movement|sha256:";
        if (movementKey.startsWith(prefix) && movementKey.length() >= prefix.length() + 64) {
            return movementKey.substring(prefix.length(), prefix.length() + 64).toLowerCase(Locale.ROOT);
        }
        return hashHex(movementKey);
    }

    private String hashHex(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException ex) {
            throw new MetricOutboxHashException("SHA-256 no disponible", ex);
        }
    }

    private static final String SOURCE_DIRECT_INGEST = "hyperliquid_direct_ingest";
    private static final String SOURCE_COPY_JOB_INGEST = "copy_job_ingest";
    private static final String SOURCE_OPERATION_EVENT_INGEST = "operation_event_ingest";

    private boolean metricEligible(String source) {
        return SOURCE_DIRECT_INGEST.equals(sourceForLog(source));
    }

    private String metricDecisionUse(String source) {
        return metricEligible(source) ? "eligible_for_joyas_and_wallet_metrics" : "audit_only_excluded_from_joyas";
    }

    private String sourceCategory(String source) {
        String normalized = sourceForLog(source);
        if (SOURCE_DIRECT_INGEST.equals(normalized)) {
            return "ORIGINAL_WALLET_DATA";
        }
        if (SOURCE_COPY_JOB_INGEST.equals(normalized)) {
            return "DERIVED_COPY_TRADE";
        }
        return "OTHER_AUDIT_SOURCE";
    }

    private String sourceForLog(String source) {
        if (!StringUtils.hasText(source)) {
            return SOURCE_OPERATION_EVENT_INGEST;
        }
        return source.trim().toLowerCase(Locale.ROOT);
    }

    private String normalizeWallet(String wallet) {
        return wallet == null ? "" : wallet.trim().toLowerCase(Locale.ROOT);
    }

    private String upper(String value) {
        return value == null ? null : value.trim().toUpperCase(Locale.ROOT);
    }

    private String safe(Object value) {
        return value == null ? "null" : String.valueOf(value).replace('\n', '_').replace('\r', '_');
    }
}
