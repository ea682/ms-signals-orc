package com.apunto.engine.outbox.service.impl;

import com.apunto.engine.entity.CopyOperationEventEntity;
import com.apunto.engine.outbox.dto.MetricCopyOperationPersistedEvent;
import com.apunto.engine.outbox.exception.MetricOutboxSerializationException;
import com.apunto.engine.outbox.service.MetricCopyOperationOutboxService;
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

import java.util.Locale;

@Slf4j
@Service
@RequiredArgsConstructor
public class MetricCopyOperationOutboxServiceImpl implements MetricCopyOperationOutboxService {
    private static final String EVENT_TYPE = "copy-operation-event-persisted-v1";
    private static final String EVENT_VERSION = "4";

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    @Value("${metric.outbox.copy-event.enabled:false}")
    private boolean enabled;

    @Override
    public void enqueue(CopyOperationEventEntity entity) {
        if (!enabled || entity == null) {
            return;
        }
        if (!isPublishable(entity)) {
            log.warn("event=metric_copy_outbox.skip reason=payload_incomplete idEvent={} userId={} wallet={}",
                    safe(entity == null ? null : entity.getIdEvent()),
                    safe(entity == null ? null : entity.getIdUser()),
                    safe(entity == null ? null : entity.getIdWalletOrigin()));
            return;
        }
        enqueue(toEvent(entity));
    }

    @Override
    public void enqueue(MetricCopyOperationPersistedEvent event) {
        if (!enabled || event == null) return;
        if (!isPublishable(event)) {
            log.warn("event=metric_copy_outbox.skip reason=payload_incomplete idEvent={} userId={} wallet={}",
                    safe(event.idEvent()), safe(event.idUser()), safe(event.wallet()));
            return;
        }
        insertOutbox(event, serialize(event));
    }

    private boolean isPublishable(CopyOperationEventEntity entity) {
        return entity.getIdEvent() != null
                && StringUtils.hasText(entity.getIdUser())
                && StringUtils.hasText(entity.getIdWalletOrigin())
                && StringUtils.hasText(entity.getEventType());
    }

    private boolean isPublishable(MetricCopyOperationPersistedEvent event) {
        return event.idEvent() != null
                && StringUtils.hasText(event.idUser())
                && StringUtils.hasText(event.wallet())
                && StringUtils.hasText(event.eventType());
    }

    private void insertOutbox(MetricCopyOperationPersistedEvent event, String payload) {
        try {
            jdbcTemplate.update(
                    """
                    INSERT INTO futuros_operaciones.metric_event_outbox(
                      event_type, aggregate_key, kafka_key, payload
                    ) VALUES (?, ?, ?, ?::jsonb)
                    """,
                    EVENT_TYPE,
                    aggregateKey(event),
                    kafkaKey(event),
                    payload
            );
            log.debug("event=metric_copy_outbox.enqueued idEvent={} userId={} wallet={}",
                    safe(event.idEvent()), safe(event.idUser()), safe(event.wallet()));
        } catch (DataAccessException ex) {
            log.error("event=metric_copy_outbox.enqueue_failed idEvent={} userId={} wallet={} errClass={} errMsg=\"{}\" {}",
                    safe(event.idEvent()), safe(event.idUser()), safe(event.wallet()),
                    ex.getClass().getSimpleName(), safe(ex.getMessage()), LogFmt.kv("component", "metric_copy_outbox"), ex);
            throw ex;
        }
    }

    private MetricCopyOperationPersistedEvent toEvent(CopyOperationEventEntity entity) {
        return new MetricCopyOperationPersistedEvent(
                EVENT_VERSION,
                entity.getIdEvent(),
                entity.getIdOperation(),
                entity.getIdOrderOrigin(),
                entity.getSourceMovementKey(),
                entity.getIdUser(),
                normalize(entity.getIdWalletOrigin()),
                upper(entity.getParsymbol()),
                upper(entity.getTypeOperation()),
                upper(entity.getEventType()),
                upper(entity.getCopyIntent()),
                entity.getBinanceOrderId(),
                entity.getClientOrderId(),
                upper(entity.getSide()),
                upper(entity.getPositionSide()),
                entity.getQtyRequested(),
                entity.getQtyExecuted(),
                entity.getPrice(),
                entity.getNotionalUsd(),
                entity.getPreviousQty(),
                entity.getResultingQty(),
                entity.getRealizedPnlUsd(),
                entity.getFeeUsd(),
                entity.getTraceId(),
                entity.getSource(),
                entity.getReasonCode(),
                entity.getEventTime(),
                entity.getDateCreation(),
                entity.getEconomicCycleId(),
                entity.getDispatchIntentId(),
                entity.getUserCopyAllocationId(),
                entity.getCopyStrategyCode(),
                entity.getScopeType(),
                entity.getScopeValue(),
                entity.getStrategyKey(),
                entity.getMetricGenerationId(),
                publishedExecutionMode(entity),
                entity.getDecision(),
                entity.getDecisionReason(),
                entity.getPriceStatus(),
                entity.getTradeIds(),
                entity.getIndividualFills(),
                entity.getAverageFillPrice(),
                entity.getEntryPrice(),
                entity.getExitPrice(),
                entity.getEntryFee(),
                entity.getExitFee(),
                entity.getTotalFees(),
                entity.getFundingPaid(),
                entity.getFundingReceived(),
                entity.getNetFunding(),
                entity.getGrossRealizedPnl(),
                entity.getNetRealizedPnl(),
                entity.getUnrealizedPnl(),
                entity.getExpectedPrice(),
                entity.getActualPrice(),
                entity.getSlippageBps(),
                entity.getSlippageUsd(),
                entity.getSubmittedAt(),
                entity.getAcceptedAt(),
                entity.getFilledAt(),
                entity.getPersistedAt(),
                entity.getSourceToSubmitLatencyMs(),
                entity.getSubmitToFillLatencyMs(),
                entity.getEndToEndLatencyMs(),
                entity.getEconomicDataStatus(),
                entity.getStrategyVersion(),
                entity.getSizingPolicyVersion(),
                entity.getSymbolMappingVersion(),
                entity.getFeeModelVersion(),
                entity.getFundingModelVersion(),
                entity.getSlippageModelVersion(),
                entity.getLiquidityModelVersion(),
                entity.getCalibrationCapitalUsd(),
                entity.getTargetLeverage(),
                entity.getCalibrationTargetNotionalUsd(),
                entity.getCopyAction(),
                entity.getNotionalBand()
        );
    }

    private String serialize(MetricCopyOperationPersistedEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException ex) {
            throw new MetricOutboxSerializationException("No se pudo serializar copy-operation-event-persisted-v1", ex);
        }
    }

    private String aggregateKey(MetricCopyOperationPersistedEvent event) {
        if (StringUtils.hasText(event.clientOrderId())) {
            return event.clientOrderId();
        }
        return String.valueOf(event.idEvent());
    }

    private String kafkaKey(MetricCopyOperationPersistedEvent event) {
        return event.idUser() + "|" + normalize(event.wallet());
    }

    private String normalize(String value) {
        return value == null ? "" : value.trim().toLowerCase(Locale.ROOT);
    }

    private String upper(String value) {
        return value == null ? null : value.trim().toUpperCase(Locale.ROOT);
    }

    private String publishedExecutionMode(CopyOperationEventEntity entity) {
        if (entity != null && entity.isShadow()
                && "SHADOW".equalsIgnoreCase(entity.getExecutionMode())) {
            return "EXECUTABLE_SHADOW";
        }
        return entity == null ? null : upper(entity.getExecutionMode());
    }

    private String safe(Object value) {
        return value == null ? "null" : String.valueOf(value).replace('\n', '_').replace('\r', '_');
    }
}
