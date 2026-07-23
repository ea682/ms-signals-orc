package com.apunto.engine.service.impl;

import com.apunto.engine.dto.CopyOperationEventRecordCommand;
import com.apunto.engine.entity.CopyOperationEventEntity;
import com.apunto.engine.entity.UserCopyAllocationEntity;
import com.apunto.engine.repository.CopyOperationEventRepository;
import com.apunto.engine.repository.CopyEconomicCycleRepository;
import com.apunto.engine.outbox.service.MetricCopyOperationOutboxService;
import com.apunto.engine.service.CopyOperationEventService;
import com.apunto.engine.service.copy.quality.RoundTripExecutionQualityPersistenceService;
import com.apunto.engine.shared.metric.MetricStrategyIdentity;
import org.springframework.beans.factory.annotation.Autowired;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Objects;
import java.util.List;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class CopyOperationEventServiceImpl implements CopyOperationEventService {

    private final CopyOperationEventRepository repository;
    private final CopyEconomicCycleRepository copyEconomicCycleRepository;
    private final MetricCopyOperationOutboxService metricCopyOperationOutboxService;
    private RoundTripExecutionQualityPersistenceService roundTripQualityService;

    @Autowired(required = false)
    void setRoundTripQualityService(RoundTripExecutionQualityPersistenceService value) {
        this.roundTripQualityService = value;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void record(CopyOperationEventRecordCommand command) {
        persist(command, false, false);
    }

    @Override
    @Transactional
    public UUID recordRequired(CopyOperationEventRecordCommand command) {
        return persist(command, true, true);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public UUID recordReconciliationRequired(CopyOperationEventRecordCommand command) {
        return persist(command, true, false);
    }

    @Override
    @Transactional(readOnly = true)
    public String findEventType(UUID eventId) {
        if (eventId == null) {
            return null;
        }
        return repository.findById(eventId)
                .map(CopyOperationEventEntity::getEventType)
                .orElse(null);
    }

    private UUID persist(CopyOperationEventRecordCommand command,
                         boolean required,
                         boolean dispatchIntentRequired) {
        if (!isRecordable(command)
                || (dispatchIntentRequired && command.getDispatchIntentId() == null)) {
            log.warn("event=copy_operation_event.skip category=audit reasonAlias=ledger_payload_incomplete friendlyReason=evento_de_historial_incompleto explanation=no_se_guardo_el_evento_porque_faltan_campos_minimos copyImpact=ledger_missing traceId={} originId={} userId={} symbol={} eventType={}",
                    safe(command == null ? null : command.getTraceId()),
                    safe(command == null ? null : command.getIdOrderOrigin()),
                    safe(command == null ? null : command.getIdUser()),
                    safe(command == null ? null : command.getParsymbol()),
                    safe(command == null ? null : command.getEventType()));
            if (required) {
                throw new IllegalArgumentException(
                        "Required copy_operation_event is missing minimum ledger fields");
            }
            return null;
        }

        // Durable intents may generate multiple cumulative partial-fill progress
        // events. Legacy events without an intent retain clientOrderId idempotency.
        if (command.getDispatchIntentId() == null && StringUtils.hasText(command.getClientOrderId())) {
            var existing = repository.findByClientOrderId(command.getClientOrderId());
            if (existing.isPresent()) {
                log.info("event=copy_operation_event.idempotent category=audit reasonAlias=client_order_already_recorded friendlyReason=evento_ya_registrado explanation=el_clientOrderId_ya_existia_en_el_historial_y_no_se_duplica copyImpact=ledger_idempotent traceId={} originId={} userId={} symbol={} eventType={} clientOrderId={} existingEventId={}",
                        safe(command.getTraceId()), safe(command.getIdOrderOrigin()), safe(command.getIdUser()), safe(command.getParsymbol()),
                        safe(command.getEventType()), safe(command.getClientOrderId()), existing.get().getIdEvent());
                return existing.get().getIdEvent();
            }
        }

        if (command.getDispatchIntentId() != null) {
            repository.lockDispatchProgress(dispatchProgressLockKey(command));
        }
        var existingProgress = command.getDispatchIntentId() == null ? java.util.Optional.<CopyOperationEventEntity>empty()
                : repository.findDispatchProgress(command.getDispatchIntentId(), command.getEventType(),
                command.getQtyExecuted(), command.getResultingQty());
        if (existingProgress.isPresent()) {
            CopyOperationEventEntity existing = existingProgress.get();
            if (mergeEconomicEvidence(existing, command)) {
                repository.saveAndFlush(existing);
                metricCopyOperationOutboxService.enqueue(existing);
                recalculateRoundTrip(existing);
                log.info("event=copy_operation_event.economic_evidence_upgraded dispatchIntentId={} eventId={} economicDataStatus={} tradeCount={} totalFees={} grossRealizedPnl={}",
                        command.getDispatchIntentId(), existing.getIdEvent(), existing.getEconomicDataStatus(),
                        existing.getTradeIds() == null ? 0 : existing.getTradeIds().size(), existing.getTotalFees(),
                        existing.getGrossRealizedPnl());
            }
            log.info("event=copy_operation_event.idempotent category=audit reasonAlias=dispatch_progress_already_recorded copyImpact=ledger_idempotent dispatchIntentId={} traceId={} originId={} userId={} symbol={} eventType={} qtyExecuted={} resultingQty={}",
                    command.getDispatchIntentId(), safe(command.getTraceId()), safe(command.getIdOrderOrigin()),
                    safe(command.getIdUser()), safe(command.getParsymbol()), safe(command.getEventType()),
                    command.getQtyExecuted(), command.getResultingQty());
            return existing.getIdEvent();
        }

        CopyOperationEventEntity entity = toEntity(command);
        try {
            repository.saveAndFlush(entity);
            metricCopyOperationOutboxService.enqueue(entity);
            recalculateRoundTrip(entity);
            log.info("event=copy_operation_event.insert_ok category=audit reasonAlias=ledger_event_recorded friendlyReason=historial_actualizado explanation=se_guardo_el_movimiento_de_la_copia_para_reconstruir_pnl copyImpact=ledger_tracked traceId={} originId={} userId={} wallet={} symbol={} eventType={} copyIntent={} orderId={} clientOrderId={} qtyExecuted={} price={} resultingQty={} realizedPnlUsd={}",
                    safe(command.getTraceId()), safe(command.getIdOrderOrigin()), safe(command.getIdUser()), safe(command.getIdWalletOrigin()), safe(command.getParsymbol()),
                    safe(command.getEventType()), safe(command.getCopyIntent()), safe(command.getBinanceOrderId()), safe(command.getClientOrderId()),
                    command.getQtyExecuted(), command.getPrice(), command.getResultingQty(), command.getRealizedPnlUsd());
            return entity.getIdEvent();
        } catch (DataIntegrityViolationException ex) {
            if (isUniqueViolation(ex) && StringUtils.hasText(command.getClientOrderId())) {
                log.info("event=copy_operation_event.insert_duplicate category=audit reasonAlias=ledger_duplicate_ignored friendlyReason=historial_ya_tenia_el_movimiento explanation=se_ignora_duplicado_por_clientOrderId copyImpact=ledger_idempotent traceId={} originId={} userId={} symbol={} eventType={} clientOrderId={}",
                        safe(command.getTraceId()), safe(command.getIdOrderOrigin()), safe(command.getIdUser()), safe(command.getParsymbol()),
                        safe(command.getEventType()), safe(command.getClientOrderId()));
                return repository.findByClientOrderId(command.getClientOrderId())
                        .map(CopyOperationEventEntity::getIdEvent)
                        .orElse(null);
            }
            log.error("event=copy_operation_event.insert_failed category=audit reasonAlias=ledger_insert_failed friendlyReason=no_se_pudo_guardar_historial explanation=fallo_bd_al_guardar_evento_de_copy_operation copyImpact=ledger_missing traceId={} originId={} userId={} symbol={} eventType={} errClass={} errMsg=\"{}\"",
                    safe(command.getTraceId()), safe(command.getIdOrderOrigin()), safe(command.getIdUser()), safe(command.getParsymbol()),
                    safe(command.getEventType()), ex.getClass().getSimpleName(), safe(ex.getMessage()));
            if (required) throw ex;
            return null;
        } catch (DataAccessException | IllegalStateException | IllegalArgumentException ex) {
            log.error("event=copy_operation_event.insert_failed category=audit reasonAlias=ledger_insert_failed friendlyReason=no_se_pudo_guardar_historial explanation=fallo_bd_al_guardar_evento_de_copy_operation copyImpact=ledger_missing traceId={} originId={} userId={} symbol={} eventType={} errClass={} errMsg=\"{}\"",
                    safe(command.getTraceId()), safe(command.getIdOrderOrigin()), safe(command.getIdUser()), safe(command.getParsymbol()),
                    safe(command.getEventType()), ex.getClass().getSimpleName(), safe(ex.getMessage()));
            if (required) throw ex;
            return null;
        }
    }

    private boolean isRecordable(CopyOperationEventRecordCommand command) {
        return command != null
                && StringUtils.hasText(command.getIdOrderOrigin())
                && StringUtils.hasText(command.getIdUser())
                && StringUtils.hasText(command.getIdWalletOrigin())
                && StringUtils.hasText(command.getParsymbol())
                && StringUtils.hasText(command.getTypeOperation())
                && StringUtils.hasText(command.getEventType());
    }

    private String dispatchProgressLockKey(CopyOperationEventRecordCommand command) {
        return "copy_event_progress|" + command.getDispatchIntentId()
                + '|' + safeKey(command.getEventType())
                + '|' + canonical(command.getQtyExecuted())
                + '|' + canonical(command.getResultingQty());
    }

    private String canonical(java.math.BigDecimal value) {
        return value == null ? "0" : value.stripTrailingZeros().toPlainString();
    }

    private String safeKey(String value) {
        return value == null ? "" : value.trim().toUpperCase(java.util.Locale.ROOT);
    }

    private CopyOperationEventEntity toEntity(CopyOperationEventRecordCommand command) {
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        UUID economicCycleId = command.getEconomicCycleId();
        if (economicCycleId == null && command.getIdOperation() != null) {
            economicCycleId = copyEconomicCycleRepository.findByCopyOperationId(command.getIdOperation())
                    .map(com.apunto.engine.entity.CopyEconomicCycleEntity::getCycleId)
                    .orElse(null);
        }
        return CopyOperationEventEntity.builder()
                .idOperation(command.getIdOperation())
                .economicCycleId(economicCycleId)
                .exchangeAccountId(command.getExchangeAccountId())
                .sourcePositionCycleId(command.getSourcePositionCycleId())
                .dispatchIntentId(command.getDispatchIntentId())
                .executionIntentId(command.getDispatchIntentId())
                .userCopyAllocationId(command.getUserCopyAllocationId())
                .copyStrategyCode(normalizeStrategy(command.getCopyStrategyCode()))
                .scopeType(MetricStrategyIdentity.scopeType(command.getScopeType(), command.getCopyStrategyCode()))
                .scopeValue(MetricStrategyIdentity.scopeValue(command.getScopeValue(), command.getCopyStrategyCode()))
                .strategyKey(strategyKey(command))
                .metricGenerationId(trim(command.getGenerationId()))
                .executionMode(normalizeExecutionMode(command.getExecutionMode()))
                .shadow(Boolean.TRUE.equals(command.getShadow()))
                .decision(safeDecision(command.getDecision()))
                .decisionReason(safeReason(command.getDecisionReason()))
                .sourceMovementKey(safeReason(command.getSourceMovementKey()))
                .idOrderOrigin(command.getIdOrderOrigin())
                .idUser(command.getIdUser())
                .idWalletOrigin(command.getIdWalletOrigin())
                .parsymbol(command.getParsymbol())
                .typeOperation(command.getTypeOperation())
                .eventType(command.getEventType())
                .copyIntent(command.getCopyIntent())
                .binanceOrderId(command.getBinanceOrderId())
                .clientOrderId(command.getClientOrderId())
                .side(command.getSide())
                .positionSide(command.getPositionSide())
                .qtyRequested(command.getQtyRequested())
                .qtyExecuted(command.getQtyExecuted())
                .price(command.getPrice())
                .priceStatus(command.getPriceStatus())
                .notionalUsd(command.getNotionalUsd())
                .previousQty(command.getPreviousQty())
                .resultingQty(command.getResultingQty())
                .realizedPnlUsd(command.getRealizedPnlUsd())
                .feeUsd(command.getFeeUsd())
                .tradeIds(command.getTradeIds())
                .requestedQuantity(command.getQtyRequested())
                .executedQuantity(command.getQtyExecuted())
                .averageFillPrice(command.getAverageFillPrice())
                .individualFills(command.getIndividualFills())
                .entryPrice(command.getEntryPrice())
                .exitPrice(command.getExitPrice())
                .entryFee(command.getEntryFee())
                .exitFee(command.getExitFee())
                .totalFees(command.getTotalFees())
                .fundingPaid(command.getFundingPaid())
                .fundingReceived(command.getFundingReceived())
                .netFunding(command.getNetFunding())
                .grossRealizedPnl(command.getGrossRealizedPnl())
                .netRealizedPnl(command.getNetRealizedPnl())
                .unrealizedPnl(command.getUnrealizedPnl())
                .expectedPrice(command.getExpectedPrice())
                .actualPrice(command.getActualPrice())
                .slippageBps(command.getSlippageBps())
                .slippageUsd(command.getSlippageUsd())
                .submittedAt(command.getSubmittedAt())
                .acceptedAt(command.getAcceptedAt())
                .filledAt(command.getFilledAt())
                .persistedAt(now)
                .sourceToSubmitLatencyMs(command.getSourceToSubmitLatencyMs())
                .submitToFillLatencyMs(command.getSubmitToFillLatencyMs())
                .endToEndLatencyMs(command.getEndToEndLatencyMs())
                .economicDataStatus(normalizeEconomicDataStatus(command.getEconomicDataStatus()))
                .strategyVersion(command.getStrategyVersion())
                .sizingPolicyVersion(command.getSizingPolicyVersion())
                .symbolMappingVersion(command.getSymbolMappingVersion())
                .feeModelVersion(command.getFeeModelVersion())
                .fundingModelVersion(command.getFundingModelVersion())
                .slippageModelVersion(command.getSlippageModelVersion())
                .liquidityModelVersion(command.getLiquidityModelVersion())
                .calibrationCapitalUsd(command.getCalibrationCapitalUsd())
                .targetLeverage(command.getTargetLeverage())
                .calibrationTargetNotionalUsd(command.getCalibrationTargetNotionalUsd())
                .copyAction(normalizeToken(command.getCopyAction() == null
                        ? command.getCopyIntent() : command.getCopyAction()))
                .notionalBand(normalizeToken(command.getNotionalBand()))
                .traceId(command.getTraceId())
                .source(command.getSource())
                .reasonCode(command.getReasonCode())
                .eventTime(Objects.requireNonNullElse(command.getEventTime(), now))
                .dateCreation(now)
                .build();
    }

    private String normalizeEconomicDataStatus(String value) {
        if (!StringUtils.hasText(value)) {
            return "PENDING_RECONCILIATION";
        }
        String normalized = value.trim().toUpperCase(java.util.Locale.ROOT);
        return switch (normalized) {
            case "KNOWN", "UNAVAILABLE" -> normalized;
            default -> "PENDING_RECONCILIATION";
        };
    }

    private boolean mergeEconomicEvidence(CopyOperationEventEntity entity,
                                          CopyOperationEventRecordCommand command) {
        boolean changed = false;
        if (entity.getIdOperation() == null && command.getIdOperation() != null) {
            entity.setIdOperation(command.getIdOperation());
            changed = true;
        }
        if (entity.getEconomicCycleId() == null && command.getEconomicCycleId() != null) {
            entity.setEconomicCycleId(command.getEconomicCycleId());
            changed = true;
        }
        if (command.getTradeIds() != null && !command.getTradeIds().isEmpty()
                && !Objects.equals(entity.getTradeIds(), command.getTradeIds())) {
            entity.setTradeIds(List.copyOf(command.getTradeIds()));
            changed = true;
        }
        if (command.getIndividualFills() != null && !command.getIndividualFills().isEmpty()
                && !Objects.equals(entity.getIndividualFills(), command.getIndividualFills())) {
            entity.setIndividualFills(List.copyOf(command.getIndividualFills()));
            changed = true;
        }
        changed |= copyBaseEconomicEvidence(entity, command);
        changed |= copyEconomicNumbers(entity, command);
        changed |= copyEconomicTimes(entity, command);
        changed |= copyEconomicVersions(entity, command);
        if (StringUtils.hasText(command.getEconomicDataStatus())) {
            String incomingStatus = normalizeEconomicDataStatus(command.getEconomicDataStatus());
            if (economicStatusRank(incomingStatus) > economicStatusRank(entity.getEconomicDataStatus())) {
                entity.setEconomicDataStatus(incomingStatus);
                changed = true;
            }
        }
        return changed;
    }

    private void recalculateRoundTrip(CopyOperationEventEntity event) {
        if (roundTripQualityService == null || event == null) return;
        try {
            roundTripQualityService.recalculate(event.getUserCopyAllocationId(), event.getEconomicCycleId());
        } catch (RuntimeException ex) {
            log.warn("event=copy.round_trip_execution_quality_failed allocationId={} economicCycleId={} errClass={} reasonCode=ROUND_TRIP_AUDIT_RECALCULATION_FAILED",
                    event.getUserCopyAllocationId(), event.getEconomicCycleId(), ex.getClass().getSimpleName());
        }
    }

    private boolean copyBaseEconomicEvidence(CopyOperationEventEntity entity,
                                             CopyOperationEventRecordCommand command) {
        boolean changed = false;
        int currentPriceRank = priceStatusRank(entity.getPriceStatus(), entity.getPrice());
        int incomingPriceRank = priceStatusRank(command.getPriceStatus(), command.getPrice());
        boolean authoritativePriceUpgrade = command.getPrice() != null
                && (entity.getPrice() == null || incomingPriceRank > currentPriceRank);
        if (authoritativePriceUpgrade) {
            entity.setPrice(command.getPrice());
            changed = true;
            if (command.getNotionalUsd() != null
                    && (entity.getNotionalUsd() == null
                    || command.getNotionalUsd().compareTo(entity.getNotionalUsd()) != 0)) {
                entity.setNotionalUsd(command.getNotionalUsd());
                changed = true;
            }
        } else if (entity.getNotionalUsd() == null && command.getNotionalUsd() != null) {
            entity.setNotionalUsd(command.getNotionalUsd());
            changed = true;
        }
        if (StringUtils.hasText(command.getPriceStatus())
                && (entity.getPriceStatus() == null || incomingPriceRank > currentPriceRank)) {
            entity.setPriceStatus(command.getPriceStatus().trim().toUpperCase(java.util.Locale.ROOT));
            changed = true;
        }
        changed |= setNumber(command.getRealizedPnlUsd(), entity.getRealizedPnlUsd(), entity::setRealizedPnlUsd);
        changed |= setNumber(command.getFeeUsd(), entity.getFeeUsd(), entity::setFeeUsd);
        return changed;
    }

    private int priceStatusRank(String status, java.math.BigDecimal price) {
        if ("AVAILABLE".equalsIgnoreCase(status)) return 2;
        if ("PENDING_RESOLUTION".equalsIgnoreCase(status)) return 1;
        return price != null && price.compareTo(java.math.BigDecimal.ZERO) > 0 ? 1 : 0;
    }

    private boolean copyEconomicNumbers(CopyOperationEventEntity entity,
                                        CopyOperationEventRecordCommand command) {
        boolean changed = false;
        changed |= setNumber(command.getAverageFillPrice(), entity.getAverageFillPrice(), entity::setAverageFillPrice);
        changed |= setNumber(command.getEntryPrice(), entity.getEntryPrice(), entity::setEntryPrice);
        changed |= setNumber(command.getExitPrice(), entity.getExitPrice(), entity::setExitPrice);
        changed |= setNumber(command.getEntryFee(), entity.getEntryFee(), entity::setEntryFee);
        changed |= setNumber(command.getExitFee(), entity.getExitFee(), entity::setExitFee);
        changed |= setNumber(command.getTotalFees(), entity.getTotalFees(), entity::setTotalFees);
        changed |= setNumber(command.getFundingPaid(), entity.getFundingPaid(), entity::setFundingPaid);
        changed |= setNumber(command.getFundingReceived(), entity.getFundingReceived(), entity::setFundingReceived);
        changed |= setNumber(command.getNetFunding(), entity.getNetFunding(), entity::setNetFunding);
        changed |= setNumber(command.getGrossRealizedPnl(), entity.getGrossRealizedPnl(), entity::setGrossRealizedPnl);
        changed |= setNumber(command.getNetRealizedPnl(), entity.getNetRealizedPnl(), entity::setNetRealizedPnl);
        changed |= setNumber(command.getUnrealizedPnl(), entity.getUnrealizedPnl(), entity::setUnrealizedPnl);
        changed |= setNumber(command.getExpectedPrice(), entity.getExpectedPrice(), entity::setExpectedPrice);
        changed |= setNumber(command.getActualPrice(), entity.getActualPrice(), entity::setActualPrice);
        changed |= setNumber(command.getSlippageBps(), entity.getSlippageBps(), entity::setSlippageBps);
        changed |= setNumber(command.getSlippageUsd(), entity.getSlippageUsd(), entity::setSlippageUsd);
        return changed;
    }

    private boolean copyEconomicTimes(CopyOperationEventEntity entity,
                                      CopyOperationEventRecordCommand command) {
        boolean changed = false;
        changed |= setValue(command.getSubmittedAt(), entity.getSubmittedAt(), entity::setSubmittedAt);
        changed |= setValue(command.getAcceptedAt(), entity.getAcceptedAt(), entity::setAcceptedAt);
        changed |= setValue(command.getFilledAt(), entity.getFilledAt(), entity::setFilledAt);
        changed |= setValue(command.getSourceToSubmitLatencyMs(), entity.getSourceToSubmitLatencyMs(), entity::setSourceToSubmitLatencyMs);
        changed |= setValue(command.getSubmitToFillLatencyMs(), entity.getSubmitToFillLatencyMs(), entity::setSubmitToFillLatencyMs);
        changed |= setValue(command.getEndToEndLatencyMs(), entity.getEndToEndLatencyMs(), entity::setEndToEndLatencyMs);
        return changed;
    }

    private boolean copyEconomicVersions(CopyOperationEventEntity entity,
                                         CopyOperationEventRecordCommand command) {
        boolean changed = false;
        if (StringUtils.hasText(command.getScopeType())) {
            changed |= setValue(command.getScopeType(), entity.getScopeType(), entity::setScopeType);
        }
        if (StringUtils.hasText(command.getScopeValue())) {
            changed |= setValue(command.getScopeValue(), entity.getScopeValue(), entity::setScopeValue);
        }
        if (StringUtils.hasText(command.getStrategyKey())) {
            changed |= setValue(command.getStrategyKey().trim(), entity.getStrategyKey(), entity::setStrategyKey);
        }
        if (StringUtils.hasText(command.getGenerationId())) {
            changed |= setValue(command.getGenerationId().trim(), entity.getMetricGenerationId(), entity::setMetricGenerationId);
        }
        changed |= setValue(command.getStrategyVersion(), entity.getStrategyVersion(), entity::setStrategyVersion);
        changed |= setValue(command.getSizingPolicyVersion(), entity.getSizingPolicyVersion(), entity::setSizingPolicyVersion);
        changed |= setValue(command.getSymbolMappingVersion(), entity.getSymbolMappingVersion(), entity::setSymbolMappingVersion);
        changed |= setValue(command.getFeeModelVersion(), entity.getFeeModelVersion(), entity::setFeeModelVersion);
        changed |= setValue(command.getFundingModelVersion(), entity.getFundingModelVersion(), entity::setFundingModelVersion);
        changed |= setValue(command.getSlippageModelVersion(), entity.getSlippageModelVersion(), entity::setSlippageModelVersion);
        changed |= setValue(command.getLiquidityModelVersion(), entity.getLiquidityModelVersion(), entity::setLiquidityModelVersion);
        changed |= setNumber(command.getCalibrationCapitalUsd(), entity.getCalibrationCapitalUsd(), entity::setCalibrationCapitalUsd);
        changed |= setNumber(command.getTargetLeverage(), entity.getTargetLeverage(), entity::setTargetLeverage);
        changed |= setNumber(command.getCalibrationTargetNotionalUsd(), entity.getCalibrationTargetNotionalUsd(), entity::setCalibrationTargetNotionalUsd);
        changed |= setValue(normalizeToken(command.getCopyAction()), entity.getCopyAction(), entity::setCopyAction);
        changed |= setValue(normalizeToken(command.getNotionalBand()), entity.getNotionalBand(), entity::setNotionalBand);
        return changed;
    }

    private boolean setNumber(java.math.BigDecimal incoming,
                              java.math.BigDecimal current,
                              java.util.function.Consumer<java.math.BigDecimal> setter) {
        if (incoming == null || (current != null && incoming.compareTo(current) == 0)) return false;
        setter.accept(incoming);
        return true;
    }

    private <T> boolean setValue(T incoming, T current, java.util.function.Consumer<T> setter) {
        if (incoming == null || Objects.equals(incoming, current)) return false;
        setter.accept(incoming);
        return true;
    }

    private int economicStatusRank(String status) {
        if ("KNOWN".equalsIgnoreCase(status)) return 2;
        if ("PENDING_RECONCILIATION".equalsIgnoreCase(status)) return 1;
        return 0;
    }

    private boolean isUniqueViolation(DataIntegrityViolationException ex) {
        Throwable t = ex;
        while (t != null) {
            String msg = t.getMessage();
            if (msg != null) {
                String normalized = msg.toLowerCase();
                if (normalized.contains("23505")
                        || normalized.contains("duplicate key value violates unique constraint")
                        || normalized.contains("ux_copy_operation_event_client_order_id")
                        || normalized.contains("ux_copy_operation_event_dispatch_progress")) {
                    return true;
                }
            }
            t = t.getCause();
        }
        return false;
    }

    private String normalizeExecutionMode(String mode) {
        return UserCopyAllocationEntity.normalizeExecutionMode(mode);
    }

    private String normalizeStrategy(String strategy) {
        if (strategy == null || strategy.isBlank()) return null;
        return strategy.trim().toUpperCase(java.util.Locale.ROOT).replace('-', '_');
    }

    private String strategyKey(CopyOperationEventRecordCommand command) {
        if (StringUtils.hasText(command.getStrategyKey())) return command.getStrategyKey().trim();
        if (!StringUtils.hasText(command.getIdWalletOrigin())) return null;
        return MetricStrategyIdentity.canonicalKey(
                command.getIdWalletOrigin(), command.getCopyStrategyCode(),
                command.getScopeType(), command.getScopeValue());
    }

    private String trim(String value) {
        return StringUtils.hasText(value) ? value.trim() : null;
    }

    private String normalizeToken(String value) {
        return StringUtils.hasText(value)
                ? value.trim().toUpperCase(java.util.Locale.ROOT).replace('-', '_')
                : null;
    }

    private String safeDecision(String value) {
        if (!StringUtils.hasText(value)) return null;
        String normalized = value.trim().toUpperCase(java.util.Locale.ROOT).replace('-', '_');
        return normalized.length() > 40 ? normalized.substring(0, 40) : normalized;
    }

    private String safeReason(String value) {
        if (!StringUtils.hasText(value)) return null;
        String clean = value.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ').replace('\"', '\'').trim();
        return clean.length() > 160 ? clean.substring(0, 160) : clean;
    }

    private String safe(String value) {
        if (!StringUtils.hasText(value)) {
            return "NA";
        }
        String clean = value.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ').replace('"', '\'');
        return clean.length() > 1000 ? clean.substring(0, 1000) : clean;
    }
}
