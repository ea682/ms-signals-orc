package com.apunto.engine.service.copy.dispatch;

import com.apunto.engine.dto.CopyOperationDto;
import com.apunto.engine.dto.CopyOperationEventRecordCommand;
import com.apunto.engine.dto.client.BinanceFuturesOrderClientResponse;
import com.apunto.engine.entity.CopyDispatchIntentEntity;
import com.apunto.engine.service.CopyOperationEventService;
import com.apunto.engine.service.CopyOperationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class CopyExecutionPersistenceService {

    private static final BigDecimal ZERO = BigDecimal.ZERO;
    private final CopyOperationService copyOperationService;
    private final CopyOperationEventService copyOperationEventService;
    private final CopyDispatchIntentStore intentStore;

    @Transactional
    public CopyOperationDto persistRecovered(CopyDispatchIntentEntity intent,
                                             BinanceFuturesOrderClientResponse response) {
        requireFilled(intent, response);
        CopyOperationDto linked = intent.getCopyOperationId() == null
                ? null
                : copyOperationService.findOperationById(intent.getCopyOperationId());
        validateLinkedOperation(intent, linked);
        String positionSide = firstNonBlank(
                intent.getPositionSide(), response.getPositionSide(),
                linked == null ? null : linked.getTypeOperation());
        CopyOperationDto current = linked != null ? linked : copyOperationService.findOperationForAllocation(
                intent.getIdOrderOrigin(), intent.getIdUser(), intent.getUserCopyAllocationId(),
                intent.getStrategyCode(), positionSide);
        String recordedEventType = intent.getCopyOperationEventId() == null
                ? null
                : copyOperationEventService.findEventType(intent.getCopyOperationEventId());
        String copyIntent = resolveIntent(
                intent.getCopyIntent(), intent.isReduceOnly(), current, recordedEventType);
        if (current == null && isExit(copyIntent)) {
            current = copyOperationService.findLatestOperationForAllocation(
                    intent.getIdOrderOrigin(), intent.getIdUser(), intent.getUserCopyAllocationId(),
                    intent.getStrategyCode(), positionSide);
        }
        BigDecimal cumulativeExecutedQty = positive(response.getExecutedQty()) ? response.getExecutedQty()
                : positive(response.getCumQty()) ? response.getCumQty() : intent.getExecutedQty();
        BigDecimal alreadyAppliedQty = nonNegative(intent.getPersistedExecutedQty());
        boolean stateAlreadyApplied = current != null
                && (linked != null
                || intent.getId().equals(current.getDispatchIntentId())
                || (isExit(copyIntent) && !current.isActive()
                && nonNegative(current.getSizePar()).compareTo(ZERO) == 0));
        if (!positive(alreadyAppliedQty) && stateAlreadyApplied) {
            // Local persistence may have committed immediately before a process crash.
            // Fail closed: do not apply the same cumulative fill a second time.
            alreadyAppliedQty = cumulativeExecutedQty;
        }
        BigDecimal stateDeltaQty = cumulativeExecutedQty.subtract(alreadyAppliedQty).max(ZERO);
        BigDecimal fillPrice = executionPrice(response, intent);
        String priceStatus = firstNonBlank(response.getAveragePriceStatus(), intent.getAveragePriceStatus(),
                positive(response.getAvgPrice()) ? "AVAILABLE" : "PENDING_RESOLUTION");
        BigDecimal previousQty = current == null ? ZERO : nonNegative(current.getSizePar());
        BigDecimal resultingQty;
        CopyOperationDto persisted;

        if (isOpen(copyIntent)) {
            if (current != null && current.isActive()) {
                resultingQty = previousQty.add(stateDeltaQty);
                boolean sameIntent = intent.getId().equals(current.getDispatchIntentId());
                BigDecimal entry = sameIntent && !"PENDING_RESOLUTION".equals(priceStatus)
                        ? fillPrice
                        : weightedEntry(current.getPriceEntry(), previousQty, fillPrice, stateDeltaQty);
                persisted = activeUpdate(current, intent, resultingQty, entry, priceStatus);
                copyOperationService.upsertActiveOperation(persisted);
            } else {
                // If the operation row itself is missing, reconstruct the cumulative
                // acknowledged fill rather than only its latest delta.
                resultingQty = cumulativeExecutedQty;
                persisted = newOpen(intent, response, positionSide, resultingQty, fillPrice, priceStatus);
                copyOperationService.upsertActiveOperation(persisted);
                persisted = reload(intent, positionSide, persisted);
            }
        } else if ("INCREASE".equals(copyIntent)) {
            if (current == null || !current.isActive()) {
                throw new IllegalStateException("Cannot recover INCREASE without active copy_operation");
            }
            resultingQty = previousQty.add(stateDeltaQty);
            BigDecimal entry = weightedEntry(current.getPriceEntry(), previousQty, fillPrice, stateDeltaQty);
            persisted = activeUpdate(current, intent, resultingQty, entry, priceStatus);
            copyOperationService.upsertActiveOperation(persisted);
        } else {
            if (current == null) {
                throw new IllegalStateException("Cannot recover reduction/close without active copy_operation");
            }
            if (!current.isActive()) {
                if (!stateAlreadyApplied) {
                    throw new IllegalStateException("Inactive copy_operation does not match the reconciled exit");
                }
                resultingQty = ZERO;
                persisted = current;
                if ("AVAILABLE".equalsIgnoreCase(priceStatus)) {
                    CopyOperationDto upgraded = copyOperationService.updateExecutionPriceEvidence(
                            current.getIdOperation(), fillPrice, priceStatus, true);
                    if (upgraded != null) {
                        persisted = upgraded;
                    }
                }
            } else {
                resultingQty = previousQty.subtract(stateDeltaQty).max(ZERO);
                if (resultingQty.compareTo(ZERO) == 0) {
                    persisted = closed(current, intent, fillPrice, priceStatus);
                    copyOperationService.closeOperation(persisted);
                } else {
                    persisted = activeUpdate(current, intent, resultingQty, current.getPriceEntry(), priceStatus);
                    copyOperationService.upsertActiveOperation(persisted);
                }
            }
        }

        BigDecimal persistedResultingQty = persisted.isActive()
                ? nonNegative(persisted.getSizePar()) : ZERO;
        BigDecimal ledgerPreviousQty = isOpen(copyIntent) || "INCREASE".equals(copyIntent)
                ? persistedResultingQty.subtract(cumulativeExecutedQty).max(ZERO)
                : persistedResultingQty.add(cumulativeExecutedQty);
        String recoveryReasonCode = recoveryReasonCode(intent, response);

        UUID requiredEventId = copyOperationEventService.recordRequired(CopyOperationEventRecordCommand.builder()
                .idOperation(persisted.getIdOperation())
                .economicCycleId(persisted.getEconomicCycleId())
                .dispatchIntentId(intent.getId())
                .userCopyAllocationId(intent.getUserCopyAllocationId())
                .copyStrategyCode(intent.getStrategyCode())
                .executionMode(intent.getExecutionMode())
                .shadow(false)
                .decision("RECOVERED_" + eventType(copyIntent, persisted.isActive()))
                .decisionReason(recoveryReasonCode.toLowerCase(java.util.Locale.ROOT))
                .sourceMovementKey(intent.getSourceEventId())
                .idOrderOrigin(intent.getIdOrderOrigin())
                .idUser(intent.getIdUser())
                .idWalletOrigin(intent.getWalletId())
                .parsymbol(intent.getSymbol())
                .typeOperation(positionSide)
                .eventType(eventType(copyIntent, persisted.isActive()))
                .copyIntent(copyIntent)
                .binanceOrderId(String.valueOf(response.getOrderId()))
                .clientOrderId(intent.getClientOrderId())
                .side(firstNonBlank(intent.getSide(), response.getSide()))
                .positionSide(positionSide)
                .qtyRequested(intent.getRequestedQty())
                .qtyExecuted(cumulativeExecutedQty)
                .price(fillPrice)
                .priceStatus(priceStatus)
                .notionalUsd(cumulativeExecutedQty.multiply(fillPrice))
                .previousQty(ledgerPreviousQty)
                .resultingQty(persistedResultingQty)
                .realizedPnlUsd(response.getGrossRealizedPnl())
                .feeUsd(response.getTotalFees())
                .tradeIds(response.getTradeIds())
                .individualFills(response.getIndividualFills())
                .averageFillPrice(fillPrice)
                .entryPrice(isOpen(copyIntent) || "INCREASE".equals(copyIntent) ? fillPrice : persisted.getPriceEntry())
                .exitPrice(isOpen(copyIntent) || "INCREASE".equals(copyIntent) ? null : fillPrice)
                .entryFee(isOpen(copyIntent) || "INCREASE".equals(copyIntent) ? response.getTotalFees() : null)
                .exitFee(isOpen(copyIntent) || "INCREASE".equals(copyIntent) ? null : response.getTotalFees())
                .totalFees(response.getTotalFees())
                .fundingPaid(response.getFundingPaid())
                .fundingReceived(response.getFundingReceived())
                .netFunding(response.getNetFunding())
                .grossRealizedPnl(response.getGrossRealizedPnl())
                .netRealizedPnl(response.getNetRealizedPnl())
                .unrealizedPnl(response.getUnrealizedPnl())
                .expectedPrice(response.getExpectedPrice() == null ? intent.getReferencePrice() : response.getExpectedPrice())
                .actualPrice(response.getActualPrice() == null ? fillPrice : response.getActualPrice())
                .slippageBps(response.getSlippageBps())
                .slippageUsd(response.getSlippageUsd())
                .submittedAt(toOffset(response.getSubmittedAt()))
                .acceptedAt(toOffset(response.getAcceptedAt()))
                .filledAt(toOffset(response.getFilledAt()))
                .sourceToSubmitLatencyMs(response.getSourceToSubmitLatencyMs())
                .submitToFillLatencyMs(response.getSubmitToFillLatencyMs())
                .endToEndLatencyMs(response.getEndToEndLatencyMs())
                .economicDataStatus(response.getEconomicDataStatus())
                .strategyVersion(response.getStrategyVersion())
                .sizingPolicyVersion(response.getSizingPolicyVersion())
                .symbolMappingVersion(response.getSymbolMappingVersion())
                .feeModelVersion(response.getFeeModelVersion())
                .fundingModelVersion(response.getFundingModelVersion())
                .slippageModelVersion(response.getSlippageModelVersion())
                .liquidityModelVersion(response.getLiquidityModelVersion())
                .traceId("reconcile-" + intent.getId())
                .source("copy_order_reconciliation")
                .reasonCode(recoveryReasonCode)
                .eventTime(eventTime(response))
                .build());
        intentStore.linkRequiredEvent(intent.getId(), requiredEventId);
        intentStore.markPersisted(intent.getId(), intent.getClientOrderId(), persisted.getIdOperation());
        intent.setPersistedExecutedQty(cumulativeExecutedQty);
        log.info("event=copy.reconciliation.found dispatchIntentId={} binanceOrderId={} status={} reasonCode={} decision=PERSIST_NOT_RESEND copyOperationId={}",
                intent.getId(), response.getOrderId(), response.getStatus(), recoveryReasonCode,
                persisted.getIdOperation());
        return persisted;
    }

    private String recoveryReasonCode(CopyDispatchIntentEntity intent,
                                      BinanceFuturesOrderClientResponse response) {
        if ("PARTIALLY_FILLED".equalsIgnoreCase(response.getStatus())) {
            return "PARTIALLY_FILLED";
        }
        if ("EXECUTION_TIMEOUT_RECONCILING".equals(intent.getLastErrorCode())) {
            return "RECONCILED_AFTER_TIMEOUT";
        }
        return "EXECUTED";
    }

    private CopyOperationDto newOpen(CopyDispatchIntentEntity intent,
                                     BinanceFuturesOrderClientResponse response,
                                     String positionSide,
                                     BigDecimal qty,
                                     BigDecimal price,
                                     String priceStatus) {
        return CopyOperationDto.builder()
                .idOrden(String.valueOf(response.getOrderId()))
                .idUser(intent.getIdUser())
                .idOrderOrigin(intent.getIdOrderOrigin())
                .idWalletOrigin(intent.getWalletId())
                .parsymbol(intent.getSymbol())
                .typeOperation(positionSide)
                .leverage(BigDecimal.valueOf(intent.getRequestedLeverage() == null || intent.getRequestedLeverage() <= 0 ? 1 : intent.getRequestedLeverage()))
                .siseUsd(qty.multiply(price))
                .sizePar(qty)
                .priceEntry(price)
                .dateCreation(eventTime(response))
                .active(true)
                .userCopyAllocationId(intent.getUserCopyAllocationId())
                .copyStrategyCode(intent.getStrategyCode())
                .executionMode(intent.getExecutionMode())
                .shadow(false)
                .dispatchIntentId(intent.getId())
                .sourceEventId(intent.getSourceEventId())
                .clientOrderId(intent.getClientOrderId())
                .priceStatus(priceStatus)
                .build();
    }

    private CopyOperationDto activeUpdate(CopyOperationDto current, CopyDispatchIntentEntity intent,
                                          BigDecimal qty, BigDecimal entry, String priceStatus) {
        return CopyOperationDto.builder()
                .idOperation(current.getIdOperation()).idOrden(current.getIdOrden()).idUser(current.getIdUser())
                .idOrderOrigin(current.getIdOrderOrigin()).idWalletOrigin(current.getIdWalletOrigin())
                .parsymbol(current.getParsymbol()).typeOperation(current.getTypeOperation())
                .leverage(current.getLeverage()).siseUsd(qty.multiply(entry)).sizePar(qty)
                .priceEntry(entry).dateCreation(current.getDateCreation()).active(true)
                .userCopyAllocationId(intent.getUserCopyAllocationId()).copyStrategyCode(intent.getStrategyCode())
                .executionMode(intent.getExecutionMode()).shadow(false).dispatchIntentId(intent.getId())
                .sourceEventId(intent.getSourceEventId()).clientOrderId(intent.getClientOrderId())
                .priceStatus(priceStatus).build();
    }

    private CopyOperationDto closed(CopyOperationDto current, CopyDispatchIntentEntity intent,
                                    BigDecimal closePrice, String priceStatus) {
        return CopyOperationDto.builder()
                .idOperation(current.getIdOperation()).idOrden(current.getIdOrden()).idUser(current.getIdUser())
                .idOrderOrigin(current.getIdOrderOrigin()).idWalletOrigin(current.getIdWalletOrigin())
                .parsymbol(current.getParsymbol()).typeOperation(current.getTypeOperation())
                .leverage(current.getLeverage()).siseUsd(ZERO).sizePar(ZERO)
                .priceEntry(current.getPriceEntry()).priceClose(closePrice).dateCreation(current.getDateCreation())
                .dateClose(OffsetDateTime.now(ZoneOffset.UTC)).active(false)
                .userCopyAllocationId(intent.getUserCopyAllocationId()).copyStrategyCode(intent.getStrategyCode())
                .executionMode(intent.getExecutionMode()).shadow(false).dispatchIntentId(intent.getId())
                .sourceEventId(intent.getSourceEventId()).clientOrderId(intent.getClientOrderId())
                .priceStatus(priceStatus).build();
    }

    private CopyOperationDto reload(CopyDispatchIntentEntity intent, String positionSide, CopyOperationDto fallback) {
        CopyOperationDto saved = copyOperationService.findOperationForAllocation(intent.getIdOrderOrigin(), intent.getIdUser(),
                intent.getUserCopyAllocationId(), intent.getStrategyCode(), positionSide);
        return saved == null ? fallback : saved;
    }

    private BigDecimal executionPrice(BinanceFuturesOrderClientResponse response, CopyDispatchIntentEntity intent) {
        if (positive(response.getAvgPrice())) return response.getAvgPrice();
        if (positive(response.getPrice())) return response.getPrice();
        BigDecimal qty = positive(response.getExecutedQty()) ? response.getExecutedQty() : response.getCumQty();
        if (positive(response.getCumQuote()) && positive(qty)) return response.getCumQuote().divide(qty, 18, RoundingMode.HALF_UP);
        if (positive(intent.getAveragePrice())) return intent.getAveragePrice();
        if (positive(intent.getReferencePrice())) return intent.getReferencePrice();
        throw new IllegalStateException("No execution or reference price available for persistence recovery");
    }

    private BigDecimal weightedEntry(BigDecimal currentPrice, BigDecimal currentQty, BigDecimal fillPrice, BigDecimal fillQty) {
        BigDecimal totalQty = currentQty.add(fillQty);
        if (!positive(totalQty)) return fillPrice;
        return nonNegative(currentPrice).multiply(currentQty).add(fillPrice.multiply(fillQty))
                .divide(totalQty, 18, RoundingMode.HALF_UP);
    }

    private void requireFilled(CopyDispatchIntentEntity intent, BinanceFuturesOrderClientResponse response) {
        if (intent == null || response == null || response.getOrderId() == null) throw new IllegalArgumentException("intent and acknowledged response are required");
        BigDecimal qty = positive(response.getExecutedQty()) ? response.getExecutedQty() : response.getCumQty();
        if (!positive(qty)) throw new IllegalStateException("Reconciled order has no executed quantity yet");
    }

    private String normalizeIntent(String value) { return value == null ? "OPEN" : value.trim().toUpperCase(java.util.Locale.ROOT); }
    private String resolveIntent(String rawIntent, boolean reduceOnly, CopyOperationDto current,
                                 String recordedEventType) {
        String normalized = normalizeIntent(rawIntent);
        if (("FLIP".equals(normalized) || "ADJUST".equals(normalized))
                && recordedEventType != null && !recordedEventType.isBlank()) {
            String persistedType = recordedEventType.trim().toUpperCase(java.util.Locale.ROOT);
            if ("OPEN".equals(persistedType)) {
                return "FLIP".equals(normalized) ? "FLIP_OPEN" : "OPEN";
            }
            if ("INCREASE".equals(persistedType)) return "INCREASE";
            if ("REDUCE".equals(persistedType)) return "REDUCE";
            if ("CLOSE".equals(persistedType)) {
                return "FLIP".equals(normalized) ? "FLIP_CLOSE" : "CLOSE";
            }
        }
        if ("FLIP".equals(normalized)) {
            if (reduceOnly) return "FLIP_CLOSE";
            return current != null && current.isActive() ? "INCREASE" : "FLIP_OPEN";
        }
        if ("ADJUST".equals(normalized)) {
            if (reduceOnly) return "REDUCE";
            return current != null && current.isActive() ? "INCREASE" : "OPEN";
        }
        return normalized;
    }
    private void validateLinkedOperation(CopyDispatchIntentEntity intent, CopyOperationDto linked) {
        if (linked == null) return;
        boolean sameUser = java.util.Objects.equals(intent.getIdUser(), linked.getIdUser());
        boolean sameAllocation = intent.getUserCopyAllocationId() == null
                || linked.getUserCopyAllocationId() == null
                || java.util.Objects.equals(intent.getUserCopyAllocationId(), linked.getUserCopyAllocationId());
        if (!sameUser || !sameAllocation) {
            throw new IllegalStateException("Linked copy_operation does not belong to dispatch intent owner/allocation");
        }
    }
    private boolean isOpen(String intent) { return "OPEN".equals(intent) || "REOPEN".equals(intent) || "FLIP_OPEN".equals(intent); }
    private boolean isExit(String intent) { return !isOpen(intent) && !"INCREASE".equals(intent); }
    private String eventType(String intent, boolean active) { if (!active) return "CLOSE"; return "INCREASE".equals(intent) ? "INCREASE" : isOpen(intent) ? "OPEN" : "REDUCE"; }
    private BigDecimal nonNegative(BigDecimal value) { return value == null ? ZERO : value.max(ZERO); }
    private boolean positive(BigDecimal value) { return value != null && value.compareTo(ZERO) > 0; }
    private OffsetDateTime eventTime(BinanceFuturesOrderClientResponse response) { return response.getUpdateTime() == null ? OffsetDateTime.now(ZoneOffset.UTC) : OffsetDateTime.ofInstant(java.time.Instant.ofEpochMilli(response.getUpdateTime()), ZoneOffset.UTC); }
    private OffsetDateTime toOffset(java.time.Instant value) { return value == null ? null : OffsetDateTime.ofInstant(value, ZoneOffset.UTC); }
    private String firstNonBlank(String... values) { if (values != null) for (String value : values) if (value != null && !value.isBlank()) return value; return null; }
}
