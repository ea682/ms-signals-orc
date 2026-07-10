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
        String positionSide = firstNonBlank(intent.getPositionSide(), response.getPositionSide());
        CopyOperationDto current = copyOperationService.findOperationForAllocation(
                intent.getIdOrderOrigin(), intent.getIdUser(), intent.getUserCopyAllocationId(),
                intent.getStrategyCode(), positionSide);
        BigDecimal cumulativeExecutedQty = positive(response.getExecutedQty()) ? response.getExecutedQty()
                : positive(response.getCumQty()) ? response.getCumQty() : intent.getExecutedQty();
        BigDecimal alreadyAppliedQty = nonNegative(intent.getPersistedExecutedQty());
        if (!positive(alreadyAppliedQty) && current != null && intent.getId().equals(current.getDispatchIntentId())) {
            // Local persistence may have committed immediately before a process crash.
            // Fail closed: do not apply the same cumulative fill a second time.
            alreadyAppliedQty = cumulativeExecutedQty;
        }
        BigDecimal executedQty = cumulativeExecutedQty.subtract(alreadyAppliedQty).max(ZERO);
        BigDecimal fillPrice = executionPrice(response, intent);
        String priceStatus = firstNonBlank(response.getAveragePriceStatus(), intent.getAveragePriceStatus(),
                positive(response.getAvgPrice()) ? "AVAILABLE" : "PENDING_RESOLUTION");
        String copyIntent = normalizeIntent(intent.getCopyIntent());
        BigDecimal previousQty = current == null ? ZERO : nonNegative(current.getSizePar());
        BigDecimal resultingQty;
        CopyOperationDto persisted;

        if (isOpen(copyIntent)) {
            if (current != null && current.isActive()) {
                resultingQty = previousQty.add(executedQty);
                boolean sameIntent = intent.getId().equals(current.getDispatchIntentId());
                BigDecimal entry = sameIntent && !"PENDING_RESOLUTION".equals(priceStatus)
                        ? fillPrice
                        : weightedEntry(current.getPriceEntry(), previousQty, fillPrice, executedQty);
                persisted = activeUpdate(current, intent, resultingQty, entry, priceStatus);
                copyOperationService.upsertActiveOperation(persisted);
            } else {
                // If the operation row itself is missing, reconstruct the cumulative
                // acknowledged fill rather than only its latest delta.
                resultingQty = cumulativeExecutedQty;
                persisted = newOpen(intent, response, resultingQty, fillPrice, priceStatus);
                copyOperationService.upsertActiveOperation(persisted);
                persisted = reload(intent, positionSide, persisted);
            }
        } else if ("INCREASE".equals(copyIntent)) {
            if (current == null || !current.isActive()) {
                throw new IllegalStateException("Cannot recover INCREASE without active copy_operation");
            }
            resultingQty = previousQty.add(executedQty);
            BigDecimal entry = weightedEntry(current.getPriceEntry(), previousQty, fillPrice, executedQty);
            persisted = activeUpdate(current, intent, resultingQty, entry, priceStatus);
            copyOperationService.upsertActiveOperation(persisted);
        } else {
            if (current == null || !current.isActive()) {
                throw new IllegalStateException("Cannot recover reduction/close without active copy_operation");
            }
            resultingQty = previousQty.subtract(executedQty).max(ZERO);
            if (resultingQty.compareTo(ZERO) == 0) {
                persisted = closed(current, intent, fillPrice, priceStatus);
                copyOperationService.closeOperation(persisted);
            } else {
                persisted = activeUpdate(current, intent, resultingQty, current.getPriceEntry(), priceStatus);
                copyOperationService.upsertActiveOperation(persisted);
            }
        }

        UUID requiredEventId = copyOperationEventService.recordRequired(CopyOperationEventRecordCommand.builder()
                .idOperation(persisted.getIdOperation())
                .dispatchIntentId(intent.getId())
                .userCopyAllocationId(intent.getUserCopyAllocationId())
                .copyStrategyCode(intent.getStrategyCode())
                .executionMode(intent.getExecutionMode())
                .shadow(false)
                .decision("RECOVERED_" + eventType(copyIntent, persisted.isActive()))
                .decisionReason("binance_fill_reconciled_without_resend")
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
                .qtyExecuted(executedQty)
                .price(fillPrice)
                .priceStatus(priceStatus)
                .notionalUsd(executedQty.multiply(fillPrice))
                .previousQty(previousQty)
                .resultingQty(persisted.isActive() ? persisted.getSizePar() : ZERO)
                .traceId("reconcile-" + intent.getId())
                .source("copy_order_reconciliation")
                .reasonCode("PERSISTENCE_RECOVERED")
                .eventTime(eventTime(response))
                .build());
        intentStore.linkRequiredEvent(intent.getId(), requiredEventId);
        intentStore.markPersisted(intent.getId(), intent.getClientOrderId(), persisted.getIdOperation());
        intent.setPersistedExecutedQty(cumulativeExecutedQty);
        log.info("event=copy.reconciliation.found dispatchIntentId={} binanceOrderId={} status={} decision=PERSIST_NOT_RESEND copyOperationId={}",
                intent.getId(), response.getOrderId(), response.getStatus(), persisted.getIdOperation());
        return persisted;
    }

    private CopyOperationDto newOpen(CopyDispatchIntentEntity intent,
                                     BinanceFuturesOrderClientResponse response,
                                     BigDecimal qty,
                                     BigDecimal price,
                                     String priceStatus) {
        return CopyOperationDto.builder()
                .idOrden(String.valueOf(response.getOrderId()))
                .idUser(intent.getIdUser())
                .idOrderOrigin(intent.getIdOrderOrigin())
                .idWalletOrigin(intent.getWalletId())
                .parsymbol(intent.getSymbol())
                .typeOperation(intent.getPositionSide())
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
    private boolean isOpen(String intent) { return "OPEN".equals(intent) || "REOPEN".equals(intent) || "FLIP_OPEN".equals(intent); }
    private String eventType(String intent, boolean active) { if (!active) return "CLOSE"; return "INCREASE".equals(intent) ? "INCREASE" : isOpen(intent) ? "OPEN" : "REDUCE"; }
    private BigDecimal nonNegative(BigDecimal value) { return value == null ? ZERO : value.max(ZERO); }
    private boolean positive(BigDecimal value) { return value != null && value.compareTo(ZERO) > 0; }
    private OffsetDateTime eventTime(BinanceFuturesOrderClientResponse response) { return response.getUpdateTime() == null ? OffsetDateTime.now(ZoneOffset.UTC) : OffsetDateTime.ofInstant(java.time.Instant.ofEpochMilli(response.getUpdateTime()), ZoneOffset.UTC); }
    private String firstNonBlank(String... values) { if (values != null) for (String value : values) if (value != null && !value.isBlank()) return value; return null; }
}
