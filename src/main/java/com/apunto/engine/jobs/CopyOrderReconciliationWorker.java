package com.apunto.engine.jobs;

import com.apunto.engine.dto.OperationDto;
import com.apunto.engine.dto.UserDetailDto;
import com.apunto.engine.dto.client.BinanceFuturesOrderClientResponse;
import com.apunto.engine.entity.CopyDispatchIntentEntity;
import com.apunto.engine.service.ProcesBinanceService;
import com.apunto.engine.service.UserDetailCachedService;
import com.apunto.engine.service.copy.dispatch.BinanceOrderExecutionNormalizer;
import com.apunto.engine.service.copy.dispatch.CopyDispatchIntentStore;
import com.apunto.engine.service.copy.dispatch.CopyExecutionPersistenceService;
import com.apunto.engine.service.copy.dispatch.CopyExecutionState;
import com.apunto.engine.service.copy.dispatch.CopyOrderReconciliationService;
import com.apunto.engine.service.copy.dispatch.NormalizedBinanceExecution;
import com.apunto.engine.shared.enums.OrderType;
import com.apunto.engine.shared.enums.PositionSide;
import com.apunto.engine.shared.enums.Side;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "copy.reconciliation.enabled", havingValue = "true", matchIfMissing = true)
public class CopyOrderReconciliationWorker {

    private final CopyOrderReconciliationService reconciliationService;
    private final CopyDispatchIntentStore intentStore;
    private final ProcesBinanceService binanceGateway;
    private final UserDetailCachedService userDetailCachedService;
    private final BinanceOrderExecutionNormalizer normalizer;
    private final CopyExecutionPersistenceService persistenceService;
    private final MeterRegistry meterRegistry;

    private final String workerId = "copy-reconcile-" + UUID.randomUUID();

    @Value("${copy.reconciliation.batch-size:50}")
    private int batchSize;
    @Value("${copy.reconciliation.max-attempts:20}")
    private int maxAttempts;
    @Value("${copy.reconciliation.dispatch-stale-after:PT30S}")
    private Duration dispatchStaleAfter;

    @Scheduled(initialDelayString = "${copy.reconciliation.initial-delay-ms:10000}",
            fixedDelayString = "${copy.reconciliation.fixed-delay-ms:5000}")
    public void scheduledRun() {
        runOnce();
    }

    public int runOnce() {
        List<CopyDispatchIntentEntity> batch = reconciliationService.claimBatch(batchSize, dispatchStaleAfter, workerId);
        int completed = 0;
        for (CopyDispatchIntentEntity intent : batch) {
            try {
                reconcileOne(intent);
                completed++;
            } catch (RuntimeException ex) {
                reconciliationService.markFailure(intent.getId(), maxAttempts, "RECONCILIATION_ITEM_FAILED", safe(ex.getMessage()));
                meterRegistry.counter("signals.copy.reconciliation.total", "result", "failed").increment();
                meterRegistry.counter("copy_reconciliation_total", "result", "failed").increment();
                reconciliationFailure(intent, "item_failed");
                if (exhausted(intent)) {
                    manualReview(intent, "item_failed_exhausted");
                }
                log.error("event=copy.reconciliation.failed dispatchIntentId={} reasonCode=RECONCILIATION_ITEM_FAILED nextRetryAt=backoff errClass={} errMsg=\"{}\"",
                        intent.getId(), ex.getClass().getSimpleName(), safe(ex.getMessage()));
            }
        }
        return completed;
    }

    private void reconcileOne(CopyDispatchIntentEntity intent) {
        long startedNs = System.nanoTime();
        try {
            reconcileOneMeasured(intent);
        } finally {
            meterRegistry.timer("copy_reconciliation_duration",
                            "execution_mode", metricTag(intent == null ? null : intent.getExecutionMode()),
                            "result", "processed")
                    .record(System.nanoTime() - startedNs, java.util.concurrent.TimeUnit.NANOSECONDS);
        }
    }

    private void reconcileOneMeasured(CopyDispatchIntentEntity intent) {
        OperationDto lookup = lookupRequest(intent);
        log.info("event=copy.reconciliation.started dispatchIntentId={} clientOrderId={} orderId={} userCopyAllocationId={} executionMode={}",
                intent.getId(), safe(intent.getClientOrderId()), intent.getBinanceOrderId(),
                intent.getUserCopyAllocationId(), intent.getExecutionMode());
        Optional<BinanceFuturesOrderClientResponse> found = intent.getBinanceOrderId() == null
                ? Optional.empty()
                : binanceGateway.findOrderByOrderId(lookup, intent.getBinanceOrderId());
        if (found.isEmpty()) {
            found = binanceGateway.findOrderByClientOrderId(lookup);
        }
        if (found.isEmpty()) {
            reconciliationService.markLookupNotFound(intent.getId(), maxAttempts);
            meterRegistry.counter("signals.copy.reconciliation.total", "result", "not_found").increment();
            meterRegistry.counter("copy_reconciliation_total", "result", "not_found").increment();
            reconciliationFailure(intent, exhausted(intent) ? "not_found_exhausted" : "not_found");
            if (exhausted(intent)) {
                manualReview(intent, "not_found_exhausted");
            }
            log.warn("event=copy.reconciliation.not_found dispatchIntentId={} decision=WAIT_NO_RESEND clientOrderId={}",
                    intent.getId(), safe(intent.getClientOrderId()));
            return;
        }

        BinanceFuturesOrderClientResponse response = found.get();
        NormalizedBinanceExecution normalized = normalizer.normalize(response);
        if (normalized.executionState() == CopyExecutionState.REJECTED) {
            intentStore.markRejected(intent.getId(), "BINANCE_ORDER_DEFINITIVELY_NOT_ACTIVE", normalized.status());
            meterRegistry.counter("signals.copy.reconciliation.total", "result", "rejected").increment();
            meterRegistry.counter("copy_reconciliation_total", "result", "rejected").increment();
            reconciliationSuccess(intent, "rejected_confirmed");
            log.info("event=copy.reconciliation.rejected dispatchIntentId={} orderId={} status={} decision=RELEASE_NO_RESEND",
                    intent.getId(), normalized.orderId(), normalized.status());
            return;
        }
        if (!normalized.accepted()) {
            reconciliationService.markFailure(intent.getId(), maxAttempts, "LOOKUP_RESPONSE_AMBIGUOUS", normalized.executionState().name());
            reconciliationFailure(intent, "lookup_ambiguous");
            if (exhausted(intent)) {
                manualReview(intent, "lookup_ambiguous_exhausted");
            }
            return;
        }
        response.setDispatchIntentId(intent.getId());
        response.setSourceEventId(intent.getSourceEventId());
        response.setReferencePrice(intent.getReferencePrice());
        response.setAveragePriceStatus(normalized.averagePriceStatus().name());
        response.setAccepted(true);
        response.setExecutionState(normalized.executionState().name());
        response.setRequiresReconciliation(normalized.requiresReconciliation());
        response.setSafeToRetrySend(false);
        intentStore.acknowledge(intent.getId(), normalized, response);

        if (normalized.executionState() == CopyExecutionState.NEW) {
            if (exhausted(intent)) {
                reconciliationService.markUnresolvedTerminal(intent.getId(), "NEW_ORDER_RECONCILIATION_EXHAUSTED");
                manualReview(intent, "new_exhausted");
                log.error("event=copy.reconciliation.failed dispatchIntentId={} reasonCode=NEW_ORDER_RECONCILIATION_EXHAUSTED nextRetryAt=manual_review decision=NO_RESEND",
                        intent.getId());
            } else {
                reconciliationService.deferNewOrder(intent.getId());
            }
            reconciliationSuccess(intent, exhausted(intent) ? "new_manual_review" : "new_deferred");
            return;
        }
        persistenceService.persistRecovered(intent, response);
        if (normalized.executionState() == CopyExecutionState.PARTIALLY_FILLED && exhausted(intent)) {
            reconciliationService.markUnresolvedTerminal(intent.getId(), "PARTIAL_FILL_RECONCILIATION_EXHAUSTED");
            manualReview(intent, "partial_exhausted");
            log.error("event=copy.reconciliation.failed dispatchIntentId={} reasonCode=PARTIAL_FILL_RECONCILIATION_EXHAUSTED nextRetryAt=manual_review decision=NO_RESEND",
                    intent.getId());
        } else if ("PENDING_RESOLUTION".equals(normalized.averagePriceStatus().name()) && exhausted(intent)) {
            reconciliationService.markPriceResolutionExhausted(intent.getId());
            manualReview(intent, "price_exhausted");
            log.warn("event=copy.reconciliation.price_unresolved dispatchIntentId={} reasonCode=PRICE_RESOLUTION_EXHAUSTED decision=KEEP_REFERENCE_PRICE_MANUAL_REVIEW",
                    intent.getId());
        }
        meterRegistry.counter("signals.copy.reconciliation.total", "result", "persisted").increment();
        meterRegistry.counter("copy_reconciliation_total", "result", "persisted").increment();
        reconciliationSuccess(intent, "persisted");
    }

    private void reconciliationSuccess(CopyDispatchIntentEntity intent, String result) {
        meterRegistry.counter("copy_reconciliation_success",
                "execution_mode", metricTag(intent == null ? null : intent.getExecutionMode()),
                "result", metricTag(result)).increment();
    }

    private void reconciliationFailure(CopyDispatchIntentEntity intent, String reasonCode) {
        meterRegistry.counter("copy_reconciliation_failure",
                "execution_mode", metricTag(intent == null ? null : intent.getExecutionMode()),
                "result", "failed",
                "reason_code", metricTag(reasonCode)).increment();
    }

    private void manualReview(CopyDispatchIntentEntity intent, String result) {
        meterRegistry.counter("copy_dispatch_manual_review",
                "execution_mode", metricTag(intent == null ? null : intent.getExecutionMode()),
                "result", metricTag(result)).increment();
    }

    private boolean exhausted(CopyDispatchIntentEntity intent) {
        return intent != null && intent.getReconciliationAttempts() >= Math.max(1, maxAttempts);
    }

    private OperationDto lookupRequest(CopyDispatchIntentEntity intent) {
        UserDetailDto user = userDetailCachedService.getUserById(intent.getIdUser())
                .orElseThrow(() -> new IllegalStateException("User/API credentials unavailable for reconciliation"));
        if (user.getUserApiKey() == null) throw new IllegalStateException("User API credentials unavailable for reconciliation");
        return OperationDto.builder()
                .symbol(intent.getSymbol())
                .side(side(intent.getSide()))
                .type(OrderType.MARKET)
                .positionSide(positionSide(intent.getPositionSide()))
                .quantity(intent.getRequestedQty() == null ? "0" : intent.getRequestedQty().toPlainString())
                .reduceOnly(intent.isReduceOnly())
                .configureAccountSettings(false)
                .clientOrderId(intent.getClientOrderId())
                .originId(intent.getIdOrderOrigin())
                .userId(intent.getIdUser())
                .walletId(intent.getWalletId())
                .apiKey(user.getUserApiKey().getApiKey())
                .secret(user.getUserApiKey().getApiSecret())
                .build();
    }

    private Side side(String value) {
        if (value == null || value.isBlank()) return Side.BUY;
        return Side.valueOf(value.trim().toUpperCase(Locale.ROOT));
    }

    private PositionSide positionSide(String value) {
        if (value == null || value.isBlank()) return PositionSide.BOTH;
        return PositionSide.valueOf(value.trim().toUpperCase(Locale.ROOT));
    }

    private String safe(String value) {
        if (value == null) return "";
        String clean = value.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ').replace('"', '\'');
        return clean.length() > 500 ? clean.substring(0, 500) : clean;
    }

    private String metricTag(String value) {
        if (value == null || value.isBlank()) return "unknown";
        String normalized = value.trim().toLowerCase(Locale.ROOT).replace('-', '_').replace(' ', '_');
        return normalized.length() > 40 ? normalized.substring(0, 40) : normalized;
    }
}
