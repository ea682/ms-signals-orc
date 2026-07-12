package com.apunto.engine.service.copy.dispatch;

import com.apunto.engine.dto.OperationDto;
import com.apunto.engine.dto.client.BinanceFuturesOrderClientResponse;
import com.apunto.engine.entity.UserCopyAllocationEntity;
import com.apunto.engine.service.ProcesBinanceService;
import com.apunto.engine.shared.exception.BinanceApiReadinessException;
import com.apunto.engine.shared.exception.CopyDispatchReconciliationPendingException;
import com.apunto.engine.shared.exception.CopyOrderRejectedException;
import com.apunto.engine.shared.exception.SkipExecutionException;
import com.apunto.engine.shared.util.LogFmt;
import lombok.extern.slf4j.Slf4j;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Locale;
import java.util.UUID;
import java.util.regex.Pattern;

@Slf4j
@Service
public class CopyDispatchCoordinator {

    private static final BigDecimal ZERO = BigDecimal.ZERO;
    private static final Pattern CLIENT_ORDER_ID = Pattern.compile("^[A-Za-z0-9._-]{1,36}$");

    private final CopyDispatchIntentStore intentStore;
    private final ProcesBinanceService binanceGateway;
    private final BinanceOrderExecutionNormalizer normalizer;
    private final CopyIdempotencyKeyFactory keyFactory;
    private final MeterRegistry meterRegistry;

    public CopyDispatchCoordinator(CopyDispatchIntentStore intentStore,
                                   ProcesBinanceService binanceGateway,
                                   BinanceOrderExecutionNormalizer normalizer,
                                   CopyIdempotencyKeyFactory keyFactory,
                                   MeterRegistry meterRegistry) {
        this.intentStore = intentStore;
        this.binanceGateway = binanceGateway;
        this.normalizer = normalizer;
        this.keyFactory = keyFactory;
        this.meterRegistry = meterRegistry;
    }

    public BinanceFuturesOrderClientResponse dispatch(OperationDto operation,
                                                      UserCopyAllocationEntity allocation,
                                                      BigDecimal referencePrice,
                                                      String traceId) {
        long dispatchStartedNs = System.nanoTime();
        CopyDispatchRequest request = request(operation, allocation, referencePrice, traceId);
        CopyDispatchPermit permit = intentStore.acquire(request);

        if (permit.decision() == CopyDispatchPermit.Decision.REUSE_ACKNOWLEDGED) {
            recordDispatch(request, "reused");
            String reasonCode = duplicateIntentReason(request);
            recordDuplicateDispatch(request, reasonCode, "reused");
            log.info("event=copy.dispatch.intent.duplicate reasonCode={} idempotencyKey={} existingIntentId={} existingStatus={} decision=NOOP_REUSE_ACK copyImpact=NO_DUPLICATE_ORDER",
                    reasonCode, request.idempotencyKey(), permit.intentId(), permit.existingStatus());
            return permit.knownResponse();
        }
        if (permit.decision() == CopyDispatchPermit.Decision.RECONCILE_EXISTING) {
            recordDispatch(request, "reconcile");
            String reasonCode = duplicateIntentReason(request);
            recordDuplicateDispatch(request, reasonCode, "reconcile");
            log.warn("event=copy.dispatch.intent.duplicate reasonCode={} idempotencyKey={} existingIntentId={} existingStatus={} decision=RECONCILE_EXISTING retryable=false copyImpact=NO_DUPLICATE_ORDER",
                    reasonCode, request.idempotencyKey(), permit.intentId(), permit.existingStatus());
            throw reconciliationPending(permit.intentId(), request, "existing_intent_not_terminal", null);
        }
        if (permit.decision() == CopyDispatchPermit.Decision.CONFLICT) {
            recordDispatch(request, "conflict");
            log.error("event=copy.dispatch.intent.conflict idempotencyKey={} existingIntentId={} existingStatus={} decision=BLOCK_PAYLOAD_MISMATCH",
                    request.idempotencyKey(), permit.intentId(), permit.existingStatus());
            throw new SkipExecutionException("COPY_IDEMPOTENCY_PAYLOAD_MISMATCH",
                    "La misma idempotency key llego con un payload de orden diferente",
                    LogFmt.kv("dispatchIntentId", permit.intentId(), "status", permit.existingStatus()));
        }
        if (permit.decision() == CopyDispatchPermit.Decision.NOOP_PERSISTED) {
            recordDispatch(request, "noop");
            String reasonCode = duplicateIntentReason(request);
            recordDuplicateDispatch(request, reasonCode, "noop");
            log.info("event=copy.dispatch.intent.duplicate reasonCode={} idempotencyKey={} existingIntentId={} existingStatus={} decision=NOOP_ALREADY_APPLIED copyImpact=NO_DUPLICATE_ORDER",
                    reasonCode, request.idempotencyKey(), permit.intentId(), permit.existingStatus());
            throw new SkipExecutionException("COPY_DISPATCH_ALREADY_APPLIED_NOOP",
                    "La intencion y su estado local ya fueron persistidos; no se reaplica",
                    LogFmt.kv("dispatchIntentId", permit.intentId(), "status", permit.existingStatus()));
        }
        if (permit.decision() == CopyDispatchPermit.Decision.REJECTED) {
            recordDispatch(request, "rejected");
            String reasonCode = durableRejectionReason(permit.existingStatus());
            log.warn("event=copy.dispatch.intent.duplicate_rejected reasonCode={} decision=NOOP_NO_RESEND result=REJECTED idempotencyKey={} existingIntentId={} existingStatus={} executionMode={} retryable=false copyImpact=NO_DUPLICATE_ORDER",
                    reasonCode, request.idempotencyKey(), permit.intentId(), permit.existingStatus(),
                    request.identity().executionMode());
            throw new SkipExecutionException(reasonCode,
                    "La intencion ya fue rechazada y no puede reenviarse",
                    LogFmt.kv("dispatchIntentId", permit.intentId(), "status", permit.existingStatus()));
        }

        try {
            log.info("event=copy.dispatch.claimed dispatchIntentId={} workerId={} attempt=1 idempotencyKey={}",
                    permit.intentId(), Thread.currentThread().getName(), request.idempotencyKey());
            meterRegistry.timer("copy_pre_network_duration",
                            "execution_mode", metricTag(request.identity().executionMode()),
                            "operation_type", metricTag(request.identity().copyIntent()),
                            "result", "authorized")
                    .record(System.nanoTime() - dispatchStartedNs, java.util.concurrent.TimeUnit.NANOSECONDS);
            meterRegistry.timer("copy.hot.path.duration",
                            "stage", "pre_network",
                            "mode", metricTag(request.identity().executionMode()),
                            "result", "authorized")
                    .record(System.nanoTime() - dispatchStartedNs, java.util.concurrent.TimeUnit.NANOSECONDS);
            long engineStartedNs = System.nanoTime();
            BinanceFuturesOrderClientResponse response;
            String engineResult = "completed";
            try {
                response = binanceGateway.operationPosition(operation);
            } catch (RuntimeException ex) {
                engineResult = "error";
                throw ex;
            } finally {
                long engineElapsedNs = System.nanoTime() - engineStartedNs;
                meterRegistry.timer("copy_binance_engine_duration",
                                "execution_mode", metricTag(request.identity().executionMode()),
                                "operation_type", metricTag(request.identity().copyIntent()),
                                "result", engineResult)
                        .record(engineElapsedNs, java.util.concurrent.TimeUnit.NANOSECONDS);
                meterRegistry.timer("copy.hot.path.duration",
                                "stage", "binance_adapter",
                                "mode", metricTag(request.identity().executionMode()),
                                "result", engineResult)
                        .record(engineElapsedNs, java.util.concurrent.TimeUnit.NANOSECONDS);
            }
            NormalizedBinanceExecution execution = normalizer.normalize(response);
            log.info("event=copy.dispatch.response.normalized dispatchIntentId={} orderId={} clientOrderId={} status={} executedQty={} avgPrice={} avgPriceStatus={} accepted={} requiresReconciliation={} safeToRetrySend={}",
                    permit.intentId(), execution.orderId(), safe(execution.clientOrderId()), execution.status(),
                    execution.executedQty(), execution.averagePrice(), execution.averagePriceStatus(),
                    execution.accepted(), execution.requiresReconciliation(), execution.safeToRetrySend());

            if (execution.executionState() == CopyExecutionState.REJECTED) {
                throw new CopyOrderRejectedException(
                        "Binance confirmo que la orden no quedo activa: " + safe(execution.status()),
                        java.util.Map.of("dispatchIntentId", permit.intentId(), "orderId", String.valueOf(execution.orderId()),
                                "clientOrderId", safe(execution.clientOrderId()), "status", safe(execution.status())));
            }
            if (!execution.accepted()) {
                intentStore.markAmbiguous(permit.intentId(), "BINANCE_RESPONSE_AMBIGUOUS", execution.executionState().name());
                recordDispatch(request, "ambiguous");
                throw reconciliationPending(permit.intentId(), request, "binance_response_ambiguous", null);
            }
            annotateResponse(response, execution, permit.intentId(), request);

            intentStore.acknowledge(permit.intentId(), execution, response);
            if (execution.executionState() == CopyExecutionState.NEW) {
                recordDispatch(request, "reconcile");
                throw reconciliationPending(permit.intentId(), request, "binance_order_acknowledged_not_filled", null);
            }
            recordDispatch(request, "accepted");
            return response;
        } catch (CopyDispatchReconciliationPendingException pending) {
            throw pending;
        } catch (CopyOrderRejectedException | BinanceApiReadinessException rejected) {
            recordDispatch(request, "rejected");
            intentStore.markRejected(permit.intentId(), rejected.getClass().getSimpleName(), safe(rejected.getMessage()));
            throw rejected;
        } catch (SkipExecutionException rejectedBeforeSend) {
            recordDispatch(request, "rejected");
            intentStore.markRejected(permit.intentId(), rejectedBeforeSend.getReasonCode(), safe(rejectedBeforeSend.getMessage()));
            throw rejectedBeforeSend;
        } catch (RuntimeException ambiguous) {
            // IMPORTANT:
            // A transport timeout or incomplete response is an ambiguous outcome.
            // The order may already exist at Binance. Move the intent to RECONCILING and
            // query by orderId/clientOrderId. Never resend until non-existence is confirmed.
            intentStore.markAmbiguous(permit.intentId(), "BINANCE_OUTCOME_AMBIGUOUS", safe(ambiguous.getMessage()));
            recordDispatch(request, "ambiguous");
            log.warn("event=copy.dispatch.ambiguous dispatchIntentId={} reasonCode=BINANCE_OUTCOME_AMBIGUOUS decision=RECONCILE_NOT_RESEND errClass={} errMsg=\"{}\"",
                    permit.intentId(), ambiguous.getClass().getSimpleName(), safe(ambiguous.getMessage()));
            throw reconciliationPending(permit.intentId(), request, "binance_outcome_ambiguous", ambiguous);
        }
    }

    public void markPersistencePending(String clientOrderId, String reasonCode, String detail) {
        markPersistencePending(null, clientOrderId, reasonCode, detail);
    }

    public void markPersistencePending(UUID intentId, String clientOrderId, String reasonCode, String detail) {
        if (intentId == null && (clientOrderId == null || clientOrderId.isBlank())) return;
        intentStore.markPersistencePending(intentId, clientOrderId, reasonCode, detail);
        log.warn("event=copy.dispatch.persistence_pending dispatchIntentId={} clientOrderId={} reasonCode={}",
                intentId, safe(clientOrderId), safe(reasonCode));
    }

    public void markPersisted(String clientOrderId, UUID copyOperationId) {
        markPersisted(null, clientOrderId, copyOperationId);
    }

    public void linkRequiredEvent(UUID intentId, UUID copyOperationEventId) {
        if (intentId == null || copyOperationEventId == null) {
            throw new IllegalArgumentException("dispatch intent and required ledger event are required");
        }
        intentStore.linkRequiredEvent(intentId, copyOperationEventId);
    }

    public void markPersisted(UUID intentId, String clientOrderId, UUID copyOperationId) {
        if (intentId == null && (clientOrderId == null || clientOrderId.isBlank())) return;
        intentStore.markPersisted(intentId, clientOrderId, copyOperationId);
        log.info("event=copy.dispatch.persisted dispatchIntentId={} clientOrderId={} copyOperationId={}",
                intentId, safe(clientOrderId), copyOperationId);
    }

    public void markPersisted(BinanceFuturesOrderClientResponse response, UUID copyOperationId) {
        if (response == null) return;
        markPersisted(response.getDispatchIntentId(), response.getClientOrderId(), copyOperationId);
    }

    private void annotateResponse(BinanceFuturesOrderClientResponse response,
                                  NormalizedBinanceExecution execution,
                                  UUID intentId,
                                  CopyDispatchRequest request) {
        if (response == null) return;
        response.setAccepted(execution.accepted());
        response.setExecutionState(execution.executionState().name());
        response.setAveragePriceStatus(execution.averagePriceStatus().name());
        response.setRequiresReconciliation(execution.requiresReconciliation());
        response.setSafeToRetrySend(execution.safeToRetrySend());
        response.setDispatchIntentId(intentId);
        response.setSourceEventId(request.identity().sourceEventId());
        response.setReferencePrice(request.referencePrice());
    }

    private CopyDispatchRequest request(OperationDto operation,
                                        UserCopyAllocationEntity allocation,
                                        BigDecimal referencePrice,
                                        String traceId) {
        if (operation == null) throw new SkipExecutionException("operation_dto_null", "OperationDto es requerido", null);
        if (allocation == null || allocation.getId() == null) {
            throw new SkipExecutionException("COPY_REAL_ALLOCATION_REQUIRED",
                    "MICRO_LIVE/LIVE requiere una allocation exacta y durable antes del dispatch", null);
        }
        String rawMode = firstNonBlank(allocation.getExecutionMode());
        String mode = rawMode == null ? null : rawMode.toUpperCase(Locale.ROOT).replace('-', '_');
        if (!"MICRO_LIVE".equals(mode) && !"LIVE".equals(mode)) {
            throw new SkipExecutionException("COPY_REAL_EXECUTION_MODE_REQUIRED",
                    "El dispatch real solo admite allocations MICRO_LIVE o LIVE",
                    LogFmt.kv("allocationId", allocation.getId(), "executionMode", safe(rawMode)));
        }
        String copyIntent = firstNonBlank(operation.getCopyIntent(), deriveIntent(operation));
        String symbol = firstNonBlank(operation.getSymbol());
        if (symbol == null) {
            throw new SkipExecutionException("COPY_SYMBOL_REQUIRED",
                    "El simbolo real es obligatorio antes de crear el dispatch intent",
                    LogFmt.kv("allocationId", allocation.getId(), "copyIntent", copyIntent));
        }
        String originId = firstNonBlank(operation.getOriginId());
        String legacySourceIdentity = originId == null ? null
                : originId + '|' + copyIntent + '|' + safe(operation.getQuantity());
        String sourceEventId = firstNonBlank(operation.getSourceEventId(), operation.getClientOrderId(),
                legacySourceIdentity);
        if (sourceEventId == null) {
            throw new SkipExecutionException("COPY_IMMUTABLE_SOURCE_ID_REQUIRED",
                    "No existe identidad inmutable para deduplicar el evento real",
                    LogFmt.kv("allocationId", allocation.getId(), "symbol", operation.getSymbol(),
                            "copyIntent", copyIntent));
        }
        BigDecimal qty = decimal(operation.getQuantity());
        if (!positive(qty)) {
            throw new SkipExecutionException("COPY_POSITIVE_QUANTITY_REQUIRED",
                    "La cantidad real debe ser numerica y mayor a cero antes del dispatch",
                    LogFmt.kv("allocationId", allocation.getId(), "symbol", operation.getSymbol(),
                            "quantity", safe(operation.getQuantity())));
        }
        CopyDispatchIdentity identity = new CopyDispatchIdentity(
                operation.getUserId(),
                allocation.getId(),
                mode,
                allocation.getCopyStrategyCode(),
                allocation.getScopeType(),
                allocation.getScopeValue(),
                sourceEventId,
                copyIntent);
        String idempotencyKey = keyFactory.create(identity);
        String clientOrderId = firstNonBlank(operation.getClientOrderId());
        if (clientOrderId == null) {
            clientOrderId = keyFactory.clientOrderId(idempotencyKey);
            operation.setClientOrderId(clientOrderId);
        }
        if (!CLIENT_ORDER_ID.matcher(clientOrderId).matches()) {
            throw new SkipExecutionException("COPY_CLIENT_ORDER_ID_INVALID",
                    "clientOrderId debe cumplir el contrato Binance antes del claim durable",
                    LogFmt.kv("allocationId", allocation.getId(), "clientOrderIdLength", clientOrderId.length()));
        }
        BigDecimal ref = positive(operation.getReferencePrice()) ? operation.getReferencePrice() : referencePrice;
        BigDecimal notional = positive(operation.getRequestedNotionalUsd())
                ? operation.getRequestedNotionalUsd()
                : positive(qty) && positive(ref) ? qty.multiply(ref) : ZERO;
        BigDecimal requestedMargin = resolveMargin(operation, notional);
        if (!operation.isReduceOnly() && !positive(requestedMargin)) {
            throw new SkipExecutionException("COPY_POSITIVE_MARGIN_REQUIRED",
                    "OPEN/INCREASE real requiere margen positivo calculado antes del dispatch",
                    LogFmt.kv("executionMode", mode, "allocationId", identity.userCopyAllocationId(),
                            "symbol", operation.getSymbol(), "copyIntent", copyIntent));
        }
        boolean reservePosition = Boolean.TRUE.equals(operation.getReservePosition());
        String requestHash = keyFactory.hashPayload(String.join("|",
                symbol, safe(name(operation.getSide())),
                safe(name(operation.getPositionSide())), safe(name(operation.getType())),
                canonical(qty), canonical(requestedMargin), canonical(notional), canonical(ref),
                operation.getLeverage() == null ? "" : Integer.toString(operation.getLeverage()),
                Boolean.toString(operation.isReduceOnly()),
                String.valueOf(Boolean.TRUE.equals(operation.getConfigureAccountSettings())),
                clientOrderId));

        return new CopyDispatchRequest(idempotencyKey, identity, operation, operation.getWalletId(),
                symbol, name(operation.getSide()), name(operation.getPositionSide()),
                operation.isReduceOnly(), qty, requestedMargin, notional, ref,
                operation.getLeverage(), reservePosition, operation.getSourceEventType(), requestHash, traceId);
    }

    private BigDecimal resolveMargin(OperationDto operation, BigDecimal notional) {
        if (operation.isReduceOnly()) return ZERO;
        if (operation.getRequestedMarginUsd() != null) return operation.getRequestedMarginUsd().max(ZERO);
        if (positive(notional) && operation.getLeverage() != null && operation.getLeverage() > 0) {
            return notional.divide(BigDecimal.valueOf(operation.getLeverage()), 18, RoundingMode.HALF_UP);
        }
        return ZERO;
    }

    private CopyDispatchReconciliationPendingException reconciliationPending(UUID intentId,
                                                                              CopyDispatchRequest request,
                                                                              String reason,
                                                                              Throwable cause) {
        return new CopyDispatchReconciliationPendingException(
                "Resultado de orden pendiente de reconciliacion; no se reenviara",
                LogFmt.kv("dispatchIntentId", intentId, "idempotencyKey", request.idempotencyKey(),
                        "clientOrderId", request.operation().getClientOrderId(), "reason", reason),
                cause);
    }

    private String deriveIntent(OperationDto operation) {
        if (operation.isReduceOnly()) return "REDUCE_OR_CLOSE";
        return Boolean.TRUE.equals(operation.getConfigureAccountSettings()) ? "OPEN" : "INCREASE";
    }

    private BigDecimal decimal(String value) {
        try {
            return value == null ? ZERO : new BigDecimal(value.trim());
        } catch (NumberFormatException ignored) {
            return ZERO;
        }
    }

    private boolean positive(BigDecimal value) {
        return value != null && value.compareTo(ZERO) > 0;
    }

    private String canonical(BigDecimal value) {
        if (value == null) return "";
        return value.stripTrailingZeros().toPlainString();
    }

    private String firstNonBlank(String... values) {
        if (values == null) return null;
        for (String value : values) if (value != null && !value.isBlank()) return value.trim();
        return null;
    }

    private String safe(String value) {
        if (value == null) return "";
        String clean = value.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ').replace('"', '\'');
        return clean.length() > 500 ? clean.substring(0, 500) : clean;
    }

    private String durableRejectionReason(String status) {
        String prefix = "REJECTED:";
        if (status != null && status.startsWith(prefix) && status.length() > prefix.length()) {
            return status.substring(prefix.length());
        }
        if ("FAILED_FINAL".equals(status)) return "COPY_DISPATCH_FAILED_FINAL";
        if ("CANCELLED".equals(status)) return "COPY_DISPATCH_CANCELLED";
        if ("MANUAL_REVIEW".equals(status)) return "COPY_DISPATCH_MANUAL_REVIEW_REQUIRED";
        return "COPY_DISPATCH_REJECTION_REASON_MISSING";
    }

    private String duplicateIntentReason(CopyDispatchRequest request) {
        return request != null && request.identity() != null
                && "MICRO_LIVE".equalsIgnoreCase(request.identity().executionMode())
                ? "MICRO_LIVE_DUPLICATE_INTENT"
                : "COPY_DUPLICATE_INTENT";
    }

    private void recordDuplicateDispatch(CopyDispatchRequest request, String reasonCode, String result) {
        String mode = request == null || request.identity() == null
                ? "unknown"
                : metricTag(request.identity().executionMode());
        meterRegistry.counter("copy_dispatch_total", "mode", mode,
                "result", metricTag(result), "reason", metricTag(reasonCode)).increment();
    }

    private void recordDispatch(CopyDispatchRequest request, String result) {
        String mode = request == null || request.identity() == null
                ? "unknown"
                : metricTag(request.identity().executionMode());
        meterRegistry.counter("copy.dispatch.total", "mode", mode, "result", metricTag(result)).increment();
    }

    private String metricTag(String value) {
        if (value == null || value.isBlank()) return "unknown";
        String normalized = value.trim().toLowerCase(Locale.ROOT).replace('-', '_').replace(' ', '_');
        return normalized.length() > 40 ? normalized.substring(0, 40) : normalized;
    }

    private String name(Enum<?> value) {
        return value == null ? null : value.name().toUpperCase(Locale.ROOT);
    }
}
