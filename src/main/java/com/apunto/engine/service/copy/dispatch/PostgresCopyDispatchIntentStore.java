package com.apunto.engine.service.copy.dispatch;

import com.apunto.engine.dto.client.BinanceFuturesOrderClientResponse;
import com.apunto.engine.entity.CopyDispatchIntentEntity;
import com.apunto.engine.repository.CopyBudgetSnapshotProjection;
import com.apunto.engine.repository.CopyDispatchIntentRepository;
import com.apunto.engine.shared.exception.SkipExecutionException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.persistence.EntityManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class PostgresCopyDispatchIntentStore implements CopyDispatchIntentStore {

    private static final BigDecimal ZERO = BigDecimal.ZERO;
    private final CopyDispatchIntentRepository repository;
    private final EntityManager entityManager;
    private final ObjectMapper objectMapper;
    private final MeterRegistry meterRegistry;

    @Value("${copy.micro-live.max-margin-per-operation-usd:20}")
    private BigDecimal maxMarginPerOrderUsd;
    @Value("${copy.micro-live.total-capital-usd:${copy.micro-live.max-capital-usd:100}}")
    private BigDecimal maxTotalMarginUsd;
    @Value("${copy.micro-live.max-concurrent-positions:5}")
    private int maxConcurrentPositions;

    @Override
    @Transactional
    public CopyDispatchPermit acquire(CopyDispatchRequest request) {
        long started = System.nanoTime();
        validate(request);
        CopyDispatchIdentity identity = request.identity();
        lockBudget(identity);
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        UUID candidateId = UUID.randomUUID();
        int inserted = repository.insertIfAbsent(
                candidateId, request.idempotencyKey(), identity.userId(), identity.userCopyAllocationId(),
                realMode(identity.executionMode()), trim(request.walletId(), 180), trim(identity.strategyCode(), 64),
                trim(identity.scopeType(), 32), trim(identity.scopeValue(), 180), trim(identity.sourceEventId(), 600),
                trim(request.operation().getOriginId(), 120), trim(request.sourceEventType(), 40),
                trim(identity.copyIntent(), 40), trim(request.symbol(), 40), trim(request.side(), 12),
                trim(request.positionSide(), 12), request.reduceOnly(), request.requestedQty(),
                nonNegative(request.requestedMarginUsd()), nonNegative(request.requestedNotionalUsd()),
                request.referencePrice(), request.requestedLeverage(), request.reservePosition() ? 1 : 0,
                trim(request.operation().getClientOrderId(), 36), request.requestHash(), now);

        CopyDispatchIntentEntity intent = repository.findByIdempotencyKey(request.idempotencyKey())
                .orElseThrow(() -> new IllegalStateException("copy dispatch intent insert/select lost"));
        if (inserted == 0) {
            meterRegistry.counter("signals.copy.dispatch.duplicate.total", "mode", safeTag(intent.getExecutionMode())).increment();
            meterRegistry.counter("copy_dispatch_duplicate_noop",
                    "execution_mode", safeTag(intent.getExecutionMode()), "result", "duplicate").increment();
            recordClaim(started, intent.getExecutionMode(), "duplicate");
            return duplicatePermit(intent, request);
        }

        if ("MICRO_LIVE".equals(realMode(identity.executionMode()))) {
            BudgetSnapshot snapshot = budgetSnapshot(identity);
            MicroLiveBudgetPolicy policy = new MicroLiveBudgetPolicy(maxMarginPerOrderUsd, maxTotalMarginUsd, maxConcurrentPositions);
            BudgetDecision decision = policy.evaluate(snapshot, request.requestedMarginUsd(), request.reservePosition());
            log.info("event=copy.budget.reserved userCopyAllocationId={} executionMode=MICRO_LIVE limitMarginUsd={} usedMarginUsd={} reservedMarginUsd={} requestedMarginUsd={} openPositions={} reservedPositions={} projectedMarginUsd={} projectedPositions={} allowed={}",
                    identity.userCopyAllocationId(), maxTotalMarginUsd, snapshot.usedMarginUsd(),
                    snapshot.reservedPendingMarginUsd(), request.requestedMarginUsd(), snapshot.openPositions(),
                    snapshot.reservedPositions(), decision.projectedMarginUsd(), decision.projectedPositions(), decision.allowed());
            if (!decision.allowed()) {
                CopyDispatchStatePolicy.requireTransition(intent.getStatus(), "REJECTED");
                intent.setStatus("REJECTED");
                intent.setReservationStatus("RELEASED");
                intent.setLastErrorCode(decision.reasonCode());
                intent.setLastErrorDetail("atomic budget reservation rejected before Binance dispatch");
                repository.saveAndFlush(intent);
                log.warn("event=copy.budget.released dispatchIntentId={} releaseReason={} allocationId={} usedMarginUsd={} reservedMarginUsd={} requestedMarginUsd={} projectedMarginUsd={} projectedPositions={}",
                        intent.getId(), decision.reasonCode(), identity.userCopyAllocationId(),
                        snapshot.usedMarginUsd(), snapshot.reservedPendingMarginUsd(), request.requestedMarginUsd(),
                        decision.projectedMarginUsd(), decision.projectedPositions());
                meterRegistry.counter("copy_reservation_rejected", "execution_mode", "micro_live",
                        "result", "rejected").increment();
                recordClaim(started, intent.getExecutionMode(), "reservation_rejected");
                return CopyDispatchPermit.rejected(intent.getId(), "REJECTED:" + decision.reasonCode());
            }
        }

        CopyDispatchStatePolicy.requireTransition(intent.getStatus(), "DISPATCHING");
        intent.setStatus("DISPATCHING");
        intent.setReservationStatus("PENDING");
        intent.setAttempts(intent.getAttempts() + 1);
        intent.setClaimedAt(now);
        intent.setSentAt(now);
        intent.setClaimedBy(Thread.currentThread().getName());
        repository.saveAndFlush(intent);
        meterRegistry.timer("signals.copy.dispatch.intent.acquire", "decision", "send", "mode", safeTag(intent.getExecutionMode()))
                .record(System.nanoTime() - started, java.util.concurrent.TimeUnit.NANOSECONDS);
        recordClaim(started, intent.getExecutionMode(), "authorized");
        meterRegistry.counter("copy_dispatch_authorized", "execution_mode", safeTag(intent.getExecutionMode()),
                "result", "authorized").increment();
        log.info("event=copy.dispatch.intent.created idempotencyKey={} dispatchIntentId={} userId={} userCopyAllocationId={} executionMode={} walletId={} strategyCode={} sourceEventId={} originId={} copyIntent={} symbol={} reservedMarginUsd={}",
                intent.getIdempotencyKey(), intent.getId(), intent.getIdUser(), intent.getUserCopyAllocationId(),
                intent.getExecutionMode(), safe(intent.getWalletId()), safe(intent.getStrategyCode()),
                safe(intent.getSourceEventId()), safe(intent.getIdOrderOrigin()), safe(intent.getCopyIntent()),
                safe(intent.getSymbol()), intent.getRequestedMarginUsd());
        return CopyDispatchPermit.send(intent.getId());
    }

    @Override
    @Transactional
    public void acknowledge(UUID intentId, NormalizedBinanceExecution execution,
                            BinanceFuturesOrderClientResponse response) {
        CopyDispatchIntentEntity intent = required(intentId);
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        String nextStatus = execution.executionState().name();
        CopyDispatchStatePolicy.requireTransition(intent.getStatus(), nextStatus);
        intent.setStatus(nextStatus);
        intent.setBinanceOrderId(execution.orderId());
        intent.setBinanceStatus(trim(execution.status(), 32));
        intent.setExecutedQty(execution.executedQty());
        intent.setAveragePrice(execution.averagePrice());
        intent.setCumulativeQuoteQty(execution.cumulativeQuoteQty());
        intent.setAveragePriceStatus(execution.averagePriceStatus().name());
        intent.setAcknowledgedAt(now);
        if (execution.executionState() == CopyExecutionState.FILLED) intent.setFilledAt(now);
        if (execution.requiresReconciliation() || execution.averagePriceStatus() == AveragePriceStatus.PENDING_RESOLUTION) {
            intent.setNextReconciliationAt(now.plusSeconds(2));
        }
        intent.setResponseSnapshot(snapshot(response));
        intent.setLastErrorCode(null);
        intent.setLastErrorDetail(null);
        repository.saveAndFlush(intent);
    }

    @Override
    @Transactional
    public void markAmbiguous(UUID intentId, String reasonCode, String detail) {
        CopyDispatchIntentEntity intent = required(intentId);
        if (CopyDispatchStatePolicy.terminalWithoutAutomaticSend(intent.getStatus())) {
            meterRegistry.counter("copy_integrity_violation", "reason_code", "terminal_to_ambiguous").increment();
            log.error("event=copy.integrity.violation dispatchIntentId={} reasonCode=TERMINAL_TO_AMBIGUOUS_BLOCKED currentStatus={} requestedStatus=RECONCILING",
                    intentId, intent.getStatus());
            return;
        }
        CopyDispatchStatePolicy.requireTransition(intent.getStatus(), "RECONCILING");
        intent.setStatus("RECONCILING");
        intent.setReservationStatus("PENDING");
        intent.setLastErrorCode(trim(reasonCode, 80));
        intent.setLastErrorDetail(trim(detail, 1000));
        intent.setNextReconciliationAt(OffsetDateTime.now(ZoneOffset.UTC).plusSeconds(2));
        repository.saveAndFlush(intent);
        meterRegistry.counter("copy_dispatch_ambiguous", "execution_mode", safeTag(intent.getExecutionMode()),
                "result", "ambiguous").increment();
    }

    @Override
    @Transactional
    public void markRejected(UUID intentId, String reasonCode, String detail) {
        CopyDispatchIntentEntity intent = required(intentId);
        if (intent.getExecutedQty() != null && intent.getExecutedQty().compareTo(ZERO) > 0) {
            meterRegistry.counter("copy_integrity_violation", "reason_code", "reject_with_fill").increment();
            throw new IllegalStateException("Cannot release a dispatch intent that already has executed quantity");
        }
        if ("PERSISTED".equals(intent.getStatus()) || "MANUAL_REVIEW".equals(intent.getStatus())) {
            meterRegistry.counter("copy_integrity_violation", "reason_code", "terminal_to_rejected").increment();
            throw new IllegalStateException("Cannot reject terminal dispatch intent " + intent.getStatus());
        }
        CopyDispatchStatePolicy.requireTransition(intent.getStatus(), "REJECTED");
        intent.setStatus("REJECTED");
        intent.setReservationStatus("RELEASED");
        intent.setLastErrorCode(trim(reasonCode, 80));
        intent.setLastErrorDetail(trim(detail, 1000));
        repository.saveAndFlush(intent);
        meterRegistry.counter("copy_dispatch_rejected", "execution_mode", safeTag(intent.getExecutionMode()),
                "result", "rejected").increment();
        log.info("event=copy.budget.released dispatchIntentId={} releaseReason={}", intentId, safe(reasonCode));
    }

    @Override
    @Transactional
    public void linkRequiredEvent(UUID intentId, UUID copyOperationEventId) {
        if (intentId == null || copyOperationEventId == null) {
            throw new IllegalArgumentException("dispatch intent and required ledger event are required");
        }
        CopyDispatchIntentEntity intent = required(intentId);
        if (CopyDispatchStatePolicy.terminalWithoutAutomaticSend(intent.getStatus())
                && !"PERSISTED".equals(intent.getStatus())) {
            throw new IllegalStateException("Cannot link ledger event to terminal intent " + intent.getStatus());
        }
        intent.setCopyOperationEventId(copyOperationEventId);
        repository.saveAndFlush(intent);
    }

    @Override
    @Transactional
    public void markPersistencePending(String clientOrderId, String reasonCode, String detail) {
        markPersistencePending(null, clientOrderId, reasonCode, detail);
    }

    @Override
    @Transactional
    public void markPersistencePending(UUID intentId, String clientOrderId, String reasonCode, String detail) {
        for (CopyDispatchIntentEntity intent : matchingIntents(intentId, clientOrderId)) {
            if ("PERSISTED".equals(intent.getStatus())) continue;
            requireMatchingClientOrderId(intent, clientOrderId);
            CopyDispatchStatePolicy.requireTransition(intent.getStatus(), "PERSISTENCE_PENDING");
            intent.setStatus("PERSISTENCE_PENDING");
            intent.setReservationStatus("PENDING");
            intent.setLastErrorCode(trim(reasonCode, 80));
            intent.setLastErrorDetail(trim(detail, 1000));
            intent.setNextReconciliationAt(OffsetDateTime.now(ZoneOffset.UTC).plusSeconds(2));
            repository.save(intent);
            log.warn("event=copy.dispatch.persistence_pending dispatchIntentId={} orderId={} clientOrderId={} reasonCode={} decision=RECONCILE_NOT_RESEND",
                    intent.getId(), intent.getBinanceOrderId(), safe(intent.getClientOrderId()),
                    safe(reasonCode));
        }
        repository.flush();
    }

    @Override
    @Transactional
    public void markPersisted(String clientOrderId, UUID copyOperationId) {
        markPersisted(null, clientOrderId, copyOperationId);
    }

    @Override
    @Transactional
    public void markPersisted(UUID intentId, String clientOrderId, UUID copyOperationId) {
        for (CopyDispatchIntentEntity intent : matchingIntents(intentId, clientOrderId)) {
            if (intent.getBinanceOrderId() == null || copyOperationId == null) {
                throw new IllegalStateException("COPY_OPERATION_LINK_MISSING: PERSISTED requires Binance order and copy_operation");
            }
            if (intent.getCopyOperationEventId() == null) {
                intent.setLastErrorCode("COPY_REQUIRED_LEDGER_LINK_MISSING");
                repository.saveAndFlush(intent);
                throw new IllegalStateException("COPY_REQUIRED_LEDGER_LINK_MISSING: PERSISTED requires copy_operation_event");
            }
            requireMatchingClientOrderId(intent, clientOrderId);
            OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
            boolean partial = "PARTIALLY_FILLED".equals(intent.getStatus());
            boolean pricePending = "PENDING_RESOLUTION".equals(intent.getAveragePriceStatus());
            String nextStatus = partial ? "PARTIALLY_FILLED" : "PERSISTED";
            CopyDispatchStatePolicy.requireTransition(intent.getStatus(), nextStatus);
            intent.setPersistedExecutedQty(max(intent.getPersistedExecutedQty(), intent.getExecutedQty()));
            intent.setStatus(nextStatus);
            intent.setReservationStatus(partial ? "PENDING" : "CONFIRMED");
            intent.setCopyOperationId(copyOperationId);
            intent.setPersistedAt(now);
            intent.setNextReconciliationAt(partial || pricePending ? now.plusSeconds(30) : null);
            repository.save(intent);
            meterRegistry.counter("copy_dispatch_persisted", "execution_mode", safeTag(intent.getExecutionMode()),
                    "result", partial ? "partial" : "persisted").increment();
            if (partial) {
                meterRegistry.counter("copy_partial_progress", "execution_mode", safeTag(intent.getExecutionMode()),
                        "result", "persisted").increment();
            }
            log.info("event=copy.budget.{} dispatchIntentId={} userCopyAllocationId={} usedMarginUsd=tracked_by_copy_operation reservedMarginUsd={} persistedExecutedQty={}",
                    partial ? "retained" : "confirmed", intent.getId(), intent.getUserCopyAllocationId(),
                    partial ? intent.getRequestedMarginUsd() : ZERO, intent.getPersistedExecutedQty());
            log.info("event=copy.dispatch.persisted dispatchIntentId={} copyOperationId={} copyOperationEventId={} orderId={} clientOrderId={}",
                    intent.getId(), copyOperationId, intent.getCopyOperationEventId(), intent.getBinanceOrderId(),
                    safe(intent.getClientOrderId()));
        }
        repository.flush();
    }

    private CopyDispatchPermit duplicatePermit(CopyDispatchIntentEntity intent, CopyDispatchRequest request) {
        String status = intent.getStatus();
        if (!Objects.equals(intent.getRequestHash(), request.requestHash())) {
            log.error("event=copy.dispatch.intent.conflict idempotencyKey={} existingIntentId={} existingStatus={} decision=BLOCK_PAYLOAD_MISMATCH",
                    intent.getIdempotencyKey(), intent.getId(), status);
            meterRegistry.counter("copy_dispatch_payload_conflict", "execution_mode", safeTag(intent.getExecutionMode()),
                    "result", "blocked").increment();
            return CopyDispatchPermit.conflict(intent.getId(), status);
        }
        if ("PERSISTED".equals(status)
                || (intent.getCopyOperationId() != null && !"PERSISTENCE_PENDING".equals(status))) {
            return CopyDispatchPermit.noop(intent.getId(), status);
        }
        if (List.of("REJECTED", "FAILED_FINAL", "CANCELLED", "MANUAL_REVIEW").contains(status)) {
            return CopyDispatchPermit.rejected(intent.getId(), status);
        }
        if (intent.getBinanceOrderId() != null && List.of("NEW", "PARTIALLY_FILLED", "FILLED", "PERSISTENCE_PENDING", "PERSISTED").contains(status)) {
            return CopyDispatchPermit.reuse(intent.getId(), response(intent), status);
        }
        return CopyDispatchPermit.reconcile(intent.getId(), status);
    }

    private BinanceFuturesOrderClientResponse response(CopyDispatchIntentEntity intent) {
        BinanceFuturesOrderClientResponse response = new BinanceFuturesOrderClientResponse();
        response.setOrderId(intent.getBinanceOrderId());
        response.setClientOrderId(intent.getClientOrderId());
        response.setSymbol(intent.getSymbol());
        response.setStatus(intent.getBinanceStatus());
        response.setExecutedQty(intent.getExecutedQty());
        response.setCumQty(intent.getExecutedQty());
        response.setOrigQty(intent.getRequestedQty());
        response.setAvgPrice(intent.getAveragePrice());
        response.setCumQuote(intent.getCumulativeQuoteQty());
        response.setSide(intent.getSide());
        response.setPositionSide(intent.getPositionSide());
        response.setReduceOnly(intent.isReduceOnly());
        response.setAccepted(true);
        response.setExecutionState(intent.getStatus());
        response.setAveragePriceStatus(intent.getAveragePriceStatus());
        response.setRequiresReconciliation(!"FILLED".equals(intent.getStatus()) || "PENDING_RESOLUTION".equals(intent.getAveragePriceStatus()));
        response.setSafeToRetrySend(false);
        response.setDispatchIntentId(intent.getId());
        response.setSourceEventId(intent.getSourceEventId());
        response.setReferencePrice(intent.getReferencePrice());
        response.setUpdateTime(intent.getAcknowledgedAt() == null ? null : intent.getAcknowledgedAt().toInstant().toEpochMilli());
        return response;
    }

    private BudgetSnapshot budgetSnapshot(CopyDispatchIdentity identity) {
        CopyBudgetSnapshotProjection snapshot = repository.loadBudgetSnapshot(
                identity.userId(), identity.userCopyAllocationId(), realMode(identity.executionMode()));
        if (snapshot == null) return BudgetSnapshot.empty();
        return new BudgetSnapshot(
                nonNegative(snapshot.getUsedMarginUsd()),
                nonNegative(snapshot.getReservedPendingMarginUsd()),
                safeInt(snapshot.getOpenPositions()),
                safeInt(snapshot.getReservedPositions()));
    }

    private int safeInt(Long value) {
        return value == null ? 0 : Math.toIntExact(value);
    }

    private List<CopyDispatchIntentEntity> matchingIntents(UUID intentId, String clientOrderId) {
        return intentId == null ? repository.findAllByClientOrderId(clientOrderId) : List.of(required(intentId));
    }

    private void requireMatchingClientOrderId(CopyDispatchIntentEntity intent, String clientOrderId) {
        if (clientOrderId != null && !clientOrderId.isBlank() && !clientOrderId.equals(intent.getClientOrderId())) {
            throw new IllegalStateException("dispatch intent/clientOrderId mismatch");
        }
    }

    private void lockBudget(CopyDispatchIdentity identity) {
        String key = identity.userId() + '|' + identity.userCopyAllocationId() + '|' + realMode(identity.executionMode());
        entityManager.createNativeQuery("select pg_advisory_xact_lock(hashtextextended(cast(:lockKey as text), 0))")
                .setParameter("lockKey", key)
                .getSingleResult();
    }

    private CopyDispatchIntentEntity required(UUID id) {
        return repository.findById(id).orElseThrow(() -> new IllegalStateException("copy dispatch intent not found: " + id));
    }

    private void validate(CopyDispatchRequest request) {
        if (request == null || request.identity() == null) throw new IllegalArgumentException("copy dispatch request is required");
        if (request.identity().userId() == null || request.identity().userId().isBlank()) throw new IllegalArgumentException("userId is required");
        if (request.identity().userCopyAllocationId() == null) throw new SkipExecutionException("COPY_ALLOCATION_REQUIRED_FOR_REAL_DISPATCH", "Allocation durable requerida para MICRO_LIVE/LIVE", null);
        if (request.operation() == null || request.operation().getClientOrderId() == null || request.operation().getClientOrderId().isBlank()) throw new IllegalArgumentException("clientOrderId is required");
    }

    private String snapshot(BinanceFuturesOrderClientResponse response) {
        if (response == null) return null;
        try {
            return trim(objectMapper.writeValueAsString(response), 4000);
        } catch (JsonProcessingException ex) {
            return "{\"snapshotError\":\"" + ex.getClass().getSimpleName() + "\"}";
        }
    }

    private String realMode(String mode) {
        String normalized = mode == null ? null : mode.trim().toUpperCase(Locale.ROOT).replace('-', '_');
        if ("MICRO_LIVE".equals(normalized) || "LIVE".equals(normalized)) return normalized;
        throw new IllegalArgumentException("Unsupported real execution mode: " + mode);
    }

    private BigDecimal max(BigDecimal first, BigDecimal second) {
        return nonNegative(first).max(nonNegative(second));
    }

    private void recordClaim(long startedNs, String executionMode, String result) {
        long elapsed = Math.max(0L, System.nanoTime() - startedNs);
        meterRegistry.timer("copy_dispatch_claim_duration", "execution_mode", safeTag(executionMode),
                "result", safeTag(result)).record(elapsed, java.util.concurrent.TimeUnit.NANOSECONDS);
        meterRegistry.timer("copy_reservation_duration", "execution_mode", safeTag(executionMode),
                "result", safeTag(result)).record(elapsed, java.util.concurrent.TimeUnit.NANOSECONDS);
    }

    private BigDecimal nonNegative(BigDecimal value) { return value == null ? ZERO : value.max(ZERO); }
    private String safeTag(String value) { return value == null || value.isBlank() ? "unknown" : value.toLowerCase(Locale.ROOT); }
    private String safe(String value) { return value == null ? "" : value.replace('\n', ' ').replace('\r', ' ').replace('"', '\''); }
    private String trim(String value, int max) { return value == null ? null : value.length() <= max ? value : value.substring(0, max); }
}
