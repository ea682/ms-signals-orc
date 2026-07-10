package com.apunto.engine.service.copy.dispatch;

import com.apunto.engine.entity.CopyDispatchIntentEntity;
import com.apunto.engine.repository.CopyDispatchIntentRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class CopyOrderReconciliationService {

    private final CopyDispatchIntentRepository repository;

    @Transactional
    public List<CopyDispatchIntentEntity> claimBatch(int limit, Duration dispatchStaleAfter, String workerId) {
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        Duration stale = dispatchStaleAfter == null ? Duration.ofSeconds(30) : dispatchStaleAfter;
        List<UUID> ids = repository.findReconciliationIdsForUpdateSkipLocked(now, now.minus(stale), Math.max(1, limit));
        if (ids.isEmpty()) return List.of();
        List<CopyDispatchIntentEntity> intents = repository.findAllById(ids);
        for (CopyDispatchIntentEntity intent : intents) {
            CopyDispatchStatePolicy.requireTransition(intent.getStatus(), "RECONCILING");
            intent.setStatus("RECONCILING");
            intent.setReconciliationAttempts(intent.getReconciliationAttempts() + 1);
            intent.setClaimedBy(workerId);
            intent.setClaimedAt(now);
            intent.setNextReconciliationAt(now.plusSeconds(claimLeaseSeconds(stale, intent.getReconciliationAttempts())));
        }
        repository.saveAllAndFlush(intents);
        return intents;
    }

    @Transactional
    public void markLookupNotFound(UUID intentId, int maxAttempts) {
        CopyDispatchIntentEntity intent = required(intentId);
        boolean exhausted = intent.getReconciliationAttempts() >= maxAttempts;
        String nextStatus = exhausted ? "MANUAL_REVIEW" : "RECONCILING";
        CopyDispatchStatePolicy.requireTransition(intent.getStatus(), nextStatus);
        intent.setStatus(nextStatus);
        // Fail closed: an unproven absence may still be a real Binance order.
        intent.setReservationStatus("PENDING");
        intent.setLastErrorCode(exhausted ? "ORDER_NOT_FOUND_REQUIRES_MANUAL_REVIEW" : "ORDER_NOT_FOUND_YET");
        intent.setLastErrorDetail("No automatic resend: Binance non-existence is not proven safely");
        intent.setNextReconciliationAt(exhausted ? null
                : OffsetDateTime.now(ZoneOffset.UTC).plusSeconds(backoffSeconds(intent.getReconciliationAttempts())));
        repository.saveAndFlush(intent);
    }

    @Transactional
    public void deferNewOrder(UUID intentId) {
        CopyDispatchIntentEntity intent = required(intentId);
        CopyDispatchStatePolicy.requireTransition(intent.getStatus(), "NEW");
        intent.setStatus("NEW");
        intent.setReservationStatus("PENDING");
        intent.setNextReconciliationAt(OffsetDateTime.now(ZoneOffset.UTC).plusSeconds(backoffSeconds(intent.getReconciliationAttempts())));
        repository.saveAndFlush(intent);
    }

    @Transactional
    public void markUnresolvedTerminal(UUID intentId, String reasonCode) {
        CopyDispatchIntentEntity intent = required(intentId);
        CopyDispatchStatePolicy.requireTransition(intent.getStatus(), "MANUAL_REVIEW");
        intent.setStatus("MANUAL_REVIEW");
        // The exchange outcome/exposure is not fully resolved. Keep budget fail-closed.
        intent.setReservationStatus("PENDING");
        intent.setLastErrorCode(trim(reasonCode, 80));
        intent.setLastErrorDetail("Automatic reconciliation exhausted; manual Binance review required; no resend");
        intent.setNextReconciliationAt(null);
        repository.saveAndFlush(intent);
    }

    @Transactional
    public void markPriceResolutionExhausted(UUID intentId) {
        CopyDispatchIntentEntity intent = required(intentId);
        CopyDispatchStatePolicy.requireTransition(intent.getStatus(), "MANUAL_REVIEW");
        intent.setStatus("MANUAL_REVIEW");
        // The fill is already represented by copy_operation; only the final price
        // remains provisional, so active operation margin owns the budget now.
        intent.setReservationStatus("CONFIRMED");
        intent.setLastErrorCode("PRICE_RESOLUTION_EXHAUSTED");
        intent.setLastErrorDetail("Reference price retained; manual fill/trade reconciliation required");
        intent.setNextReconciliationAt(null);
        repository.saveAndFlush(intent);
    }

    @Transactional
    public void markFailure(UUID intentId, int maxAttempts, String code, String detail) {
        CopyDispatchIntentEntity intent = required(intentId);
        boolean exhausted = intent.getReconciliationAttempts() >= maxAttempts;
        String nextStatus = exhausted ? "MANUAL_REVIEW" : "RECONCILING";
        CopyDispatchStatePolicy.requireTransition(intent.getStatus(), nextStatus);
        intent.setStatus(nextStatus);
        intent.setReservationStatus("PENDING");
        intent.setLastErrorCode(trim(code, 80));
        intent.setLastErrorDetail(trim(detail, 1000));
        intent.setNextReconciliationAt(exhausted ? null
                : OffsetDateTime.now(ZoneOffset.UTC).plusSeconds(backoffSeconds(intent.getReconciliationAttempts())));
        repository.saveAndFlush(intent);
    }

    private CopyDispatchIntentEntity required(UUID id) {
        return repository.findById(id).orElseThrow(() -> new IllegalStateException("copy dispatch intent not found: " + id));
    }

    static long claimLeaseSeconds(Duration configuredLease, int attempts) {
        Duration lease = configuredLease == null || configuredLease.isZero() || configuredLease.isNegative()
                ? Duration.ofSeconds(30) : configuredLease;
        long minimumLeaseSeconds = Math.max(1L, lease.toSeconds());
        return Math.max(minimumLeaseSeconds, backoffSeconds(attempts));
    }

    private static long backoffSeconds(int attempts) {
        int exponent = Math.min(Math.max(0, attempts - 1), 8);
        return Math.min(300, 2L << exponent);
    }

    private String trim(String value, int max) {
        if (value == null) return null;
        String clean = value.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ');
        return clean.length() <= max ? clean : clean.substring(0, max);
    }
}
