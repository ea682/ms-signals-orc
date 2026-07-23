package com.apunto.engine.service.copy.certification;

import com.apunto.engine.entity.UserCopyAllocationEntity;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AutomaticLiveCertificationServiceTest {

    private final FakeStore store = new FakeStore();
    private final Clock clock = Clock.fixed(Instant.parse("2026-07-17T12:00:00Z"), ZoneOffset.UTC);
    private final AutomaticLiveCertificationService service = new AutomaticLiveCertificationService(
            store,
            new ManualLiveCertificationCatalogService(store, store, clock),
            new ManualLiveCertificationService(store, new LiveCertificationTransitionPolicy(), clock),
            new LiveCertificationRuntimeProperties());

    @Test
    void oneValidRealMicroLiveTestCreatesGlobalApprovedCertification() {
        AutomaticLiveCertificationResult result = service.certify(microAllocation(), Map.of(
                "submittedOrders", 12L,
                "filledOrders", 12L,
                "closedOperations", 3L,
                "dispatchErrors", 0L));

        assertTrue(result.approved(), result.reasonCode());
        assertEquals(LiveCertificationStatus.LIVE_APPROVED, store.record.status());
        assertEquals(2, store.audits.size());
        assertEquals(Boolean.TRUE, store.createdEvidence.get("realMicroLiveEvidence"));
        assertEquals(1, store.createdEvidence.get("validMicroLiveTests"));
    }

    @Test
    void incompleteMicroLiveEvidenceCannotCreateCertification() {
        AutomaticLiveCertificationResult result = service.certify(microAllocation(), Map.of(
                "submittedOrders", 12L,
                "filledOrders", 12L,
                "closedOperations", 0L));

        assertFalse(result.approved());
        assertEquals("LIVE_CERTIFICATION_REAL_MICRO_EVIDENCE_REQUIRED", result.reasonCode());
        assertTrue(store.audits.isEmpty());
    }

    private static UserCopyAllocationEntity microAllocation() {
        return UserCopyAllocationEntity.builder()
                .id(42L)
                .idUser(UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"))
                .walletId("0xABC")
                .copyStrategyCode("MOVEMENT_ALL")
                .scopeType("ALL")
                .scopeValue("ALL")
                .executionMode("MICRO_LIVE")
                .resolvedQuoteAsset("USDC")
                .build();
    }

    private static final class FakeStore
            implements LiveCertificationCatalogStore, LiveCertificationTransitionStore {
        private LiveCertificationCatalogRecord record;
        private final List<LiveCertificationAuditFact> audits = new ArrayList<>();
        private final Map<String, LiveCertificationAuditFact> auditsByKey = new HashMap<>();
        private Map<String, Object> createdEvidence = Map.of();

        @Override
        public Optional<LiveCertificationCatalogRecord> findByCreationKey(String creationKey) {
            return Optional.empty();
        }

        @Override
        public Optional<LiveCertificationCatalogRecord> findByIdentity(LiveCertificationIdentity identity) {
            return record != null && record.identity().equals(identity) ? Optional.of(record) : Optional.empty();
        }

        @Override
        public boolean insert(LiveCertificationCatalogRecord value, String creationKey,
                              Map<String, Object> evidenceSnapshot, String actor, String reason) {
            if (record != null) return false;
            record = value;
            createdEvidence = Map.copyOf(evidenceSnapshot);
            return true;
        }

        @Override
        public Optional<LiveCertificationIdentity> findIdentityById(UUID certificationId) {
            return record != null && record.id().equals(certificationId)
                    ? Optional.of(record.identity()) : Optional.empty();
        }

        @Override
        public Optional<LiveCertificationSnapshot> lockById(UUID certificationId) {
            return record != null && record.id().equals(certificationId)
                    ? Optional.of(record.snapshot()) : Optional.empty();
        }

        @Override
        public Optional<LiveCertificationAuditFact> findAuditByTransitionKey(String transitionKey) {
            return Optional.ofNullable(auditsByKey.get(transitionKey));
        }

        @Override
        public boolean compareAndSet(UUID certificationId, long expectedVersion,
                                     LiveCertificationStatus expectedStatus,
                                     LiveCertificationStatus nextStatus) {
            if (record == null || !record.id().equals(certificationId)
                    || record.version() != expectedVersion || record.status() != expectedStatus) {
                return false;
            }
            record = new LiveCertificationCatalogRecord(record.id(), record.identity(), record.evidenceLevel(),
                    nextStatus, expectedVersion + 1);
            return true;
        }

        @Override
        public void appendAudit(LiveCertificationAuditFact audit) {
            audits.add(audit);
            auditsByKey.put(audit.transitionKey(), audit);
        }
    }
}
