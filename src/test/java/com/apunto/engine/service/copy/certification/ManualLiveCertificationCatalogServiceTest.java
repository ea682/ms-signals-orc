package com.apunto.engine.service.copy.certification;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ManualLiveCertificationCatalogServiceTest {

    private final FakeCatalogStore catalog = new FakeCatalogStore();
    private final FakeTransitionStore audits = new FakeTransitionStore();
    private final ManualLiveCertificationCatalogService service =
            new ManualLiveCertificationCatalogService(catalog, audits,
                    Clock.fixed(Instant.parse("2026-07-13T12:00:00Z"), ZoneOffset.UTC));

    @Test
    void createsValidationStateAndImmutableCreationAudit() {
        LiveCertificationCreateResult result = service.create(command(
                LiveCertificationStatus.MICRO_LIVE_VALIDATING, "create-a445-v3"));

        assertTrue(result.created());
        assertFalse(result.idempotent());
        assertEquals(LiveCertificationStatus.MICRO_LIVE_VALIDATING,
                result.certification().status());
        assertEquals(1, audits.auditCount);
    }

    @Test
    void creationKeyReplayIsIdempotent() {
        LiveCertificationCreateCommand command = command(
                LiveCertificationStatus.MICRO_LIVE_VALIDATING, "create-a445-v3");

        LiveCertificationCreateResult first = service.create(command);
        LiveCertificationCreateResult replay = service.create(command);

        assertTrue(first.created());
        assertTrue(replay.created());
        assertTrue(replay.idempotent());
        assertEquals(1, catalog.insertCount);
        assertEquals(1, audits.auditCount);
    }

    @Test
    void cannotCreateDirectlyAsLiveApproved() {
        LiveCertificationCreateResult result = service.create(command(
                LiveCertificationStatus.LIVE_APPROVED, "unsafe-direct-live"));

        assertFalse(result.created());
        assertEquals("LIVE_CERTIFICATION_INITIAL_STATUS_INVALID", result.reasonCode());
        assertEquals(0, catalog.insertCount);
    }

    private LiveCertificationCreateCommand command(LiveCertificationStatus status, String key) {
        return new LiveCertificationCreateCommand(
                TestCertificationFixtures.identity(
                        new java.math.BigDecimal("100"), new java.math.BigDecimal("250"),
                        new java.math.BigDecimal("5")),
                LiveEvidenceLevel.MICRO_LIVE_CALIBRATED,
                status,
                "operator@example.com",
                "initial evidence reviewed",
                Map.of("calibrationId", "cal-1"),
                key);
    }

    private static final class FakeCatalogStore implements LiveCertificationCatalogStore {
        private final Map<String, LiveCertificationCatalogRecord> byKey = new HashMap<>();
        private int insertCount;

        @Override
        public Optional<LiveCertificationCatalogRecord> findByCreationKey(String creationKey) {
            return Optional.ofNullable(byKey.get(creationKey));
        }

        @Override
        public Optional<LiveCertificationCatalogRecord> findByIdentity(LiveCertificationIdentity identity) {
            return byKey.values().stream().filter(row -> row.identity().equals(identity)).findFirst();
        }

        @Override
        public boolean insert(LiveCertificationCatalogRecord record, String creationKey,
                              Map<String, Object> evidenceSnapshot, String actor, String reason) {
            if (byKey.containsKey(creationKey)) return false;
            byKey.put(creationKey, record);
            insertCount++;
            return true;
        }

        @Override
        public Optional<LiveCertificationIdentity> findIdentityById(UUID certificationId) {
            return byKey.values().stream()
                    .filter(row -> row.id().equals(certificationId))
                    .map(LiveCertificationCatalogRecord::identity)
                    .findFirst();
        }
    }

    private static final class FakeTransitionStore implements LiveCertificationTransitionStore {
        private int auditCount;

        @Override public Optional<LiveCertificationSnapshot> lockById(UUID id) { return Optional.empty(); }
        @Override public Optional<LiveCertificationAuditFact> findAuditByTransitionKey(String key) { return Optional.empty(); }
        @Override public boolean compareAndSet(UUID id, long version, LiveCertificationStatus prior, LiveCertificationStatus next) { return false; }
        @Override public void appendAudit(LiveCertificationAuditFact audit) { auditCount++; }
    }
}
