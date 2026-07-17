package com.apunto.engine.service.copy.certification;

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

class ManualLiveCertificationServiceTest {

    private static final UUID CERTIFICATION_ID = UUID.fromString("11111111-1111-1111-1111-111111111111");
    private final FakeStore store = new FakeStore();
    private final ManualLiveCertificationService service = new ManualLiveCertificationService(
            store,
            new LiveCertificationTransitionPolicy(),
            Clock.fixed(Instant.parse("2026-07-13T12:00:00Z"), ZoneOffset.UTC));

    @Test
    void compareAndSetTransitionAppendsOneImmutableAuditFact() {
        store.snapshot = new LiveCertificationSnapshot(
                CERTIFICATION_ID, LiveCertificationStatus.MICRO_LIVE_VALIDATING, 7L);

        LiveCertificationTransitionResult result = service.transition(command("approve-7", 7L));

        assertTrue(result.applied());
        assertFalse(result.idempotent());
        assertEquals(LiveCertificationStatus.LIVE_APPROVED, store.snapshot.status());
        assertEquals(8L, store.snapshot.version());
        assertEquals(1, store.audits.size());
        assertEquals("operator@example.com", store.audits.getFirst().actor());
    }

    @Test
    void duplicateTransitionKeyIsIdempotentAndDoesNotAppendAgain() {
        store.snapshot = new LiveCertificationSnapshot(
                CERTIFICATION_ID, LiveCertificationStatus.MICRO_LIVE_VALIDATING, 7L);
        LiveCertificationTransitionCommand command = command("approve-7", 7L);

        LiveCertificationTransitionResult first = service.transition(command);
        LiveCertificationTransitionResult replay = service.transition(command);

        assertTrue(first.applied());
        assertTrue(replay.applied());
        assertTrue(replay.idempotent());
        assertEquals(1, store.audits.size());
        assertEquals(1, store.updateCount);
    }

    @Test
    void staleExpectedVersionCannotOverwriteNewerState() {
        store.snapshot = new LiveCertificationSnapshot(
                CERTIFICATION_ID, LiveCertificationStatus.MICRO_LIVE_VALIDATING, 8L);

        LiveCertificationTransitionResult result = service.transition(command("approve-stale", 7L));

        assertFalse(result.applied());
        assertEquals("LIVE_CERTIFICATION_VERSION_CONFLICT", result.reasonCode());
        assertEquals(0, store.updateCount);
        assertTrue(store.audits.isEmpty());
    }

    private LiveCertificationTransitionCommand command(String key, long version) {
        return new LiveCertificationTransitionCommand(
                CERTIFICATION_ID,
                version,
                LiveCertificationStatus.MICRO_LIVE_VALIDATING,
                LiveCertificationStatus.LIVE_APPROVED,
                false,
                "operator@example.com",
                "manual review complete",
                Map.of("calibrationId", "cal-1", "sampleCount", 30),
                key);
    }

    private static final class FakeStore implements LiveCertificationTransitionStore {
        private LiveCertificationSnapshot snapshot;
        private final List<LiveCertificationAuditFact> audits = new ArrayList<>();
        private final Map<String, LiveCertificationAuditFact> byKey = new HashMap<>();
        private int updateCount;

        @Override
        public Optional<LiveCertificationSnapshot> lockById(UUID certificationId) {
            return snapshot != null && snapshot.id().equals(certificationId)
                    ? Optional.of(snapshot)
                    : Optional.empty();
        }

        @Override
        public Optional<LiveCertificationAuditFact> findAuditByTransitionKey(String transitionKey) {
            return Optional.ofNullable(byKey.get(transitionKey));
        }

        @Override
        public boolean compareAndSet(UUID certificationId, long expectedVersion,
                                     LiveCertificationStatus expectedStatus,
                                     LiveCertificationStatus nextStatus) {
            if (snapshot == null || snapshot.version() != expectedVersion || snapshot.status() != expectedStatus) {
                return false;
            }
            snapshot = new LiveCertificationSnapshot(certificationId, nextStatus, expectedVersion + 1);
            updateCount++;
            return true;
        }

        @Override
        public void appendAudit(LiveCertificationAuditFact audit) {
            audits.add(audit);
            byKey.put(audit.transitionKey(), audit);
        }
    }
}
