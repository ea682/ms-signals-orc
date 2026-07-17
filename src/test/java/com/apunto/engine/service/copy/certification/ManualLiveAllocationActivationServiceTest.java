package com.apunto.engine.service.copy.certification;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ManualLiveAllocationActivationServiceTest {

    private static final OffsetDateTime NOW = OffsetDateTime.parse("2026-07-13T12:00:00Z");
    private static final UUID CERT_ID = UUID.fromString("11111111-1111-1111-1111-111111111111");
    private final FakeStore store = new FakeStore();
    private final ManualLiveAllocationActivationService service = new ManualLiveAllocationActivationService(
            store, Clock.fixed(Instant.parse("2026-07-13T12:00:00Z"), ZoneOffset.UTC));

    @Test
    void activatesTheSameAdoptedAllocationOnlyWhenFlatAndSettled() {
        store.authorization = validAuthorization();

        LiveAllocationActivationResult result = service.activate(command("activate-55"));

        assertTrue(result.activated());
        assertEquals("LIVE", store.snapshot.executionMode());
        assertEquals(1, store.auditByKey.size());
    }

    @Test
    void openMicroLivePositionBlocksPolicyChangeMidCycle() {
        store.authorization = validAuthorization();
        store.openOperations = 1;

        LiveAllocationActivationResult result = service.activate(command("activate-open"));

        assertFalse(result.activated());
        assertEquals("LIVE_ACTIVATION_OPEN_POSITIONS_EXIST", result.reasonCode());
        assertEquals("MICRO_LIVE", store.snapshot.executionMode());
    }

    @Test
    void expiredAdoptionBlocksActivation() {
        LiveActivationAuthorization authorization = validAuthorization();
        store.authorization = authorization.withExpiresAt(NOW.minusSeconds(1));

        LiveAllocationActivationResult result = service.activate(command("activate-expired"));

        assertFalse(result.activated());
        assertEquals("LIVE_ADOPTION_EXPIRED", result.reasonCode());
    }

    @Test
    void activationReplayIsIdempotent() {
        store.authorization = validAuthorization();
        LiveAllocationActivationCommand command = command("activate-once");

        LiveAllocationActivationResult first = service.activate(command);
        LiveAllocationActivationResult replay = service.activate(command);

        assertTrue(first.activated());
        assertTrue(replay.activated());
        assertTrue(replay.idempotent());
        assertEquals(1, store.updateCount);
    }

    private LiveAllocationActivationCommand command(String key) {
        return new LiveAllocationActivationCommand(
                55L, CERT_ID, "operator@example.com", "approved for live", key);
    }

    private LiveActivationAuthorization validAuthorization() {
        return new LiveActivationAuthorization(
                CERT_ID, LiveCertificationStatus.LIVE_APPROVED, "VALID", true,
                NOW.minusMinutes(1), NOW.plusMinutes(10));
    }

    private static final class FakeStore implements LiveAllocationActivationStore {
        private LiveAllocationActivationSnapshot snapshot = new LiveAllocationActivationSnapshot(
                55L, UUID.fromString("22222222-2222-2222-2222-222222222222"),
                "0xabc", "MOVEMENT_ALL", "ALL", "ALL", "MICRO_LIVE", "ACTIVE", true, null);
        private LiveActivationAuthorization authorization;
        private long openOperations;
        private long pendingIntents;
        private int updateCount;
        private final Map<String, LiveAllocationActivationAudit> auditByKey = new HashMap<>();

        @Override public Optional<LiveAllocationActivationSnapshot> lockAllocation(Long id) { return Optional.ofNullable(snapshot); }
        @Override public Optional<LiveAllocationActivationAudit> findAudit(String key) { return Optional.ofNullable(auditByKey.get(key)); }
        @Override public long countOpenOperations(Long id) { return openOperations; }
        @Override public long countNonTerminalIntents(Long id) { return pendingIntents; }
        @Override public Optional<LiveActivationAuthorization> findAuthorization(Long id, UUID cert) { return Optional.ofNullable(authorization); }
        @Override public boolean activate(Long id, String actor, String reason, OffsetDateTime at) {
            if (!"MICRO_LIVE".equals(snapshot.executionMode())) return false;
            snapshot = snapshot.withExecutionMode("LIVE");
            updateCount++;
            return true;
        }
        @Override public void appendAudit(LiveAllocationActivationAudit audit) { auditByKey.put(audit.activationKey(), audit); }
    }
}
