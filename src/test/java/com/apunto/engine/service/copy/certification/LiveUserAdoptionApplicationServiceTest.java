package com.apunto.engine.service.copy.certification;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LiveUserAdoptionApplicationServiceTest {

    private static final UUID CERT_ID = UUID.fromString("11111111-1111-1111-1111-111111111111");
    private static final OffsetDateTime NOW = OffsetDateTime.parse("2026-07-13T12:00:00Z");

    @Test
    void certificationIdentityIsLoadedServerSideBeforeValidation() {
        CapturingAdoptionStore adoptionStore = new CapturingAdoptionStore();
        LiveCertificationIdentity identity = TestCertificationFixtures.identity(
                new BigDecimal("100"), new BigDecimal("250"), new BigDecimal("5"));
        LiveUserAdoptionApplicationService service = service(
                new LookupCatalogStore(Optional.of(identity)), adoptionStore);

        LiveUserAdoptionResult result = service.validateAndPersist(command(CERT_ID));

        assertTrue(result.persisted());
        assertTrue(result.decision().valid());
        assertEquals(identity, adoptionStore.request.certificationIdentity());
    }

    @Test
    void unknownCertificationDoesNotPersistAdoption() {
        CapturingAdoptionStore adoptionStore = new CapturingAdoptionStore();
        LiveUserAdoptionApplicationService service = service(
                new LookupCatalogStore(Optional.empty()), adoptionStore);

        LiveUserAdoptionResult result = service.validateAndPersist(command(CERT_ID));

        assertFalse(result.persisted());
        assertEquals("LIVE_CERTIFICATION_MISSING", result.reasonCode());
        assertEquals(0, adoptionStore.writes);
    }

    private LiveUserAdoptionApplicationService service(LiveCertificationCatalogStore catalog,
                                                       CapturingAdoptionStore store) {
        LiveUserAdoptionValidator validator = new LiveUserAdoptionValidator(Clock.fixed(
                Instant.parse("2026-07-13T12:00:00Z"), ZoneOffset.UTC));
        return new LiveUserAdoptionApplicationService(catalog,
                new LiveUserAdoptionPersistenceService(validator, store));
    }

    private LiveUserAdoptionCommand command(UUID certificationId) {
        return new LiveUserAdoptionCommand(
                certificationId, UUID.fromString("22222222-2222-2222-2222-222222222222"), 55L,
                new BigDecimal("300"), new BigDecimal("100"), new BigDecimal("5"), "USDC",
                "ISOLATED", "ISOLATED", true, true, true,
                NOW.minusSeconds(1), NOW.plusMinutes(10));
    }

    private static final class CapturingAdoptionStore implements LiveUserAdoptionStore {
        private int writes;
        private UserAdoptionValidationRequest request;
        @Override public void upsert(UserAdoptionValidationRequest request, UserAdoptionValidationDecision decision) {
            writes++;
            this.request = request;
        }
    }

    private record LookupCatalogStore(Optional<LiveCertificationIdentity> identity)
            implements LiveCertificationCatalogStore {
        @Override public Optional<LiveCertificationCatalogRecord> findByCreationKey(String key) { return Optional.empty(); }
        @Override public Optional<LiveCertificationCatalogRecord> findByIdentity(LiveCertificationIdentity value) { return Optional.empty(); }
        @Override public boolean insert(LiveCertificationCatalogRecord record, String key, Map<String, Object> evidence, String actor, String reason) { return false; }
        @Override public Optional<LiveCertificationIdentity> findIdentityById(UUID id) { return identity; }
    }
}
