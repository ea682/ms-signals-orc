package com.apunto.engine.service.copy.certification;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LiveUserAdoptionPersistenceServiceTest {

    private static final OffsetDateTime NOW = OffsetDateTime.parse("2026-07-13T12:00:00Z");

    @Test
    void rejectedUserIsPersistedWithoutChangingGlobalCertification() {
        CapturingStore store = new CapturingStore();
        LiveUserAdoptionPersistenceService service = new LiveUserAdoptionPersistenceService(
                new LiveUserAdoptionValidator(Clock.fixed(
                        Instant.parse("2026-07-13T12:00:00Z"), ZoneOffset.UTC)), store);
        LiveCertificationIdentity identity = TestCertificationFixtures.identity(
                new BigDecimal("100"), new BigDecimal("250"), new BigDecimal("5"));
        UserAdoptionValidationRequest request = new UserAdoptionValidationRequest(
                UUID.randomUUID(), UUID.randomUUID(), 55L, identity,
                new BigDecimal("20"), new BigDecimal("100"), new BigDecimal("5"), "USDC",
                "ISOLATED", "ISOLATED", true, true, true,
                NOW.minusSeconds(1), NOW.plusMinutes(10));

        UserAdoptionValidationDecision decision = service.validateAndPersist(request);

        assertFalse(decision.valid());
        assertTrue(decision.reasonCodes().contains("LIVE_ADOPTION_BALANCE_INSUFFICIENT"));
        assertEquals(1, store.writes);
        assertEquals("REJECTED", store.status);
    }

    private static final class CapturingStore implements LiveUserAdoptionStore {
        private int writes;
        private String status;

        @Override
        public void upsert(UserAdoptionValidationRequest request,
                           UserAdoptionValidationDecision decision) {
            writes++;
            status = decision.valid() ? "VALID" : "REJECTED";
        }
    }
}
