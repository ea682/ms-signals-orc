package com.apunto.engine.service.copy.certification;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LiveEntryAuthorizationServiceTest {

    private static final OffsetDateTime NOW = OffsetDateTime.parse("2026-07-13T12:00:00Z");
    private static final UUID USER_ID = UUID.fromString("22222222-2222-2222-2222-222222222222");

    @Test
    void exactApprovedCertificationAndValidAdoptionAllowsEntry() {
        LiveEntryAuthorizationRequest request = request(new BigDecimal("250"), new BigDecimal("5"));
        LiveCertificationAuthorizationRecord record = validRecord(
                TestCertificationFixtures.identity(new BigDecimal("100"), new BigDecimal("250"), new BigDecimal("5")));
        LiveEntryAuthorizationService service = new LiveEntryAuthorizationService(ignored -> List.of(record));

        LiveEntryAuthorizationDecision decision = service.evaluate(request, NOW);

        assertTrue(decision.allowed());
        assertEquals("LIVE_CERTIFICATION_AND_ADOPTION_VALID", decision.reasonCode());
    }

    @Test
    void wrongBandOrModelVersionCannotAuthorizeEntry() {
        LiveEntryAuthorizationRequest request = request(new BigDecimal("251"), new BigDecimal("5"));
        LiveCertificationAuthorizationRecord record = validRecord(
                TestCertificationFixtures.identity(new BigDecimal("100"), new BigDecimal("250"), new BigDecimal("5")));
        LiveEntryAuthorizationService service = new LiveEntryAuthorizationService(ignored -> List.of(record));

        LiveEntryAuthorizationDecision decision = service.evaluate(request, NOW);

        assertFalse(decision.allowed());
        assertEquals("LIVE_CERTIFICATION_IDENTITY_MISMATCH", decision.reasonCode());
    }

    @Test
    void expiredAdoptionBlocksWithoutMutatingGlobalCertification() {
        LiveEntryAuthorizationRequest request = request(new BigDecimal("100"), new BigDecimal("5"));
        LiveCertificationAuthorizationRecord valid = validRecord(
                TestCertificationFixtures.identity(new BigDecimal("100"), new BigDecimal("250"), new BigDecimal("5")));
        LiveCertificationAuthorizationRecord expired = valid.withAdoptionWindow(
                NOW.minusMinutes(20), NOW.minusSeconds(1));
        LiveEntryAuthorizationService service = new LiveEntryAuthorizationService(ignored -> List.of(expired));

        LiveEntryAuthorizationDecision decision = service.evaluate(request, NOW);

        assertFalse(decision.allowed());
        assertEquals("LIVE_ADOPTION_EXPIRED", decision.reasonCode());
        assertEquals(LiveCertificationStatus.LIVE_APPROVED, expired.certificationStatus());
    }

    @Test
    void ambiguousOverlappingCertificationsFailClosed() {
        LiveEntryAuthorizationRequest request = request(new BigDecimal("150"), new BigDecimal("5"));
        LiveCertificationAuthorizationRecord first = validRecord(
                TestCertificationFixtures.identity(new BigDecimal("100"), new BigDecimal("250"), new BigDecimal("5")));
        LiveCertificationAuthorizationRecord second = validRecord(
                TestCertificationFixtures.identity(new BigDecimal("100"), new BigDecimal("1000"), new BigDecimal("5")));
        LiveEntryAuthorizationService service = new LiveEntryAuthorizationService(ignored -> List.of(first, second));

        LiveEntryAuthorizationDecision decision = service.evaluate(request, NOW);

        assertFalse(decision.allowed());
        assertEquals("LIVE_CERTIFICATION_AMBIGUOUS", decision.reasonCode());
    }

    private LiveEntryAuthorizationRequest request(BigDecimal capital, BigDecimal leverage) {
        return TestCertificationFixtures.request(USER_ID, 55L, capital, leverage);
    }

    private LiveCertificationAuthorizationRecord validRecord(LiveCertificationIdentity identity) {
        return new LiveCertificationAuthorizationRecord(
                UUID.randomUUID(), identity, LiveCertificationStatus.LIVE_APPROVED,
                UUID.randomUUID(), "VALID", true, true, true, true, true, true, true, true,
                NOW.minusMinutes(1), NOW.plusMinutes(10));
    }
}
