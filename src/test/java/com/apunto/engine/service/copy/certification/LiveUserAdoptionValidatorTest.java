package com.apunto.engine.service.copy.certification;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LiveUserAdoptionValidatorTest {

    private static final OffsetDateTime NOW = OffsetDateTime.parse("2026-07-13T12:00:00Z");
    private final LiveUserAdoptionValidator validator = new LiveUserAdoptionValidator(
            Clock.fixed(Instant.parse("2026-07-13T12:00:00Z"), ZoneOffset.UTC));

    @Test
    void acceptsOnlyACompleteObservationInsideTheExactBand() {
        UserAdoptionValidationDecision decision = validator.validate(validRequest());

        assertTrue(decision.valid());
        assertTrue(decision.reasonCodes().isEmpty());
        assertTrue(decision.capitalBandValid());
        assertTrue(decision.leverageValid());
    }

    @Test
    void rejectsInsufficientBalanceWrongLeverageAndExpiredObservation() {
        UserAdoptionValidationRequest valid = validRequest();
        UserAdoptionValidationRequest invalid = new UserAdoptionValidationRequest(
                valid.certificationId(), valid.userId(), valid.allocationId(), valid.certificationIdentity(),
                new BigDecimal("99"), new BigDecimal("100"), new BigDecimal("10"), "USDC",
                "CROSSED", "ISOLATED", true, true, true,
                NOW.minusMinutes(20), NOW.minusSeconds(1));

        UserAdoptionValidationDecision decision = validator.validate(invalid);

        assertFalse(decision.valid());
        assertTrue(decision.reasonCodes().contains("LIVE_ADOPTION_BALANCE_INSUFFICIENT"));
        assertTrue(decision.reasonCodes().contains("LIVE_ADOPTION_LEVERAGE_MISMATCH"));
        assertTrue(decision.reasonCodes().contains("LIVE_ADOPTION_MARGIN_MODE_MISMATCH"));
        assertTrue(decision.reasonCodes().contains("LIVE_ADOPTION_EXPIRED"));
    }

    @Test
    void futureObservationCannotBecomeValidThroughClockSkew() {
        UserAdoptionValidationRequest valid = validRequest();
        UserAdoptionValidationRequest future = new UserAdoptionValidationRequest(
                valid.certificationId(), valid.userId(), valid.allocationId(), valid.certificationIdentity(),
                valid.balanceUsd(), valid.assignedCapitalUsd(), valid.targetLeverage(), valid.quoteAsset(),
                valid.observedMarginMode(), valid.requiredMarginMode(), true, true, true,
                NOW.plusSeconds(1), NOW.plusMinutes(10));

        UserAdoptionValidationDecision decision = validator.validate(future);

        assertFalse(decision.valid());
        assertTrue(decision.reasonCodes().contains("LIVE_ADOPTION_OBSERVED_AT_IN_FUTURE"));
    }

    private UserAdoptionValidationRequest validRequest() {
        return new UserAdoptionValidationRequest(
                UUID.fromString("11111111-1111-1111-1111-111111111111"),
                UUID.fromString("22222222-2222-2222-2222-222222222222"),
                55L,
                TestCertificationFixtures.identity(new BigDecimal("100"), new BigDecimal("250"), new BigDecimal("5")),
                new BigDecimal("300"),
                new BigDecimal("100"),
                new BigDecimal("5"),
                "USDC",
                "ISOLATED",
                "ISOLATED",
                true,
                true,
                true,
                NOW.minusSeconds(1),
                NOW.plusMinutes(10));
    }
}
