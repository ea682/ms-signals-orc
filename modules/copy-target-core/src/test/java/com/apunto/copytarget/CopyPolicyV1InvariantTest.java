package com.apunto.copytarget;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CopyPolicyV1InvariantTest {

    private static final Instant NOW = Instant.parse("2026-08-28T12:00:00Z");

    @Test
    void legacySourcePositionDoesNotInferUncertifiedMargin() {
        SourcePosition source = legacyPosition("A", "AUSDT", "100", "10");

        assertEquals(MarginProvenance.UNAVAILABLE, source.marginProvenance());
        assertNull(source.marginUsedUsd());
    }

    @Test
    void unavailableMarginBlocksEntryWithoutInferringAClose() {
        SourcePosition source = legacyPosition("A", "AUSDT", "100", "10");
        ExistingTargetPosition existing = new ExistingTargetPosition(
                "AUSDT", SourceSide.LONG, bd("2"), bd("10"), bd("4"));

        TargetPortfolioResult result = new TargetPortfolioCalculator().calculate(
                request(source, existing));

        assertEquals(DecisionCode.BLOCKED_SOURCE_MARGIN_UNAVAILABLE,
                result.portfolioDecisionCode());
        assertFalse(result.entrySizingAllowed());
        assertTrue(result.selectedLegs().isEmpty());
        assertEquals(DecisionCode.BLOCKED_SOURCE_MARGIN_UNAVAILABLE,
                result.omittedLegs().getFirst().decisionCode());
    }

    @Test
    void explicitAndCertifiedDerivedMarginAreDistinguishable() {
        SourcePosition explicit = new SourcePosition(
                "A", "A", "AUSDT", SourceSide.LONG,
                bd("10"), bd("100"), bd("20"), bd("10"), bd("10"), bd("5"),
                42L, bd("100"));
        SourcePosition derived = SourcePosition.derivedCertified(
                "B", "B", "BUSDT", SourceSide.SHORT,
                bd("10"), bd("100"), bd("10"), bd("10"), bd("5"),
                42L, bd("100"));

        assertEquals(MarginProvenance.EXPLICIT, explicit.marginProvenance());
        assertEquals(MarginProvenance.DERIVED_CERTIFIED, derived.marginProvenance());
        assertEquals(0, bd("20").compareTo(derived.marginUsedUsd()));
    }

    @Test
    void authorizedCapitalReservesTenPercentThenDeploysNinetyPercentOfRemainder() {
        assertEquals(0, bd("81").compareTo(CopyPolicyV1Capital.authorizedSizingCapital(bd("100"))));
        assertEquals(0, bd("81").compareTo(CopyPolicyV1Capital.authorizedSizingCapital(bd("120"))));
        assertEquals(0, bd("40.5").compareTo(CopyPolicyV1Capital.authorizedSizingCapital(bd("50"))));
    }

    private TargetPortfolioRequest request(SourcePosition source, ExistingTargetPosition existing) {
        return TargetPortfolioRequest.builder()
                .calculatedAt(NOW)
                .sourceAccountEquityUsd(bd("1000"))
                .equityObservedAt(NOW.minusSeconds(2))
                .equitySource("CERTIFIED_SOURCE_ACCOUNT")
                .maximumEquityAge(Duration.ofSeconds(30))
                .sourceSnapshotVersion(42L)
                .sourcePositions(List.of(source))
                .targetAllocatedCapitalUsd(bd("81"))
                .targetLeverage(bd("5"))
                .availableMarginUsd(bd("81"))
                .usedMarginUsd(BigDecimal.ZERO)
                .reservedMarginUsd(BigDecimal.ZERO)
                .existingPositions(List.of(existing))
                .managedExistingPositions(List.of(existing))
                .portfolioExistingPositions(List.of(existing))
                .filters(List.of(new BinanceSymbolFilter(
                        "AUSDT", true, "USDT", bd("0.001"), bd("1000000"), bd("0.001"),
                        bd("0.001"), bd("0.01"), bd("20"), bd("100"))))
                .quoteAsset("USDT")
                .versions(new CalculationVersions("strategy-v1", "copy-policy-v1", "symbols-v1"))
                .build();
    }

    private SourcePosition legacyPosition(String id, String symbol, String notional, String markPrice) {
        BigDecimal price = bd(markPrice);
        return new SourcePosition(
                id, id, symbol, SourceSide.LONG,
                bd(notional).divide(price), bd(notional), price, price, bd("5"),
                42L, bd("100"));
    }

    private static BigDecimal bd(String value) {
        return new BigDecimal(value);
    }
}
