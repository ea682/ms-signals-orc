package com.apunto.copytarget;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TargetPortfolioCalculatorTest {

    private static final Instant NOW = Instant.parse("2026-07-13T12:00:00Z");

    private final TargetPortfolioCalculator calculator = new TargetPortfolioCalculator();

    @Test
    void usesSourceNotionalOverEquityAndTargetCapital() {
        TargetPortfolioResult result = calculator.calculate(request(
                bd("500000"),
                List.of(position("HYPE", "HYPEUSDT", "100000", "100", SourceSide.LONG)),
                bd("100"),
                bd("5"),
                null
        ));

        TargetLegDecision leg = result.selectedLegs().getFirst();
        assertEquals(0, bd("0.2").compareTo(leg.sourceExposureRatio()));
        assertEquals(0, bd("20").compareTo(leg.targetNotionalUsd()));
        assertEquals(0, bd("4").compareTo(leg.targetMarginUsd()));
    }

    @Test
    void zeroOrNegativeTargetCapitalBlocksOnlyNewExposure() {
        List<SourcePosition> source = List.of(
                position("A", "AUSDT", "100", "10", SourceSide.LONG));

        TargetPortfolioResult zero = calculator.calculate(request(
                bd("1000"), source, BigDecimal.ZERO, bd("5"), null));

        assertEquals(DecisionCode.BLOCKED_INSUFFICIENT_MARGIN, zero.portfolioDecisionCode());
        assertTrue(zero.selectedLegs().isEmpty());
        assertEquals(DecisionCode.SKIPPED_CAPITAL_EXHAUSTED,
                zero.omittedLegs().getFirst().decisionCode());
        assertThrows(IllegalArgumentException.class,
                () -> request(bd("1000"), source, bd("-1"), bd("5"), null));
    }

    @Test
    void sourceExposureAboveOneHundredPercentUsesOneCommonScaleFactor() {
        TargetPortfolioResult result = calculator.calculate(request(
                bd("100"),
                List.of(position("A", "AUSDT", "600", "10", SourceSide.LONG)),
                bd("100"), bd("5"), null));

        TargetLegDecision leg = result.selectedLegs().getFirst();
        assertEquals(0, bd("6").compareTo(leg.sourceExposureRatio()));
        assertTrue(result.portfolioScaleFactor().compareTo(BigDecimal.ONE) < 0);
        assertTrue(result.totalTargetMarginUsd().compareTo(bd("100")) <= 0);
    }

    @Test
    void sourceLeverageDoesNotReplaceNotionalOverEquityExposure() {
        SourcePosition base = position("A", "AUSDT", "200", "10", SourceSide.LONG);
        SourcePosition lowLeverage = new SourcePosition(
                base.sourceLegId(), base.sourceSymbol(), base.targetSymbol(), base.side(), base.quantity(),
                base.notionalUsd(), base.markPrice(), base.entryPrice(), bd("2"),
                base.snapshotVersion(), base.liquidityScore());
        SourcePosition highLeverage = new SourcePosition(
                base.sourceLegId(), base.sourceSymbol(), base.targetSymbol(), base.side(), base.quantity(),
                base.notionalUsd(), base.markPrice(), base.entryPrice(), bd("50"),
                base.snapshotVersion(), base.liquidityScore());

        TargetPortfolioResult low = calculator.calculate(request(
                bd("1000"), List.of(lowLeverage), bd("100"), bd("5"), null));
        TargetPortfolioResult high = calculator.calculate(request(
                bd("1000"), List.of(highLeverage), bd("100"), bd("5"), null));

        assertEquals(low.selectedLegs(), high.selectedLegs());
    }

    @Test
    void doesNotAssignFixedTwentyDollarsPerPosition() {
        TargetPortfolioResult result = calculator.calculate(request(
                bd("1000"),
                List.of(
                        position("A", "AUSDT", "100", "10", SourceSide.LONG),
                        position("B", "BUSDT", "500", "10", SourceSide.LONG)
                ),
                bd("100"),
                bd("5"),
                null
        ));

        assertEquals(0, bd("10").compareTo(result.leg("AUSDT", SourceSide.LONG).targetNotionalUsd()));
        assertEquals(0, bd("50").compareTo(result.leg("BUSDT", SourceSide.LONG).targetNotionalUsd()));
    }

    @Test
    void hasNoImplicitFivePositionLimit() {
        List<SourcePosition> positions = new ArrayList<>();
        for (int index = 0; index < 8; index++) {
            positions.add(position("L" + index, "S" + index + "USDT", "100", "10", SourceSide.LONG));
        }

        TargetPortfolioResult result = calculator.calculate(request(
                bd("1000"), positions, bd("1000"), bd("5"), null));

        assertEquals(8, result.selectedLegs().size());
        assertTrue(result.omittedLegs().isEmpty());
    }

    @Test
    void optionalPositionLimitIsDeterministicAndExplicit() {
        List<SourcePosition> positions = List.of(
                position("C", "CUSDT", "100", "10", SourceSide.LONG),
                position("A", "AUSDT", "300", "10", SourceSide.LONG),
                position("B", "BUSDT", "200", "10", SourceSide.LONG)
        );

        TargetPortfolioResult result = calculator.calculate(request(
                bd("1000"), positions, bd("1000"), bd("5"), 2));

        assertEquals(List.of("AUSDT", "BUSDT"), result.selectedLegs().stream()
                .map(TargetLegDecision::targetSymbol).toList());
        assertEquals(DecisionCode.SKIPPED_USER_POSITION_LIMIT, result.omittedLegs().getFirst().decisionCode());
    }

    @Test
    void positionLimitDoesNotSpendASlotOnAnIneligibleHigherExposureCandidate() {
        List<SourcePosition> positions = List.of(
                position("A", "AUSDT", "60", "10", SourceSide.LONG),
                position("B", "BUSDT", "40", "10", SourceSide.LONG));
        List<BinanceSymbolFilter> filters = List.of(
                new BinanceSymbolFilter("AUSDT", true, "USDT", bd("0.001"), bd("1000"), bd("0.001"),
                        bd("100"), bd("0.01"), bd("20"), bd("100")),
                new BinanceSymbolFilter("BUSDT", true, "USDT", bd("0.001"), bd("1000"), bd("0.001"),
                        bd("1"), bd("0.01"), bd("20"), bd("100")));
        TargetPortfolioRequest input = requestBuilder(
                bd("100"), positions, bd("100"), bd("1"), 1).filters(filters).build();

        TargetPortfolioResult result = calculator.calculate(input);

        assertEquals(List.of("BUSDT"), result.selectedLegs().stream()
                .map(TargetLegDecision::targetSymbol).toList());
        assertTrue(result.omittedLegs().stream().anyMatch(leg -> leg.targetSymbol().equals("AUSDT")
                && leg.decisionCode() == DecisionCode.SKIPPED_BELOW_MIN_NOTIONAL));
    }

    @Test
    void positionLimitNeverClosesAnIncumbentToMakeRoomForANewEntry() {
        ExistingTargetPosition incumbent = new ExistingTargetPosition(
                "AUSDT", SourceSide.LONG, bd("1"), bd("10"), bd("2"));
        TargetPortfolioRequest input = requestBuilder(
                bd("1000"),
                List.of(
                        position("B", "BUSDT", "500", "10", SourceSide.LONG),
                        position("A", "AUSDT", "100", "10", SourceSide.LONG)),
                bd("100"), bd("5"), 1)
                .existingPositions(List.of(incumbent))
                .managedExistingPositions(List.of(incumbent))
                .portfolioExistingPositions(List.of(incumbent))
                .build();

        TargetPortfolioResult result = calculator.calculate(input);

        assertTrue(result.selectedLegs().stream().anyMatch(leg -> leg.targetSymbol().equals("AUSDT")));
        assertFalse(result.selectedLegs().stream().anyMatch(leg -> leg.targetSymbol().equals("BUSDT")));
        assertFalse(result.selectedLegs().stream().anyMatch(leg -> leg.targetSymbol().equals("AUSDT")
                && leg.deltaAction() == DeltaAction.CLOSE));
        assertTrue(result.omittedLegs().stream().anyMatch(leg -> leg.targetSymbol().equals("BUSDT")
                && leg.decisionCode() == DecisionCode.SKIPPED_USER_POSITION_LIMIT));
    }

    @Test
    void positionLimitAccountsForOpenPositionsFromOtherAllocations() {
        ExistingTargetPosition externalAllocation = new ExistingTargetPosition(
                "XUSDT", SourceSide.LONG, bd("1"), bd("10"), bd("2"));
        TargetPortfolioRequest input = requestBuilder(
                bd("1000"),
                List.of(
                        position("A", "AUSDT", "300", "10", SourceSide.LONG),
                        position("B", "BUSDT", "200", "10", SourceSide.LONG)),
                bd("1000"), bd("5"), 2)
                .existingPositions(List.of(externalAllocation))
                .managedExistingPositions(List.of(externalAllocation))
                .portfolioExistingPositions(List.of())
                .build();

        TargetPortfolioResult result = calculator.calculate(input);

        assertEquals(List.of("AUSDT"), result.selectedLegs().stream()
                .map(TargetLegDecision::targetSymbol).toList());
        assertTrue(result.omittedLegs().stream().anyMatch(leg -> leg.targetSymbol().equals("BUSDT")
                && leg.decisionCode() == DecisionCode.SKIPPED_USER_POSITION_LIMIT));
    }

    @Test
    void lowerRankedCandidateCannotDiluteAHigherRankedCandidateBelowMinimum() {
        List<SourcePosition> positions = List.of(
                position("A", "AUSDT", "60", "10", SourceSide.LONG),
                position("B", "BUSDT", "40", "10", SourceSide.LONG));
        List<BinanceSymbolFilter> filters = List.of(
                new BinanceSymbolFilter("AUSDT", true, "USDT", bd("0.001"), bd("1000"), bd("0.001"),
                        bd("50"), bd("0.01"), bd("20"), bd("100")),
                new BinanceSymbolFilter("BUSDT", true, "USDT", bd("0.001"), bd("1000"), bd("0.001"),
                        bd("1"), bd("0.01"), bd("20"), bd("100")));
        TargetPortfolioRequest input = requestBuilder(
                bd("100"), positions, bd("100"), bd("1"), 2)
                .availableMarginUsd(bd("60"))
                .filters(filters)
                .build();

        TargetPortfolioResult result = calculator.calculate(input);

        assertEquals(List.of("AUSDT"), result.selectedLegs().stream()
                .map(TargetLegDecision::targetSymbol).toList());
        assertTrue(result.omittedLegs().stream().anyMatch(leg -> leg.targetSymbol().equals("BUSDT")
                && leg.decisionCode() == DecisionCode.SKIPPED_NOT_SELECTED_BY_USER_POLICY));
    }

    @Test
    void sourceAliasCollisionFailsClosedWithoutClosingTheExistingTarget() {
        ExistingTargetPosition existing = new ExistingTargetPosition(
                "XUSDT", SourceSide.LONG, bd("1"), bd("10"), bd("2"));
        List<SourcePosition> source = List.of(
                position("SOURCE_A", "XUSDT", "300", "10", SourceSide.LONG),
                position("SOURCE_B", "XUSDT", "200", "10", SourceSide.LONG));
        TargetPortfolioRequest input = requestBuilder(
                bd("1000"), source, bd("100"), bd("5"), null)
                .existingPositions(List.of(existing))
                .managedExistingPositions(List.of(existing))
                .portfolioExistingPositions(List.of(existing))
                .build();

        TargetPortfolioResult result = calculator.calculate(input);

        assertEquals(DecisionCode.BLOCKED_TARGET_SYMBOL_COLLISION, result.portfolioDecisionCode());
        assertFalse(result.entrySizingAllowed());
        assertEquals(2, result.omittedLegs().stream()
                .filter(leg -> leg.decisionCode() == DecisionCode.BLOCKED_TARGET_SYMBOL_COLLISION)
                .count());
        assertFalse(result.selectedLegs().stream().anyMatch(leg -> leg.targetSymbol().equals("XUSDT")
                && (leg.deltaAction() == DeltaAction.CLOSE || leg.deltaAction() == DeltaAction.FLIP_CLOSE)));
    }

    @Test
    void appliesOneCommonPortfolioScaleFactor() {
        TargetPortfolioResult result = calculator.calculate(request(
                bd("100"),
                List.of(
                        position("A", "AUSDT", "100", "10", SourceSide.LONG),
                        position("B", "BUSDT", "100", "10", SourceSide.LONG)
                ),
                bd("100"),
                bd("1"),
                null,
                bd("60"),
                bd("0"),
                bd("0")
        ));

        assertEquals(0, bd("0.3").compareTo(result.portfolioScaleFactor()));
        assertEquals(0, bd("30").compareTo(result.leg("AUSDT", SourceSide.LONG).targetNotionalUsd()));
        assertEquals(0, bd("30").compareTo(result.leg("BUSDT", SourceSide.LONG).targetNotionalUsd()));
    }

    @Test
    void resultDoesNotDependOnSourceCollectionOrder() {
        List<SourcePosition> original = List.of(
                position("A", "AUSDT", "300", "7", SourceSide.LONG),
                position("B", "BUSDT", "200", "13", SourceSide.SHORT),
                position("C", "CUSDT", "100", "17", SourceSide.LONG)
        );
        List<SourcePosition> reversed = new ArrayList<>(original);
        Collections.reverse(reversed);

        TargetPortfolioResult first = calculator.calculate(request(bd("1000"), original, bd("500"), bd("5"), null));
        TargetPortfolioResult second = calculator.calculate(request(bd("1000"), reversed, bd("500"), bd("5"), null));

        assertEquals(first, second);
    }

    @Test
    void roundsDownAndNeverRaisesQuantityToMeetMinNotional() {
        BinanceSymbolFilter filter = new BinanceSymbolFilter(
                "AUSDT", true, "USDT", bd("0.1"), bd("100000"), bd("0.1"),
                bd("5"), bd("0.01"), bd("20"), bd("100")
        );
        TargetPortfolioRequest request = requestBuilder(
                bd("1000"),
                List.of(position("A", "AUSDT", "49", "10", SourceSide.LONG)),
                bd("100"), bd("5"), null
        ).filters(List.of(filter)).build();

        TargetPortfolioResult result = calculator.calculate(request);

        assertTrue(result.selectedLegs().isEmpty());
        assertEquals(DecisionCode.SKIPPED_BELOW_MIN_NOTIONAL, result.omittedLegs().getFirst().decisionCode());
        assertEquals(0, bd("0.49").compareTo(result.omittedLegs().getFirst().rawQuantity()));
        assertEquals(0, bd("0.4").compareTo(result.omittedLegs().getFirst().roundedQuantity()));
    }

    @Test
    void rejectsRoundedQuantityAboveTheExchangeMaximum() {
        BinanceSymbolFilter filter = new BinanceSymbolFilter(
                "AUSDT", true, "USDT", bd("0.1"), bd("2"), bd("0.1"),
                bd("5"), bd("0.01"), bd("20"), bd("100")
        );
        TargetPortfolioRequest request = requestBuilder(
                bd("1000"),
                List.of(position("A", "AUSDT", "100", "10", SourceSide.LONG)),
                bd("1000"), bd("5"), null
        ).filters(List.of(filter)).build();

        TargetPortfolioResult result = calculator.calculate(request);

        assertTrue(result.selectedLegs().isEmpty());
        assertEquals(DecisionCode.REJECTED_BY_BINANCE_FILTER,
                result.omittedLegs().getFirst().decisionCode());
    }

    @Test
    void zeroSourceNotionalIsAlreadyAtTargetWhenNoTargetPositionExists() {
        TargetPortfolioResult result = calculator.calculate(request(
                bd("1000"),
                List.of(position("A", "AUSDT", "0", "10", SourceSide.LONG)),
                bd("100"), bd("5"), null
        ));

        assertTrue(result.selectedLegs().isEmpty());
        assertEquals(DecisionCode.SKIPPED_ALREADY_AT_TARGET,
                result.omittedLegs().getFirst().decisionCode());
    }

    @Test
    void missingOrStaleEquityBlocksExposureIncrease() {
        TargetPortfolioRequest missing = requestBuilder(
                null,
                List.of(position("A", "AUSDT", "100", "10", SourceSide.LONG)),
                bd("100"), bd("5"), null
        ).build();
        TargetPortfolioRequest stale = requestBuilder(
                bd("1000"),
                List.of(position("A", "AUSDT", "100", "10", SourceSide.LONG)),
                bd("100"), bd("5"), null
        ).equityObservedAt(NOW.minus(Duration.ofMinutes(10))).maximumEquityAge(Duration.ofSeconds(30)).build();

        assertEquals(DecisionCode.BLOCKED_SOURCE_EQUITY_MISSING,
                calculator.calculate(missing).portfolioDecisionCode());
        assertEquals(DecisionCode.BLOCKED_SOURCE_EQUITY_STALE,
                calculator.calculate(stale).portfolioDecisionCode());
    }

    @Test
    void invalidEquityDoesNotTurnAStillOpenSourceLegIntoAnInferredClose() {
        ExistingTargetPosition existing = new ExistingTargetPosition(
                "AUSDT", SourceSide.LONG, bd("2"), bd("10"), bd("4"));
        TargetPortfolioRequest request = requestBuilder(
                null,
                List.of(position("A", "AUSDT", "100", "10", SourceSide.LONG)),
                bd("100"), bd("5"), null
        ).existingPositions(List.of(existing)).build();

        TargetPortfolioResult result = calculator.calculate(request);

        assertEquals(DecisionCode.BLOCKED_SOURCE_EQUITY_MISSING, result.portfolioDecisionCode());
        assertTrue(result.selectedLegs().isEmpty());
        assertEquals(1, result.omittedLegs().size());
        assertEquals(DecisionCode.BLOCKED_SOURCE_EQUITY_MISSING,
                result.omittedLegs().getFirst().decisionCode());
    }

    @Test
    void futureEquityObservationFailsClosed() {
        TargetPortfolioRequest request = requestBuilder(
                bd("1000"),
                List.of(position("A", "AUSDT", "100", "10", SourceSide.LONG)),
                bd("100"), bd("5"), null
        ).equityObservedAt(NOW.plusSeconds(1)).build();

        assertEquals(DecisionCode.BLOCKED_SOURCE_EQUITY_INVALID,
                calculator.calculate(request).portfolioDecisionCode());
    }

    @Test
    void closeRemainsAllowedWithoutEquity() {
        TargetPortfolioRequest request = requestBuilder(
                null,
                List.of(),
                bd("100"), bd("5"), null
        ).existingPositions(List.of(new ExistingTargetPosition(
                "AUSDT", SourceSide.LONG, bd("2"), bd("10"), bd("4")
        ))).build();

        TargetPortfolioResult result = calculator.calculate(request);

        TargetLegDecision close = result.selectedLegs().getFirst();
        assertEquals(DeltaAction.CLOSE, close.deltaAction());
        assertEquals(0, BigDecimal.ZERO.compareTo(close.targetQuantity()));
        assertEquals(0, bd("2").compareTo(close.deltaQuantity().abs()));
        assertFalse(result.entrySizingAllowed());
    }

    @Test
    void roundedQuantityNeverExceedsRawQuantityAcrossGeneratedInputs() {
        for (int index = 1; index <= 500; index++) {
            int caseIndex = index;
            BigDecimal sourceNotional = BigDecimal.valueOf(index * 13L).movePointLeft(2);
            BigDecimal price = BigDecimal.valueOf((index % 97) + 1L).movePointRight(1);
            TargetPortfolioRequest request = request(
                    bd("1000"),
                    List.of(position("A" + index, "AUSDT", sourceNotional.toPlainString(), price.toPlainString(), SourceSide.LONG)),
                    bd("1000"), bd("5"), null
            );

            TargetLegDecision leg = calculator.calculate(request).allLegs().getFirst();
            assertTrue(leg.roundedQuantity().compareTo(leg.rawQuantity()) <= 0,
                    () -> "rounded quantity exceeded raw quantity for case " + caseIndex);
        }
    }

    @Test
    void manualBinanceExposureBlocksEntryWithoutAssigningItToThePortfolio() {
        ExistingTargetPosition manual = new ExistingTargetPosition(
                "AUSDT", SourceSide.LONG, bd("1"), bd("10"), bd("2"));
        TargetPortfolioRequest request = requestBuilder(
                bd("1000"),
                List.of(position("A", "AUSDT", "200", "10", SourceSide.LONG)),
                bd("100"), bd("5"), null
        ).existingPositions(List.of(manual))
                .managedExistingPositions(List.of())
                .portfolioExistingPositions(List.of())
                .build();

        TargetPortfolioResult result = calculator.calculate(request);

        assertFalse(result.entrySizingAllowed());
        assertEquals(DecisionCode.BLOCKED_EXISTING_EXPOSURE_CONFLICT,
                result.portfolioDecisionCode());
        TargetLegDecision desired = result.selectedLegs().getFirst();
        assertEquals(DeltaAction.OPEN, desired.deltaAction());
        assertEquals(0, BigDecimal.ZERO.compareTo(desired.existingQuantity()));
        assertEquals(0, bd("2").compareTo(desired.targetQuantity()));
    }

    @Test
    void sharedSymbolUsesAllocationAttributionWhileValidatingTheAccountAggregate() {
        ExistingTargetPosition account = new ExistingTargetPosition(
                "AUSDT", SourceSide.LONG, bd("3"), bd("10"), bd("6"));
        ExistingTargetPosition allocation = new ExistingTargetPosition(
                "AUSDT", SourceSide.LONG, bd("1"), bd("10"), bd("2"));
        TargetPortfolioRequest request = requestBuilder(
                bd("1000"),
                List.of(position("A", "AUSDT", "200", "10", SourceSide.LONG)),
                bd("100"), bd("5"), null
        ).existingPositions(List.of(account))
                .managedExistingPositions(List.of(account))
                .portfolioExistingPositions(List.of(allocation))
                .build();

        TargetPortfolioResult result = calculator.calculate(request);

        assertTrue(result.entrySizingAllowed());
        TargetLegDecision desired = result.selectedLegs().getFirst();
        assertEquals(DeltaAction.INCREASE, desired.deltaAction());
        assertEquals(0, bd("1").compareTo(desired.existingQuantity()));
        assertEquals(0, bd("1").compareTo(desired.deltaQuantity()));
    }

    @Test
    void externalCloseBlocksFurtherExposureUntilManagedStateIsReconciled() {
        ExistingTargetPosition managed = new ExistingTargetPosition(
                "AUSDT", SourceSide.LONG, bd("1"), bd("10"), bd("2"));
        TargetPortfolioRequest request = requestBuilder(
                bd("1000"),
                List.of(position("A", "AUSDT", "200", "10", SourceSide.LONG)),
                bd("100"), bd("5"), null
        ).existingPositions(List.of())
                .managedExistingPositions(List.of(managed))
                .portfolioExistingPositions(List.of(managed))
                .build();

        TargetPortfolioResult result = calculator.calculate(request);

        assertFalse(result.entrySizingAllowed());
        assertEquals(DecisionCode.BLOCKED_EXISTING_EXPOSURE_CONFLICT,
                result.portfolioDecisionCode());
    }

    @Test
    void exposureConflictAndUnavailableSnapshotNeverBlockAnAttributedClose() {
        ExistingTargetPosition actual = new ExistingTargetPosition(
                "AUSDT", SourceSide.LONG, bd("2"), bd("10"), bd("4"));
        ExistingTargetPosition managed = new ExistingTargetPosition(
                "AUSDT", SourceSide.LONG, bd("1"), bd("10"), bd("2"));
        TargetPortfolioRequest conflict = requestBuilder(
                bd("1000"), List.of(), bd("100"), bd("5"), null
        ).existingPositions(List.of(actual))
                .managedExistingPositions(List.of(managed))
                .portfolioExistingPositions(List.of(managed))
                .build();
        TargetPortfolioRequest unavailable = requestBuilder(
                bd("1000"), List.of(), bd("100"), bd("5"), null
        ).existingPositions(List.of(managed))
                .managedExistingPositions(List.of(managed))
                .portfolioExistingPositions(List.of(managed))
                .targetPositionSnapshotStatus(TargetPositionSnapshotStatus.UNAVAILABLE)
                .build();

        for (TargetPortfolioRequest request : List.of(conflict, unavailable)) {
            TargetPortfolioResult result = calculator.calculate(request);
            assertFalse(result.entrySizingAllowed());
            TargetLegDecision close = result.selectedLegs().getFirst();
            assertEquals(DeltaAction.CLOSE, close.deltaAction());
            assertEquals(0, bd("1").compareTo(close.deltaQuantity().abs()));
        }
        assertEquals(DecisionCode.BLOCKED_EXISTING_EXPOSURE_CONFLICT,
                calculator.calculate(conflict).portfolioDecisionCode());
        assertEquals(DecisionCode.BLOCKED_TARGET_POSITION_SNAPSHOT_UNAVAILABLE,
                calculator.calculate(unavailable).portfolioDecisionCode());
    }

    @Test
    void oppositeAttributedSideProducesFlipCloseAndNeverAnUnsequencedOpen() {
        ExistingTargetPosition opposite = new ExistingTargetPosition(
                "AUSDT", SourceSide.SHORT, bd("1"), bd("10"), bd("2"));
        TargetPortfolioRequest request = requestBuilder(
                bd("1000"),
                List.of(position("A", "AUSDT", "200", "10", SourceSide.LONG)),
                bd("100"), bd("5"), null
        ).existingPositions(List.of(opposite))
                .managedExistingPositions(List.of(opposite))
                .portfolioExistingPositions(List.of(opposite))
                .build();

        TargetPortfolioResult result = calculator.calculate(request);

        TargetLegDecision desired = result.selectedLegs().stream()
                .filter(decision -> decision.sourceLegId().equals("A"))
                .findFirst().orElseThrow();
        TargetLegDecision close = result.selectedLegs().stream()
                .filter(decision -> decision.deltaAction() == DeltaAction.FLIP_CLOSE)
                .findFirst().orElseThrow();
        assertTrue(desired.waitsForOppositeClose());
        assertEquals(DeltaAction.OPEN, desired.deltaAction());
        assertEquals(SourceSide.SHORT, close.side());
        assertEquals(0, bd("1").compareTo(close.deltaQuantity().abs()));
    }

    private TargetPortfolioRequest request(BigDecimal equity,
                                           List<SourcePosition> positions,
                                           BigDecimal capital,
                                           BigDecimal leverage,
                                           Integer maxPositions) {
        return requestBuilder(equity, positions, capital, leverage, maxPositions).build();
    }

    private TargetPortfolioRequest request(BigDecimal equity,
                                           List<SourcePosition> positions,
                                           BigDecimal capital,
                                           BigDecimal leverage,
                                           Integer maxPositions,
                                           BigDecimal availableMargin,
                                           BigDecimal usedMargin,
                                           BigDecimal reservedMargin) {
        return requestBuilder(equity, positions, capital, leverage, maxPositions)
                .availableMarginUsd(availableMargin)
                .usedMarginUsd(usedMargin)
                .reservedMarginUsd(reservedMargin)
                .build();
    }

    private TargetPortfolioRequest.Builder requestBuilder(BigDecimal equity,
                                                          List<SourcePosition> positions,
                                                          BigDecimal capital,
                                                          BigDecimal leverage,
                                                          Integer maxPositions) {
        List<BinanceSymbolFilter> filters = positions.stream()
                .map(SourcePosition::targetSymbol)
                .distinct()
                .map(symbol -> new BinanceSymbolFilter(
                        symbol, true, "USDT", bd("0.001"), bd("1000000"), bd("0.001"),
                        bd("0.001"), bd("0.01"), bd("20"), bd("100")
                )).toList();
        return TargetPortfolioRequest.builder()
                .calculatedAt(NOW)
                .sourceAccountEquityUsd(equity)
                .equityObservedAt(NOW.minusSeconds(2))
                .equitySource("HYPERLIQUID_CLEARINGHOUSE_MARGIN_SUMMARY")
                .maximumEquityAge(Duration.ofSeconds(30))
                .sourceSnapshotVersion(42L)
                .sourcePositions(positions)
                .targetAllocatedCapitalUsd(capital)
                .targetLeverage(leverage)
                .availableMarginUsd(capital)
                .usedMarginUsd(BigDecimal.ZERO)
                .reservedMarginUsd(BigDecimal.ZERO)
                .existingPositions(List.of())
                .filters(filters)
                .quoteAsset("USDT")
                .userMaxConcurrentPositions(maxPositions)
                .versions(new CalculationVersions("strategy-v3", "sizing-v3", "symbols-v3"));
    }

    private SourcePosition position(String id, String targetSymbol, String notional, String markPrice, SourceSide side) {
        BigDecimal price = bd(markPrice);
        return new SourcePosition(
                id,
                id,
                targetSymbol,
                side,
                bd(notional).divide(price, 18, RoundingMode.DOWN),
                bd(notional),
                price,
                price,
                bd("10"),
                42L,
                bd("100")
        );
    }

    private static BigDecimal bd(String value) {
        return new BigDecimal(value);
    }
}
