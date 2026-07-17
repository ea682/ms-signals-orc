package com.apunto.copytarget;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LiquiditySimulationEngineTest {

    @Test
    void producesAllFourExecutionStrategiesFromTheSameRealBookSnapshot() {
        List<LiquiditySimulationResult> results = new LiquiditySimulationEngine()
                .simulateAll(request(new BigDecimal("1500"), assumptions(BigDecimal.ZERO, null)));

        assertEquals(List.of(
                        LiquidityExecutionStrategy.SINGLE_MARKET,
                        LiquidityExecutionStrategy.FRAGMENTED,
                        LiquidityExecutionStrategy.TWAP,
                        LiquidityExecutionStrategy.PARTICIPATION_CAP),
                results.stream().map(LiquiditySimulationResult::executionStrategy).toList());
        assertTrue(results.stream().allMatch(result -> result.vwap() != null));
        assertTrue(results.stream().allMatch(result -> result.expectedSlippageBps().signum() >= 0));
        assertTrue(results.stream().allMatch(result -> result.evidenceLevel() == LiquidityEvidenceLevel.SIMULATED));
        assertTrue(results.stream().noneMatch(LiquiditySimulationResult::realValidated));
    }

    @Test
    void oneMillionCapitalCannotBeRealValidatedWhenVisibleDepthIsInsufficient() {
        LiquiditySimulationResult result = new LiquiditySimulationEngine()
                .simulateAll(request(new BigDecimal("1000000"), assumptions(BigDecimal.ZERO, null)))
                .getFirst();

        assertEquals(LiquiditySimulationStatus.INSUFFICIENT_DEPTH, result.status());
        assertTrue(result.fillPercentage().compareTo(new BigDecimal("1")) < 0);
        assertTrue(result.unfilledNotionalUsd().signum() > 0);
        assertEquals(LiquidityEvidenceLevel.SIMULATED, result.evidenceLevel());
        assertFalse(result.realValidated());
    }

    @Test
    void disappearingLiquidityReducesFillAndNeverImprovesVwap() {
        LiquiditySimulationEngine engine = new LiquiditySimulationEngine();
        LiquiditySimulationResult stable = engine.simulateAll(
                request(new BigDecimal("4000"), assumptions(BigDecimal.ZERO, null))).getFirst();
        LiquiditySimulationResult disappearing = engine.simulateAll(
                request(new BigDecimal("4000"), assumptions(new BigDecimal("0.60"), null))).getFirst();

        assertTrue(disappearing.filledNotionalUsd().compareTo(stable.filledNotionalUsd()) < 0);
        assertTrue(disappearing.unfilledNotionalUsd().compareTo(stable.unfilledNotionalUsd()) > 0);
        assertTrue(disappearing.vwap().compareTo(stable.vwap()) >= 0);
    }

    @Test
    void sourceCloseInterruptsTwapBeforeOpeningMoreSlices() {
        LiquiditySimulationResult uninterrupted = result(
                request(new BigDecimal("4000"), assumptions(BigDecimal.ZERO, null)),
                LiquidityExecutionStrategy.TWAP);
        LiquiditySimulationResult interrupted = result(
                request(new BigDecimal("4000"), assumptions(BigDecimal.ZERO, 2_100L)),
                LiquidityExecutionStrategy.TWAP);

        assertTrue(interrupted.sourceClosedBeforeCompletion());
        assertTrue(interrupted.filledNotionalUsd().compareTo(uninterrupted.filledNotionalUsd()) < 0);
        assertTrue(interrupted.estimatedExecutionMillis() <= 2_100L);
    }

    @Test
    void inputLevelOrderDoesNotChangeTheResult() {
        LiquiditySimulationRequest ordered = request(new BigDecimal("3500"), assumptions(BigDecimal.ZERO, null));
        List<OrderBookLevel> reversedAsks = new ArrayList<>(ordered.orderBook().asks());
        Collections.reverse(reversedAsks);
        OrderBookSnapshot permutedBook = new OrderBookSnapshot(
                ordered.orderBook().symbol(), ordered.orderBook().capturedAt(),
                ordered.orderBook().source(), ordered.orderBook().sequenceNumber(),
                ordered.orderBook().bids(), reversedAsks);
        LiquiditySimulationRequest permuted = new LiquiditySimulationRequest(
                permutedBook, ordered.side(), ordered.requestedNotionalUsd(), ordered.assumptions(), ordered.modelVersion());

        LiquiditySimulationResult a = result(ordered, LiquidityExecutionStrategy.SINGLE_MARKET);
        LiquiditySimulationResult b = result(permuted, LiquidityExecutionStrategy.SINGLE_MARKET);

        assertEquals(0, a.vwap().compareTo(b.vwap()));
        assertEquals(0, a.filledNotionalUsd().compareTo(b.filledNotionalUsd()));
    }

    private static LiquiditySimulationResult result(LiquiditySimulationRequest request,
                                                    LiquidityExecutionStrategy strategy) {
        return new LiquiditySimulationEngine().simulateAll(request).stream()
                .filter(value -> value.executionStrategy() == strategy)
                .findFirst()
                .orElseThrow();
    }

    private static LiquiditySimulationRequest request(BigDecimal notional,
                                                      LiquiditySimulationAssumptions assumptions) {
        return new LiquiditySimulationRequest(
                new OrderBookSnapshot(
                        "BTCUSDT",
                        Instant.parse("2026-07-13T12:00:00Z"),
                        "BINANCE_FAPI_DEPTH",
                        881L,
                        List.of(
                                new OrderBookLevel(new BigDecimal("99"), new BigDecimal("10")),
                                new OrderBookLevel(new BigDecimal("98"), new BigDecimal("20")),
                                new OrderBookLevel(new BigDecimal("97"), new BigDecimal("30"))),
                        List.of(
                                new OrderBookLevel(new BigDecimal("100"), new BigDecimal("10")),
                                new OrderBookLevel(new BigDecimal("101"), new BigDecimal("20")),
                                new OrderBookLevel(new BigDecimal("102"), new BigDecimal("30")))),
                SourceSide.LONG,
                notional,
                assumptions,
                "liquidity-v3"
        );
    }

    private static LiquiditySimulationAssumptions assumptions(BigDecimal disappearingLiquidityPct,
                                                              Long sourceCloseAfterMillis) {
        return new LiquiditySimulationAssumptions(
                BigDecimal.ONE,
                new BigDecimal("0.10"),
                10,
                1_000L,
                disappearingLiquidityPct,
                new BigDecimal("2"),
                100L,
                sourceCloseAfterMillis,
                new BigDecimal("4"),
                new BigDecimal("1")
        );
    }
}
