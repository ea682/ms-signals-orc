package com.apunto.copytarget;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;

public final class LiquiditySimulationEngine {

    private static final BigDecimal BPS = new BigDecimal("10000");
    private static final BigDecimal EIGHT_HOURS_MILLIS = new BigDecimal("28800000");
    private static final MathContext MC = new MathContext(34, RoundingMode.HALF_UP);

    public List<LiquiditySimulationResult> simulateAll(LiquiditySimulationRequest request) {
        List<LiquiditySimulationResult> results = new ArrayList<>(4);
        for (LiquidityExecutionStrategy strategy : LiquidityExecutionStrategy.values()) {
            results.add(simulate(request, strategy));
        }
        return List.copyOf(results);
    }

    public LiquiditySimulationResult simulate(LiquiditySimulationRequest request,
                                              LiquidityExecutionStrategy strategy) {
        List<OrderBookLevel> levels = request.side() == SourceSide.LONG
                ? request.orderBook().asks()
                : request.orderBook().bids();
        if (levels.isEmpty()) {
            return noBook(request, strategy);
        }

        LiquiditySimulationAssumptions assumptions = request.assumptions();
        BigDecimal survival = BigDecimal.ONE.subtract(assumptions.disappearingLiquidityPct(), MC);
        BigDecimal adverseFactor = assumptions.adverseSelectionBps().divide(BPS, MC);
        BigDecimal totalVisibleNotional = BigDecimal.ZERO;
        List<EffectiveLevel> effectiveLevels = new ArrayList<>(levels.size());
        for (OrderBookLevel level : levels) {
            BigDecimal effectivePrice = request.side() == SourceSide.LONG
                    ? level.price().multiply(BigDecimal.ONE.add(adverseFactor, MC), MC)
                    : level.price().multiply(BigDecimal.ONE.subtract(adverseFactor, MC), MC);
            BigDecimal effectiveQuantity = level.quantity().multiply(survival, MC);
            BigDecimal notional = effectivePrice.multiply(effectiveQuantity, MC);
            totalVisibleNotional = totalVisibleNotional.add(notional, MC);
            effectiveLevels.add(new EffectiveLevel(effectivePrice, effectiveQuantity));
        }

        int plannedSlices = strategy == LiquidityExecutionStrategy.SINGLE_MARKET
                ? 1
                : assumptions.fragmentCount();
        long plannedDuration = assumptions.networkLatencyMillis()
                + Math.max(0L, (long) plannedSlices - 1L) * assumptions.intervalMillis();
        int completedSlices = completedSlices(assumptions, plannedSlices);
        boolean sourceClosed = completedSlices < plannedSlices;
        long duration = sourceClosed && assumptions.sourceCloseAfterMillis() != null
                ? Math.min(plannedDuration, assumptions.sourceCloseAfterMillis())
                : plannedDuration;

        BigDecimal maxDepthCapacity = totalVisibleNotional
                .multiply(assumptions.maximumDepthConsumptionPct(), MC);
        BigDecimal temporalCapacity = maxDepthCapacity;
        if (plannedSlices > 1 && completedSlices < plannedSlices) {
            temporalCapacity = temporalCapacity.multiply(
                    BigDecimal.valueOf(completedSlices).divide(BigDecimal.valueOf(plannedSlices), MC), MC);
        }
        if (strategy == LiquidityExecutionStrategy.PARTICIPATION_CAP) {
            BigDecimal participationCapacity = totalVisibleNotional
                    .multiply(assumptions.participationCapPct(), MC)
                    .multiply(BigDecimal.valueOf(completedSlices), MC);
            temporalCapacity = temporalCapacity.min(participationCapacity);
        }
        BigDecimal desiredNotional = request.requestedNotionalUsd().min(temporalCapacity);

        BigDecimal remaining = desiredNotional;
        BigDecimal filled = BigDecimal.ZERO;
        BigDecimal filledQuantity = BigDecimal.ZERO;
        for (EffectiveLevel level : effectiveLevels) {
            if (remaining.signum() <= 0) {
                break;
            }
            BigDecimal levelNotional = level.price().multiply(level.quantity(), MC);
            BigDecimal quoteTaken = remaining.min(levelNotional);
            BigDecimal quantityTaken = quoteTaken.divide(level.price(), MC);
            filled = filled.add(quoteTaken, MC);
            filledQuantity = filledQuantity.add(quantityTaken, MC);
            remaining = remaining.subtract(quoteTaken, MC);
        }

        BigDecimal unfilled = request.requestedNotionalUsd().subtract(filled, MC).max(BigDecimal.ZERO);
        BigDecimal vwap = filledQuantity.signum() == 0 ? null : filled.divide(filledQuantity, MC);
        BigDecimal bestPrice = levels.getFirst().price();
        BigDecimal slippageBps = vwap == null
                ? null
                : adverseSlippageBps(request.side(), bestPrice, vwap);
        BigDecimal depthConsumed = ratio(filled, totalVisibleNotional);
        BigDecimal fillPercentage = ratio(filled, request.requestedNotionalUsd());
        BigDecimal fees = filled.multiply(assumptions.takerFeeBps(), MC).divide(BPS, MC);
        BigDecimal funding = filled
                .multiply(assumptions.fundingBpsPerEightHours(), MC)
                .divide(BPS, MC)
                .multiply(BigDecimal.valueOf(duration), MC)
                .divide(EIGHT_HOURS_MILLIS, MC);
        LiquiditySimulationStatus status = filled.compareTo(request.requestedNotionalUsd()) >= 0
                ? LiquiditySimulationStatus.ESTIMATED
                : LiquiditySimulationStatus.INSUFFICIENT_DEPTH;

        return new LiquiditySimulationResult(
                strategy,
                status,
                request.requestedNotionalUsd(),
                filled,
                unfilled,
                bestPrice,
                vwap,
                slippageBps,
                depthConsumed,
                fillPercentage,
                duration,
                depthConsumed,
                fees,
                funding,
                assumptions.adverseSelectionBps(),
                sourceClosed,
                LiquidityEvidenceLevel.SIMULATED,
                false,
                request.orderBook().capturedAt(),
                request.modelVersion()
        );
    }

    private int completedSlices(LiquiditySimulationAssumptions assumptions, int plannedSlices) {
        if (assumptions.sourceCloseAfterMillis() == null || plannedSlices == 1) {
            return plannedSlices;
        }
        long availableAfterLatency = assumptions.sourceCloseAfterMillis() - assumptions.networkLatencyMillis();
        if (availableAfterLatency < 0) {
            return 0;
        }
        if (assumptions.intervalMillis() == 0) {
            return plannedSlices;
        }
        long completed = availableAfterLatency / assumptions.intervalMillis() + 1L;
        return (int) Math.max(0L, Math.min(plannedSlices, completed));
    }

    private BigDecimal adverseSlippageBps(SourceSide side, BigDecimal best, BigDecimal vwap) {
        BigDecimal difference = side == SourceSide.LONG
                ? vwap.subtract(best, MC)
                : best.subtract(vwap, MC);
        return difference.max(BigDecimal.ZERO).divide(best, MC).multiply(BPS, MC);
    }

    private BigDecimal ratio(BigDecimal numerator, BigDecimal denominator) {
        if (denominator == null || denominator.signum() <= 0) {
            return BigDecimal.ZERO;
        }
        return numerator.divide(denominator, MC).max(BigDecimal.ZERO).min(BigDecimal.ONE);
    }

    private LiquiditySimulationResult noBook(LiquiditySimulationRequest request,
                                             LiquidityExecutionStrategy strategy) {
        return new LiquiditySimulationResult(
                strategy,
                LiquiditySimulationStatus.NO_BOOK,
                request.requestedNotionalUsd(),
                BigDecimal.ZERO,
                request.requestedNotionalUsd(),
                null,
                null,
                null,
                BigDecimal.ZERO,
                BigDecimal.ZERO,
                request.assumptions().networkLatencyMillis(),
                BigDecimal.ZERO,
                BigDecimal.ZERO,
                BigDecimal.ZERO,
                request.assumptions().adverseSelectionBps(),
                false,
                LiquidityEvidenceLevel.SIMULATED,
                false,
                request.orderBook().capturedAt(),
                request.modelVersion()
        );
    }

    private record EffectiveLevel(BigDecimal price, BigDecimal quantity) {
    }
}
