package com.apunto.copytarget.runner;

import com.apunto.copytarget.BinanceSymbolFilter;
import com.apunto.copytarget.CalculationVersions;
import com.apunto.copytarget.CopyExecutionMode;
import com.apunto.copytarget.CopyPolicyV1SizingStepRequest;
import com.apunto.copytarget.CopyPolicyWorld;
import com.apunto.copytarget.ExistingTargetPosition;
import com.apunto.copytarget.MarginProvenance;
import com.apunto.copytarget.SourcePosition;
import com.apunto.copytarget.SourceSide;
import com.apunto.copytarget.TargetPortfolioRequest;
import com.apunto.copytarget.TargetPositionSnapshotStatus;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

final class CopyPolicyV1Protocol {

    private CopyPolicyV1Protocol() {
    }

    record StepRequest(
            String requestId,
            String wallet,
            String strategyKey,
            CopyPolicyWorld world,
            CopyExecutionMode executionMode,
            BigDecimal initialAccountEquityUsd,
            BigDecimal currentMtmEquityUsd,
            Instant calculatedAt,
            BigDecimal sourceAccountEquityUsd,
            Instant equityObservedAt,
            String equitySource,
            long maximumEquityAgeMs,
            long sourceSnapshotVersion,
            List<SourceLeg> sourcePositions,
            BigDecimal targetLeverage,
            BigDecimal availableMarginUsd,
            BigDecimal usedMarginUsd,
            List<TargetPosition> existingPositions,
            List<TargetPosition> managedExistingPositions,
            List<TargetPosition> portfolioExistingPositions,
            TargetPositionSnapshotStatus targetPositionSnapshotStatus,
            List<SymbolFilter> filters,
            String quoteAsset,
            Integer userMaxConcurrentPositions,
            String strategyVersion,
            String sizingPolicyVersion,
            String symbolMappingVersion
    ) {
        CopyPolicyV1SizingStepRequest toCore() {
            TargetPortfolioRequest sizing = TargetPortfolioRequest.builder()
                    .calculatedAt(calculatedAt)
                    .sourceAccountEquityUsd(sourceAccountEquityUsd)
                    .equityObservedAt(equityObservedAt)
                    .equitySource(equitySource)
                    .maximumEquityAge(Duration.ofMillis(maximumEquityAgeMs))
                    .sourceSnapshotVersion(sourceSnapshotVersion)
                    .sourcePositions(values(sourcePositions).stream().map(SourceLeg::toCore).toList())
                    .targetAllocatedCapitalUsd(initialAccountEquityUsd)
                    .targetLeverage(targetLeverage)
                    .availableMarginUsd(availableMarginUsd)
                    .usedMarginUsd(usedMarginUsd)
                    .reservedMarginUsd(BigDecimal.ZERO)
                    .existingPositions(values(existingPositions).stream().map(TargetPosition::toCore).toList())
                    .managedExistingPositions(values(managedExistingPositions).stream().map(TargetPosition::toCore).toList())
                    .portfolioExistingPositions(values(portfolioExistingPositions).stream().map(TargetPosition::toCore).toList())
                    .targetPositionSnapshotStatus(targetPositionSnapshotStatus)
                    .filters(values(filters).stream().map(SymbolFilter::toCore).toList())
                    .quoteAsset(quoteAsset)
                    .userMaxConcurrentPositions(userMaxConcurrentPositions)
                    .versions(new CalculationVersions(
                            strategyVersion, sizingPolicyVersion, symbolMappingVersion))
                    .build();
            return new CopyPolicyV1SizingStepRequest(
                    wallet, strategyKey, world, executionMode, initialAccountEquityUsd,
                    currentMtmEquityUsd, sizing);
        }
    }

    record SourceLeg(
            String sourceLegId,
            String sourceSymbol,
            String targetSymbol,
            SourceSide side,
            BigDecimal quantity,
            BigDecimal notionalUsd,
            BigDecimal marginUsedUsd,
            MarginProvenance marginProvenance,
            BigDecimal markPrice,
            BigDecimal entryPrice,
            BigDecimal leverage,
            long snapshotVersion,
            BigDecimal liquidityScore
    ) {
        SourcePosition toCore() {
            return new SourcePosition(
                    sourceLegId, sourceSymbol, targetSymbol, side, quantity, notionalUsd,
                    marginUsedUsd, marginProvenance, markPrice, entryPrice, leverage,
                    snapshotVersion, liquidityScore);
        }
    }

    record TargetPosition(
            String symbol,
            SourceSide side,
            BigDecimal quantity,
            BigDecimal markPrice,
            BigDecimal marginUsd
    ) {
        ExistingTargetPosition toCore() {
            return new ExistingTargetPosition(symbol, side, quantity, markPrice, marginUsd);
        }
    }

    record SymbolFilter(
            String symbol,
            boolean trading,
            String quoteAsset,
            BigDecimal minQty,
            BigDecimal maxQty,
            BigDecimal stepSize,
            BigDecimal minNotional,
            BigDecimal tickSize,
            BigDecimal maximumLeverage,
            BigDecimal liquidityScore
    ) {
        BinanceSymbolFilter toCore() {
            return new BinanceSymbolFilter(
                    symbol, trading, quoteAsset, minQty, maxQty, stepSize, minNotional,
                    tickSize, maximumLeverage, liquidityScore);
        }
    }

    private static <T> List<T> values(List<T> values) {
        return values == null ? List.of() : values;
    }
}
