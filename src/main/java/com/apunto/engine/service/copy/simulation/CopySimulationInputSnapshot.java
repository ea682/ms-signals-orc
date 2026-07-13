package com.apunto.engine.service.copy.simulation;

import com.apunto.copytarget.BinanceSymbolFilter;
import com.apunto.copytarget.CalculationVersions;
import com.apunto.copytarget.SourcePosition;
import com.apunto.copytarget.TargetPortfolioRequest;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

public record CopySimulationInputSnapshot(
        Instant calculatedAt,
        BigDecimal sourceAccountEquityUsd,
        Instant equityObservedAt,
        String equitySource,
        long maximumEquityAgeMillis,
        long sourceSnapshotVersion,
        List<SourcePosition> sourcePositions,
        List<BinanceSymbolFilter> filters,
        String quoteAsset,
        Integer userMaxConcurrentPositions,
        CalculationVersions versions
) {
    public CopySimulationInputSnapshot {
        Objects.requireNonNull(calculatedAt, "calculatedAt");
        sourcePositions = List.copyOf(sourcePositions == null ? List.of() : sourcePositions);
        filters = List.copyOf(filters == null ? List.of() : filters);
        Objects.requireNonNull(quoteAsset, "quoteAsset");
        Objects.requireNonNull(versions, "versions");
        if (maximumEquityAgeMillis < 0) {
            throw new IllegalArgumentException("maximumEquityAgeMillis must not be negative");
        }
    }

    public static CopySimulationInputSnapshot from(TargetPortfolioRequest request) {
        Objects.requireNonNull(request, "request");
        return new CopySimulationInputSnapshot(
                request.calculatedAt(),
                request.sourceAccountEquityUsd(),
                request.equityObservedAt(),
                request.equitySource(),
                request.maximumEquityAge().toMillis(),
                request.sourceSnapshotVersion(),
                request.sourcePositions(),
                request.filters(),
                request.quoteAsset(),
                request.userMaxConcurrentPositions(),
                request.versions()
        );
    }

    public TargetPortfolioRequest toRequest() {
        return TargetPortfolioRequest.builder()
                .calculatedAt(calculatedAt)
                .sourceAccountEquityUsd(sourceAccountEquityUsd)
                .equityObservedAt(equityObservedAt)
                .equitySource(equitySource)
                .maximumEquityAge(java.time.Duration.ofMillis(maximumEquityAgeMillis))
                .sourceSnapshotVersion(sourceSnapshotVersion)
                .sourcePositions(sourcePositions)
                .targetAllocatedCapitalUsd(BigDecimal.ZERO)
                .targetLeverage(BigDecimal.ONE)
                .availableMarginUsd(BigDecimal.ZERO)
                .usedMarginUsd(BigDecimal.ZERO)
                .reservedMarginUsd(BigDecimal.ZERO)
                .existingPositions(List.of())
                .filters(filters)
                .quoteAsset(quoteAsset)
                .userMaxConcurrentPositions(userMaxConcurrentPositions)
                .versions(versions)
                .build();
    }
}
