package com.apunto.copytarget;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.HexFormat;

final class CopyPolicyV1SizingDigest {

    private CopyPolicyV1SizingDigest() {
    }

    static String sha256(CopyPolicyV1SizingStepRequest step,
                         TargetPortfolioRequest request,
                         BigDecimal defensiveEquity,
                         BigDecimal authorizedCapital) {
        StringBuilder value = new StringBuilder("copy-policy-v1-sizing-step|1");
        add(value, step.wallet());
        add(value, step.strategyKey());
        add(value, step.world());
        add(value, step.executionMode());
        add(value, step.initialAccountEquityUsd());
        add(value, step.currentMtmEquityUsd());
        add(value, defensiveEquity);
        add(value, authorizedCapital);
        add(value, request.calculatedAt());
        add(value, request.sourceAccountEquityUsd());
        add(value, request.equityObservedAt());
        add(value, request.equitySource());
        add(value, request.maximumEquityAge());
        add(value, request.sourceSnapshotVersion());
        add(value, request.targetAllocatedCapitalUsd());
        add(value, request.targetLeverage());
        add(value, request.availableMarginUsd());
        add(value, request.usedMarginUsd());
        add(value, request.reservedMarginUsd());
        add(value, request.targetPositionSnapshotStatus());
        add(value, request.quoteAsset());
        add(value, request.userMaxConcurrentPositions());
        add(value, request.versions().strategyVersion());
        add(value, request.versions().sizingPolicyVersion());
        add(value, request.versions().symbolMappingVersion());

        request.sourcePositions().stream()
                .sorted(Comparator.comparing(SourcePosition::sourceLegId))
                .forEach(position -> {
                    add(value, "source");
                    add(value, position.sourceLegId());
                    add(value, position.sourceSymbol());
                    add(value, position.targetSymbol());
                    add(value, position.side());
                    add(value, position.quantity());
                    add(value, position.notionalUsd());
                    add(value, position.marginUsedUsd());
                    add(value, position.marginProvenance());
                    add(value, position.markPrice());
                    add(value, position.entryPrice());
                    add(value, position.leverage());
                    add(value, position.snapshotVersion());
                    add(value, position.liquidityScore());
                });
        request.filters().stream()
                .sorted(Comparator.comparing(BinanceSymbolFilter::symbol))
                .forEach(filter -> {
                    add(value, "filter");
                    add(value, filter.symbol());
                    add(value, filter.trading());
                    add(value, filter.quoteAsset());
                    add(value, filter.minQty());
                    add(value, filter.maxQty());
                    add(value, filter.stepSize());
                    add(value, filter.minNotional());
                    add(value, filter.tickSize());
                    add(value, filter.maximumLeverage());
                    add(value, filter.liquidityScore());
                });
        appendPositions(value, "actual", request.existingPositions());
        appendPositions(value, "managed", request.managedExistingPositions());
        appendPositions(value, "portfolio", request.portfolioExistingPositions());

        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(value.toString().getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }

    private static void appendPositions(StringBuilder value,
                                        String category,
                                        java.util.List<ExistingTargetPosition> positions) {
        positions.stream()
                .sorted(Comparator.comparing(ExistingTargetPosition::key))
                .forEach(position -> {
                    add(value, category);
                    add(value, position.symbol());
                    add(value, position.side());
                    add(value, position.quantity());
                    add(value, position.markPrice());
                    add(value, position.marginUsd());
                });
    }

    private static void add(StringBuilder target, Object raw) {
        String value;
        if (raw == null) {
            value = "<null>";
        } else if (raw instanceof BigDecimal decimal) {
            value = decimal.signum() == 0 ? "0" : decimal.stripTrailingZeros().toPlainString();
        } else {
            value = raw.toString();
        }
        target.append('|').append(value.length()).append(':').append(value);
    }
}
