package com.apunto.engine.hyperliquid.service.impl;

import com.apunto.engine.hyperliquid.dto.HyperliquidDeltaRequest;
import com.apunto.engine.hyperliquid.dto.HyperliquidMappedDelta;
import com.apunto.engine.hyperliquid.dto.HyperliquidSourcePortfolioPosition;
import com.apunto.engine.hyperliquid.model.HyperliquidDeltaType;

import java.math.BigDecimal;
import java.util.Locale;

/**
 * Validates whether an estimated position delta carries a complete source snapshot that can
 * seed current origin state. The resulting baseline is state-only: it is never an economic OPEN
 * and never supplies realized or unrealized PnL.
 */
final class HyperliquidOriginBaselinePolicy {

    static final String BASELINE_KIND = "NON_ECONOMIC_ESTIMATED";

    Decision evaluate(HyperliquidMappedDelta mapped) {
        if (mapped == null || !HyperliquidDeltaType.from(mapped.deltaType()).canAdjustExistingCopy()) {
            return Decision.invalid("not_adjustment");
        }
        HyperliquidDeltaRequest request = mapped.request();
        if (request == null) {
            return Decision.invalid("request_missing");
        }
        if (!"POSITION_DELTA".equalsIgnoreCase(clean(request.economicEventKind()))) {
            return Decision.invalid("not_position_delta");
        }
        if (!Boolean.TRUE.equals(request.sourceEstimated())) {
            return Decision.invalid("source_not_estimated");
        }
        if (!positiveVersion(request.walletVersion())) {
            return Decision.invalid("wallet_version_missing");
        }
        if (!samePositiveVersion(request.snapshotVersion(), request.sourceSnapshotVersion())) {
            return Decision.invalid("source_snapshot_version_mismatch");
        }
        if (!samePositiveVersion(request.snapshotVersion(), request.sourcePortfolioSnapshotVersion())) {
            return Decision.invalid("portfolio_snapshot_version_mismatch");
        }
        if (!Boolean.TRUE.equals(request.sourcePortfolioComplete())) {
            return Decision.invalid("portfolio_incomplete");
        }

        HyperliquidSourcePortfolioPosition sourcePosition = request.sourcePortfolioPositions().stream()
                .filter(position -> sameSymbol(position.sourceSymbol(), mapped.symbol()))
                .filter(position -> sameText(position.sourceSide(), mapped.side()))
                .findFirst()
                .orElse(null);
        if (sourcePosition == null) {
            return Decision.invalid("source_position_missing");
        }
        if (!samePositiveVersion(request.snapshotVersion(), sourcePosition.sourceSnapshotVersion())) {
            return Decision.invalid("position_snapshot_version_mismatch");
        }
        if (!positive(sourcePosition.sourcePositionQuantity())) {
            return Decision.invalid("source_quantity_missing");
        }
        if (!sameDecimal(request.sourcePositionQuantity(), sourcePosition.sourcePositionQuantity())) {
            return Decision.invalid("source_quantity_mismatch");
        }
        if (!positive(sourcePosition.sourcePositionNotionalUsd())) {
            return Decision.invalid("source_notional_missing");
        }
        if (!positive(sourcePosition.sourceMarkPrice())) {
            return Decision.invalid("source_mark_price_missing");
        }
        if (!positive(sourcePosition.sourceEntryPrice())) {
            return Decision.invalid("source_entry_price_missing");
        }
        if (!positive(sourcePosition.sourceLeverage())) {
            return Decision.invalid("source_leverage_missing");
        }

        return Decision.valid(request.snapshotVersion(), sourcePosition);
    }

    private boolean positiveVersion(Long value) {
        return value != null && value > 0L;
    }

    private boolean samePositiveVersion(Long expected, Long actual) {
        return positiveVersion(expected) && expected.equals(actual);
    }

    private boolean positive(BigDecimal value) {
        return value != null && value.compareTo(BigDecimal.ZERO) > 0;
    }

    private boolean sameDecimal(BigDecimal left, BigDecimal right) {
        return left != null && right != null && left.compareTo(right) == 0;
    }

    private boolean sameText(String left, String right) {
        return clean(left).equals(clean(right));
    }

    private boolean sameSymbol(String left, String right) {
        return normalizeSymbol(left).equals(normalizeSymbol(right));
    }

    private String normalizeSymbol(String value) {
        return clean(value)
                .replace("-", "")
                .replace("_", "")
                .replace("/", "")
                .replace(".", "")
                .replace(" ", "");
    }

    private String clean(String value) {
        return value == null ? "" : value.trim().toUpperCase(Locale.ROOT);
    }

    record Decision(
            boolean valid,
            String reason,
            String baselineKind,
            boolean economicEvent,
            Long snapshotVersion,
            Long sourceTs,
            BigDecimal sizeQty,
            BigDecimal notionalUsd,
            BigDecimal markPrice,
            BigDecimal entryPrice,
            BigDecimal leverage,
            BigDecimal realizedPnlUsd,
            BigDecimal unrealizedPnlUsd
    ) {
        private static Decision valid(
                Long snapshotVersion,
                HyperliquidSourcePortfolioPosition position
        ) {
            return new Decision(
                    true,
                    "valid_source_snapshot",
                    BASELINE_KIND,
                    false,
                    snapshotVersion,
                    position.sourceTs(),
                    position.sourcePositionQuantity(),
                    position.sourcePositionNotionalUsd(),
                    position.sourceMarkPrice(),
                    position.sourceEntryPrice(),
                    position.sourceLeverage(),
                    null,
                    null
            );
        }

        private static Decision invalid(String reason) {
            return new Decision(
                    false,
                    reason,
                    null,
                    false,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null
            );
        }
    }
}
