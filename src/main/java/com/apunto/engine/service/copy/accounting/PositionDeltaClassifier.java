package com.apunto.engine.service.copy.accounting;

import java.math.BigDecimal;

public final class PositionDeltaClassifier {

    private static final BigDecimal ZERO = BigDecimal.ZERO;

    public PositionDeltaType classify(BigDecimal previousQty, BigDecimal resultingQty) {
        BigDecimal previous = nonNegative(previousQty);
        BigDecimal resulting = nonNegative(resultingQty);
        int resultingVsZero = resulting.compareTo(ZERO);
        if (previous.compareTo(ZERO) == 0 && resultingVsZero > 0) {
            return PositionDeltaType.OPEN;
        }
        int compare = resulting.compareTo(previous);
        if (compare > 0) {
            return PositionDeltaType.INCREASE;
        }
        if (compare < 0 && resultingVsZero > 0) {
            return PositionDeltaType.REDUCE;
        }
        if (resultingVsZero == 0 && previous.compareTo(ZERO) > 0) {
            return PositionDeltaType.CLOSE_FULL;
        }
        return PositionDeltaType.NOOP;
    }

    public PositionDeltaClassification classify(PositionDeltaClassificationInput input) {
        BigDecimal rawPrevious = input == null ? null : input.previousQty();
        BigDecimal rawResulting = input == null ? null : input.resultingQty();
        if (isNegative(rawPrevious) || isNegative(rawResulting)) {
            return new PositionDeltaClassification(
                    PositionDeltaType.INVALID,
                    false,
                    ZERO,
                    "INVALID_QTY",
                    "NEGATIVE_QTY"
            );
        }
        if (rawPrevious == null || rawResulting == null) {
            PositionDeltaType fallback = fallbackDeltaType(input);
            return new PositionDeltaClassification(
                    fallback,
                    false,
                    ZERO,
                    reasonCode(fallback),
                    "POSITION_QTY_MISSING_FOR_MATH"
            );
        }
        BigDecimal previous = nonNegative(input == null ? null : input.previousQty());
        BigDecimal resulting = nonNegative(input == null ? null : input.resultingQty());
        String previousSide = normalize(input == null ? null : input.previousSide());
        String resultingSide = normalize(input == null ? null : input.resultingSide());
        PositionDeltaType computed = classify(previous, resulting);

        if (previous.compareTo(ZERO) > 0
                && resulting.compareTo(ZERO) > 0
                && previousSide != null
                && resultingSide != null
                && !previousSide.equals(resultingSide)) {
            computed = PositionDeltaType.FLIP;
        } else if (computed == PositionDeltaType.NOOP && isSnapshotLike(input)) {
            computed = PositionDeltaType.SNAPSHOT_NOOP;
        }

        boolean shouldRealizePnl = computed == PositionDeltaType.REDUCE || computed == PositionDeltaType.CLOSE_FULL;
        BigDecimal qtyToRealize = switch (computed) {
            case REDUCE -> previous.subtract(resulting).abs();
            case CLOSE_FULL -> previous;
            default -> ZERO;
        };
        return new PositionDeltaClassification(
                computed,
                shouldRealizePnl,
                qtyToRealize,
                reasonCode(computed),
                warningCode(input, computed)
        );
    }

    private BigDecimal nonNegative(BigDecimal value) {
        return value == null || value.compareTo(ZERO) <= 0 ? ZERO : value;
    }

    private boolean isNegative(BigDecimal value) {
        return value != null && value.compareTo(ZERO) < 0;
    }

    private boolean isSnapshotLike(PositionDeltaClassificationInput input) {
        String originalDeltaType = normalize(input == null ? null : input.originalDeltaType());
        String originalEventType = normalize(input == null ? null : input.originalEventType());
        return "UPDATE".equals(originalDeltaType)
                || "NO_CHANGE".equals(originalDeltaType)
                || "SNAPSHOT".equals(originalDeltaType)
                || "SNAPSHOT".equals(originalEventType);
    }

    private String reasonCode(PositionDeltaType computed) {
        return computed == null ? "INVALID" : computed.name();
    }

    private String warningCode(PositionDeltaClassificationInput input, PositionDeltaType computed) {
        if (input == null || computed == null) {
            return null;
        }
        String originalEventType = normalize(input.originalEventType());
        String originalDeltaType = normalize(input.originalDeltaType());
        if (("OPEN".equals(originalEventType) || "INCREASE".equals(originalEventType) || "FLIP".equals(originalEventType))
                && (computed == PositionDeltaType.REDUCE || computed == PositionDeltaType.CLOSE_FULL || computed == PositionDeltaType.NOOP || computed == PositionDeltaType.SNAPSHOT_NOOP)) {
            return "EVENT_TYPE_CONTRADICTS_POSITION_MATH";
        }
        if (("REDUCE".equals(originalEventType) || "CLOSE".equals(originalEventType) || "PANIC_CLOSE".equals(originalEventType))
                && (computed == PositionDeltaType.OPEN || computed == PositionDeltaType.INCREASE)) {
            return "EVENT_TYPE_CONTRADICTS_POSITION_MATH";
        }
        if (("RESIZE".equals(originalDeltaType) || "UPDATE".equals(originalDeltaType))
                && computed == PositionDeltaType.OPEN) {
            return "EVENT_TYPE_CONTRADICTS_POSITION_MATH";
        }
        return null;
    }

    private PositionDeltaType fallbackDeltaType(PositionDeltaClassificationInput input) {
        String originalEventType = normalize(input == null ? null : input.originalEventType());
        String originalDeltaType = normalize(input == null ? null : input.originalDeltaType());
        if ("CLOSE".equals(originalEventType) || "PANIC_CLOSE".equals(originalEventType) || "CLOSE".equals(originalDeltaType)) {
            return PositionDeltaType.CLOSE_FULL;
        }
        if ("REDUCE".equals(originalEventType)) {
            return PositionDeltaType.REDUCE;
        }
        if ("OPEN".equals(originalEventType) || "OPEN".equals(originalDeltaType)) {
            return PositionDeltaType.OPEN;
        }
        if ("INCREASE".equals(originalEventType) || "RESIZE".equals(originalDeltaType)) {
            return PositionDeltaType.INCREASE;
        }
        if ("FLIP".equals(originalEventType) || "FLIP".equals(originalDeltaType)) {
            return PositionDeltaType.FLIP;
        }
        if (isSnapshotLike(input)) {
            return PositionDeltaType.SNAPSHOT_NOOP;
        }
        return PositionDeltaType.INVALID;
    }

    private String normalize(String value) {
        return value == null || value.isBlank() ? null : value.trim().toUpperCase();
    }
}
