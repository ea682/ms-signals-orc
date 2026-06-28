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
        if ("OPEN".equals(originalEventType)
                && (computed == PositionDeltaType.REDUCE || computed == PositionDeltaType.CLOSE_FULL || computed == PositionDeltaType.NOOP || computed == PositionDeltaType.SNAPSHOT_NOOP)) {
            return "EVENT_TYPE_CONTRADICTS_POSITION_MATH";
        }
        if (("RESIZE".equals(originalDeltaType) || "UPDATE".equals(originalDeltaType))
                && computed == PositionDeltaType.OPEN) {
            return "EVENT_TYPE_CONTRADICTS_POSITION_MATH";
        }
        return null;
    }

    private String normalize(String value) {
        return value == null || value.isBlank() ? null : value.trim().toUpperCase();
    }
}
