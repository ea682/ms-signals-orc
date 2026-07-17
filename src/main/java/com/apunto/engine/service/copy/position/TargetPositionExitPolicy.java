package com.apunto.engine.service.copy.position;

import com.apunto.copytarget.ExistingTargetPosition;
import com.apunto.copytarget.SourceSide;
import com.apunto.copytarget.TargetPositionSnapshotStatus;
import com.apunto.engine.shared.enums.PositionSide;

import java.math.BigDecimal;

public final class TargetPositionExitPolicy {

    public static final String EXTERNAL_FLAT = "RECONCILED_EXTERNAL_POSITION_ALREADY_FLAT";
    public static final String AUTHORITATIVE_CAP = "EXIT_QUANTITY_CAPPED_TO_AUTHORITATIVE_POSITION";
    public static final String LOCAL_FALLBACK = "EXIT_USING_LOCAL_QUANTITY_SNAPSHOT_UNAVAILABLE";

    private TargetPositionExitPolicy() {
    }

    public static TargetPositionExitDecision resolve(
            BinanceTargetPositionSnapshot snapshot,
            String symbol,
            PositionSide side,
            BigDecimal localQuantity
    ) {
        BigDecimal local = nonNegative(localQuantity);
        if (snapshot == null || snapshot.status() != TargetPositionSnapshotStatus.AUTHORITATIVE
                || symbol == null || symbol.isBlank()
                || (side != PositionSide.LONG && side != PositionSide.SHORT)) {
            return new TargetPositionExitDecision(false, false, local, LOCAL_FALLBACK);
        }

        SourceSide targetSide = side == PositionSide.LONG ? SourceSide.LONG : SourceSide.SHORT;
        BigDecimal actual = snapshot.positions().stream()
                .filter(position -> position.symbol().equalsIgnoreCase(symbol.trim()))
                .filter(position -> position.side() == targetSide)
                .map(ExistingTargetPosition::quantity)
                .reduce(BigDecimal.ZERO, BigDecimal::add);
        if (actual.compareTo(BigDecimal.ZERO) <= 0 || local.compareTo(BigDecimal.ZERO) <= 0) {
            return new TargetPositionExitDecision(true, true, BigDecimal.ZERO, EXTERNAL_FLAT);
        }
        BigDecimal close = local.min(actual);
        return new TargetPositionExitDecision(
                true, false, close,
                close.compareTo(local) < 0 ? AUTHORITATIVE_CAP : "EXIT_QUANTITY_AUTHORITATIVE");
    }

    private static BigDecimal nonNegative(BigDecimal value) {
        return value == null || value.compareTo(BigDecimal.ZERO) <= 0 ? BigDecimal.ZERO : value;
    }
}
