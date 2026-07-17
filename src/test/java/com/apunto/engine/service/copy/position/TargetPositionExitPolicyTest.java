package com.apunto.engine.service.copy.position;

import com.apunto.copytarget.ExistingTargetPosition;
import com.apunto.copytarget.SourceSide;
import com.apunto.copytarget.TargetPositionSnapshotStatus;
import com.apunto.engine.shared.enums.PositionSide;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TargetPositionExitPolicyTest {

    @Test
    void authoritativeExternalFlatClosesLocalStateWithoutSubmittingAnotherOrder() {
        TargetPositionExitDecision decision = TargetPositionExitPolicy.resolve(
                snapshot(TargetPositionSnapshotStatus.AUTHORITATIVE, List.of()),
                "BTCUSDT", PositionSide.LONG, new BigDecimal("1"));

        assertTrue(decision.authoritative());
        assertTrue(decision.alreadyFlat());
        assertEquals(TargetPositionExitPolicy.EXTERNAL_FLAT, decision.reasonCode());
    }

    @Test
    void authoritativePartialExternalCloseCapsReduceOnlyQuantity() {
        ExistingTargetPosition actual = new ExistingTargetPosition(
                "BTCUSDT", SourceSide.LONG, new BigDecimal("0.4"),
                new BigDecimal("60000"), BigDecimal.ONE);
        TargetPositionExitDecision decision = TargetPositionExitPolicy.resolve(
                snapshot(TargetPositionSnapshotStatus.AUTHORITATIVE, List.of(actual)),
                "BTCUSDT", PositionSide.LONG, BigDecimal.ONE);

        assertFalse(decision.alreadyFlat());
        assertEquals(0, new BigDecimal("0.4").compareTo(decision.closeQuantity()));
        assertEquals(TargetPositionExitPolicy.AUTHORITATIVE_CAP, decision.reasonCode());
    }

    @Test
    void unavailableSnapshotNeverBlocksExitAndUsesConservativeLocalQuantity() {
        TargetPositionExitDecision decision = TargetPositionExitPolicy.resolve(
                snapshot(TargetPositionSnapshotStatus.UNAVAILABLE, List.of()),
                "BTCUSDT", PositionSide.LONG, BigDecimal.ONE);

        assertFalse(decision.authoritative());
        assertFalse(decision.alreadyFlat());
        assertEquals(0, BigDecimal.ONE.compareTo(decision.closeQuantity()));
        assertEquals(TargetPositionExitPolicy.LOCAL_FALLBACK, decision.reasonCode());
    }

    private BinanceTargetPositionSnapshot snapshot(
            TargetPositionSnapshotStatus status,
            List<ExistingTargetPosition> positions
    ) {
        return new BinanceTargetPositionSnapshot(
                status, Instant.now(), "BINANCE_POSITION_RISK", positions, "test", "");
    }
}
