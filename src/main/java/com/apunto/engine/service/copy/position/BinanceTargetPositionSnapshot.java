package com.apunto.engine.service.copy.position;

import com.apunto.copytarget.ExistingTargetPosition;
import com.apunto.copytarget.TargetPositionSnapshotStatus;

import java.time.Instant;
import java.util.List;

public record BinanceTargetPositionSnapshot(
        TargetPositionSnapshotStatus status,
        Instant observedAt,
        String source,
        List<ExistingTargetPosition> positions,
        String reasonCode,
        String reasonDetail
) {
    public BinanceTargetPositionSnapshot {
        positions = List.copyOf(positions == null ? List.of() : positions);
        source = source == null ? "BINANCE_POSITION_RISK" : source;
        reasonCode = reasonCode == null ? "" : reasonCode;
        reasonDetail = reasonDetail == null ? "" : reasonDetail;
    }

    public boolean authoritative() {
        return status == TargetPositionSnapshotStatus.AUTHORITATIVE;
    }
}
