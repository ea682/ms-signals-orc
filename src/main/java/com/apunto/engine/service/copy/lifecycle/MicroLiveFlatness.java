package com.apunto.engine.service.copy.lifecycle;

public record MicroLiveFlatness(
        boolean flat,
        long activePositions,
        long pendingDispatches
) {
}
