package com.apunto.engine.service.copy.simulation;

import java.util.UUID;

public record CopySimulationJob(
        UUID id,
        String sourceEventId,
        Long allocationId,
        int resumeCursor,
        CopySimulationInputSnapshot inputSnapshot
) {
}
