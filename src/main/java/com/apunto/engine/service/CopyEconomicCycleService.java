package com.apunto.engine.service;

import com.apunto.engine.dto.CopyOperationDto;

import java.time.OffsetDateTime;
import java.util.UUID;

public interface CopyEconomicCycleService {

    CycleIdentity open(CopyOperationDto operation, UUID copyOperationId);

    void close(UUID copyOperationId, String sourceEventId, OffsetDateTime closedAt);

    record CycleIdentity(UUID cycleId, long cycleSequence) {
    }
}
