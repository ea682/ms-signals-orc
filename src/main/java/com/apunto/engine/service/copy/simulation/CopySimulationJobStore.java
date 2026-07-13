package com.apunto.engine.service.copy.simulation;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

public interface CopySimulationJobStore {
    boolean enqueue(CopySimulationContext context, CopySimulationInputSnapshot snapshot);
    List<CopySimulationJob> claimBatch(String workerId, int limit);
    boolean isPauseRequested(UUID jobId);
    void saveScenario(UUID jobId, CopySimulationScenarioFact scenario);
    void markCompleted(UUID jobId);
    void markPaused(UUID jobId);
    void markFailed(UUID jobId, String error, OffsetDateTime retryAt);
    int requeueStale(OffsetDateTime threshold);

    default boolean requestPause(UUID jobId) { throw new UnsupportedOperationException(); }
    default boolean resume(UUID jobId) { throw new UnsupportedOperationException(); }
}
