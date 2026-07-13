package com.apunto.engine.service.copy.simulation;

import com.apunto.copytarget.LiquiditySimulationAssumptions;
import com.apunto.copytarget.LiquiditySimulationResult;
import com.apunto.copytarget.OrderBookSnapshot;

import java.time.OffsetDateTime;
import java.util.List;

public interface CopyLiquiditySimulationStore {
    List<CopyLiquidityCandidate> claimBatch(String workerId, int limit);
    int requeueStale(OffsetDateTime threshold);
    void saveResults(CopyLiquidityCandidate candidate,
                     OrderBookSnapshot snapshot,
                     LiquiditySimulationAssumptions assumptions,
                     List<LiquiditySimulationResult> results);
    void markNoBook(CopyLiquidityCandidate candidate, String reason, OffsetDateTime retryAt);
    void markFailed(CopyLiquidityCandidate candidate, String reason, OffsetDateTime retryAt);
}
