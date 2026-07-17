package com.apunto.engine.service.copy.simulation;

import com.apunto.copytarget.BinanceSymbolFilter;
import com.apunto.copytarget.CalculationVersions;
import com.apunto.copytarget.SourcePosition;
import com.apunto.copytarget.SourceSide;
import com.apunto.copytarget.TargetPortfolioRequest;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CopySimulationSubmissionServiceTest {

    @Test
    void onlyMicroLiveCanCreateCapitalLeverageSimulationJobs() {
        RecordingStore store = new RecordingStore(true);
        CopySimulationSubmissionService service = new CopySimulationSubmissionService(store);

        assertEquals(CopySimulationSubmissionResult.NOT_APPLICABLE, service.submit(context("SHADOW"), request()));
        assertEquals(CopySimulationSubmissionResult.NOT_APPLICABLE, service.submit(context("LIVE"), request()));
        assertEquals(0, store.enqueueCalls);

        assertEquals(CopySimulationSubmissionResult.ENQUEUED, service.submit(context("MICRO_LIVE"), request()));
        assertEquals(1, store.enqueueCalls);
    }

    @Test
    void durableIdempotencyIsReportedAsAlreadyEnqueued() {
        RecordingStore store = new RecordingStore(false);
        CopySimulationSubmissionService service = new CopySimulationSubmissionService(store);

        assertEquals(CopySimulationSubmissionResult.ALREADY_ENQUEUED,
                service.submit(context("MICRO_LIVE"), request()));
    }

    @Test
    void snapshotRoundTripPreservesThePureCalculatorInput() {
        TargetPortfolioRequest original = request();

        TargetPortfolioRequest restored = CopySimulationInputSnapshot.from(original).toRequest();

        assertEquals(0, restored.sourceAccountEquityUsd().compareTo(original.sourceAccountEquityUsd()));
        assertEquals(original.sourceSnapshotVersion(), restored.sourceSnapshotVersion());
        assertEquals(original.sourcePositions(), restored.sourcePositions());
        assertEquals(original.filters(), restored.filters());
        assertEquals("USDC", restored.quoteAsset());
        assertEquals(original.versions(), restored.versions());
    }

    private static CopySimulationContext context(String mode) {
        return new CopySimulationContext(
                mode,
                "source-event-1",
                "user-1",
                41L,
                "0xa445",
                "generation-1",
                "MOVEMENT_ALL",
                "copy-strategy-v3",
                "ALL",
                "ALL"
        );
    }

    static TargetPortfolioRequest request() {
        Instant now = Instant.parse("2026-07-13T12:00:00Z");
        return TargetPortfolioRequest.builder()
                .calculatedAt(now)
                .sourceAccountEquityUsd(new BigDecimal("500000"))
                .equityObservedAt(now.minusSeconds(2))
                .equitySource("HYPERLIQUID_CLEARINGHOUSE")
                .maximumEquityAge(Duration.ofSeconds(30))
                .sourceSnapshotVersion(77L)
                .sourcePositions(List.of(new SourcePosition(
                        "leg-1", "HYPE", "HYPEUSDC", SourceSide.LONG,
                        new BigDecimal("4"), new BigDecimal("100000"),
                        new BigDecimal("25000"), new BigDecimal("24000"),
                        new BigDecimal("10"), 77L, new BigDecimal("0.9"))))
                .targetAllocatedCapitalUsd(new BigDecimal("100"))
                .targetLeverage(new BigDecimal("5"))
                .availableMarginUsd(new BigDecimal("100"))
                .usedMarginUsd(BigDecimal.ZERO)
                .reservedMarginUsd(BigDecimal.ZERO)
                .existingPositions(List.of())
                .filters(List.of(new BinanceSymbolFilter(
                        "HYPEUSDC", true, "USDC", new BigDecimal("0.001"),
                        new BigDecimal("100000"), new BigDecimal("0.001"),
                        new BigDecimal("5"), new BigDecimal("0.001"),
                        new BigDecimal("20"), new BigDecimal("0.9"))))
                .quoteAsset("USDC")
                .userMaxConcurrentPositions(null)
                .versions(new CalculationVersions(
                        "copy-strategy-v3", "proportional-portfolio-v3", "binance-symbol-map-v3"))
                .build();
    }

    private static final class RecordingStore implements CopySimulationJobStore {
        private final boolean enqueueResult;
        private int enqueueCalls;

        private RecordingStore(boolean enqueueResult) {
            this.enqueueResult = enqueueResult;
        }

        @Override
        public boolean enqueue(CopySimulationContext context, CopySimulationInputSnapshot snapshot) {
            enqueueCalls++;
            return enqueueResult;
        }

        @Override public List<CopySimulationJob> claimBatch(String workerId, int limit) { return List.of(); }
        @Override public boolean isPauseRequested(UUID jobId) { return false; }
        @Override public boolean requestPause(UUID jobId) { return false; }
        @Override public boolean resume(UUID jobId) { return false; }
        @Override public void saveScenario(UUID jobId, CopySimulationScenarioFact scenario) { }
        @Override public void markCompleted(UUID jobId) { }
        @Override public void markPaused(UUID jobId) { }
        @Override public void markFailed(UUID jobId, String error, OffsetDateTime retryAt) { }
        @Override public int requeueStale(OffsetDateTime threshold) { return 0; }
    }
}
