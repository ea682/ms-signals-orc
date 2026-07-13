package com.apunto.engine.service.copy.simulation;

import com.apunto.copytarget.LiquidityEvidenceLevel;
import com.apunto.copytarget.LiquiditySimulationAssumptions;
import com.apunto.copytarget.OrderBookLevel;
import com.apunto.copytarget.OrderBookSnapshot;
import com.apunto.copytarget.SourceSide;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CopyLiquiditySimulationWorkerTest {

    @Test
    void realSnapshotProducesFourSimulationOnlyExecutionStrategies() {
        RecordingStore store = new RecordingStore();
        CopyOrderBookProvider provider = (symbol, limit) -> Optional.of(book());
        CopyLiquiditySimulationWorker worker = new CopyLiquiditySimulationWorker(store, provider, assumptions());

        CopyLiquidityWorkerOutcome outcome = worker.process(candidate());

        assertEquals(CopyLiquidityWorkerOutcome.COMPLETED, outcome);
        assertEquals(4, store.results.size());
        assertTrue(store.results.stream().allMatch(value -> value.evidenceLevel() == LiquidityEvidenceLevel.SIMULATED));
        assertTrue(store.results.stream().noneMatch(value -> value.realValidated()));
        assertEquals(List.of("completed"), store.terminalEvents);
    }

    @Test
    void missingBookDoesNotInventLiquidityAndRemainsRetryable() {
        RecordingStore store = new RecordingStore();
        CopyOrderBookProvider provider = (symbol, limit) -> Optional.empty();
        CopyLiquiditySimulationWorker worker = new CopyLiquiditySimulationWorker(store, provider, assumptions());

        assertEquals(CopyLiquidityWorkerOutcome.NO_BOOK, worker.process(candidate()));

        assertTrue(store.results.isEmpty());
        assertEquals(List.of("no_book"), store.terminalEvents);
        assertFalse(store.retryAt.isBefore(OffsetDateTime.now()));
    }

    private static CopyLiquidityCandidate candidate() {
        return new CopyLiquidityCandidate(
                UUID.fromString("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
                81L,
                "BTCUSDT",
                SourceSide.LONG,
                new BigDecimal("1000000"),
                0
        );
    }

    private static OrderBookSnapshot book() {
        return new OrderBookSnapshot(
                "BTCUSDT",
                Instant.parse("2026-07-13T12:00:00Z"),
                "BINANCE_FAPI_DEPTH",
                1001L,
                List.of(new OrderBookLevel(new BigDecimal("99990"), new BigDecimal("1"))),
                List.of(new OrderBookLevel(new BigDecimal("100000"), new BigDecimal("1")))
        );
    }

    private static LiquiditySimulationAssumptions assumptions() {
        return new LiquiditySimulationAssumptions(
                new BigDecimal("0.30"), new BigDecimal("0.05"), 10, 1_000L,
                new BigDecimal("0.10"), new BigDecimal("2"), 100L, null,
                new BigDecimal("4"), new BigDecimal("1"));
    }

    private static final class RecordingStore implements CopyLiquiditySimulationStore {
        private final List<com.apunto.copytarget.LiquiditySimulationResult> results = new ArrayList<>();
        private final List<String> terminalEvents = new ArrayList<>();
        private OffsetDateTime retryAt;

        @Override public List<CopyLiquidityCandidate> claimBatch(String workerId, int limit) { return List.of(); }
        @Override public int requeueStale(OffsetDateTime threshold) { return 0; }

        @Override
        public void saveResults(CopyLiquidityCandidate candidate,
                                OrderBookSnapshot snapshot,
                                LiquiditySimulationAssumptions assumptions,
                                List<com.apunto.copytarget.LiquiditySimulationResult> values) {
            results.addAll(values);
            terminalEvents.add("completed");
        }

        @Override
        public void markNoBook(CopyLiquidityCandidate candidate, String reason, OffsetDateTime retryAt) {
            this.retryAt = retryAt;
            terminalEvents.add("no_book");
        }

        @Override public void markFailed(CopyLiquidityCandidate candidate, String reason, OffsetDateTime retryAt) {
            this.retryAt = retryAt;
            terminalEvents.add("failed");
        }
    }
}
