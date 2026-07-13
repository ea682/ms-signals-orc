package com.apunto.engine.service.copy.simulation;

import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CopySimulationWorkerTest {

    @Test
    void persistsExactlyFortyScenariosAndCompletes() {
        RecordingStore store = new RecordingStore(false);
        CopySimulationJob job = job(0);
        CopySimulationWorker worker = new CopySimulationWorker(store, 0L);

        CopySimulationWorkerOutcome outcome = worker.process(job);

        assertEquals(CopySimulationWorkerOutcome.COMPLETED, outcome);
        assertEquals(40, store.scenarios.size());
        assertEquals(List.of("completed"), store.terminalEvents);
    }

    @Test
    void resumeCursorSkipsAlreadyCommittedScenarios() {
        RecordingStore store = new RecordingStore(false);
        CopySimulationJob job = job(17);
        CopySimulationWorker worker = new CopySimulationWorker(store, 0L);

        worker.process(job);

        assertEquals(23, store.scenarios.size());
        assertEquals(17, store.scenarios.getFirst().scenarioIndex());
        assertEquals(List.of("completed"), store.terminalEvents);
    }

    @Test
    void pauseStopsBeforeNextScenarioAndDoesNotComplete() {
        CopySimulationJobStore store = new RecordingStore(true);
        CopySimulationJob job = job(8);
        CopySimulationWorker worker = new CopySimulationWorker(store, 0L);

        assertEquals(CopySimulationWorkerOutcome.PAUSED, worker.process(job));

        RecordingStore recording = (RecordingStore) store;
        assertEquals(0, recording.scenarios.size());
        assertEquals(List.of("paused"), recording.terminalEvents);
    }

    private static CopySimulationJob job(int cursor) {
        return new CopySimulationJob(
                UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
                "source-event-1",
                41L,
                cursor,
                CopySimulationInputSnapshot.from(CopySimulationSubmissionServiceTest.request())
        );
    }

    private static final class RecordingStore implements CopySimulationJobStore {
        private final boolean paused;
        private final List<CopySimulationScenarioFact> scenarios = new ArrayList<>();
        private final List<String> terminalEvents = new ArrayList<>();

        private RecordingStore(boolean paused) {
            this.paused = paused;
        }

        @Override public boolean enqueue(CopySimulationContext context, CopySimulationInputSnapshot snapshot) { return true; }
        @Override public List<CopySimulationJob> claimBatch(String workerId, int limit) { return List.of(); }
        @Override public boolean isPauseRequested(UUID jobId) { return paused; }
        @Override public void saveScenario(UUID jobId, CopySimulationScenarioFact scenario) { scenarios.add(scenario); }
        @Override public void markCompleted(UUID jobId) { terminalEvents.add("completed"); }
        @Override public void markPaused(UUID jobId) { terminalEvents.add("paused"); }
        @Override public void markFailed(UUID jobId, String error, OffsetDateTime retryAt) { terminalEvents.add("failed"); }
        @Override public int requeueStale(OffsetDateTime threshold) { return 0; }
    }
}
