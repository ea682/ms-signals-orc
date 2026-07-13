package com.apunto.engine.service.copy.simulation;

import com.apunto.copytarget.CapitalLeverageMatrixSimulator;
import com.apunto.copytarget.CapitalLeverageScenario;
import com.apunto.copytarget.TargetPortfolioCalculator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;

@Slf4j
@Component
@ConditionalOnProperty(prefix = "copy.simulation.worker", name = "enabled", havingValue = "true")
public class CopySimulationWorker {

    private final CopySimulationJobStore store;
    private final long yieldMillis;
    private final CapitalLeverageMatrixSimulator simulator =
            new CapitalLeverageMatrixSimulator(new TargetPortfolioCalculator());

    @Value("${copy.simulation.worker.batch-size:1}")
    private int batchSize = 1;

    @Value("${copy.simulation.worker.stale-lock:PT10M}")
    private Duration staleLock = Duration.ofMinutes(10);

    @Value("${copy.simulation.worker.retry-delay:PT1M}")
    private Duration retryDelay = Duration.ofMinutes(1);

    @Value("${copy.simulation.worker.instance-id:${spring.application.name}:cold-simulation}")
    private String workerId = "cold-simulation";

    public CopySimulationWorker(
            CopySimulationJobStore store,
            @Value("${copy.simulation.worker.yield-ms:2}") long yieldMillis
    ) {
        this.store = store;
        this.yieldMillis = Math.max(0L, yieldMillis);
    }

    @Scheduled(fixedDelayString = "${copy.simulation.worker.fixed-delay-ms:2000}")
    public void tick() {
        store.requeueStale(OffsetDateTime.now().minus(staleLock));
        for (CopySimulationJob job : store.claimBatch(workerId, Math.max(1, batchSize))) {
            try {
                process(job);
            } catch (RuntimeException ex) {
                store.markFailed(job.id(), safeMessage(ex), OffsetDateTime.now().plus(retryDelay));
                log.error("event=copy.simulation.failed jobId={} sourceEventId={} allocationId={} cursor={} errorClass={} error={}",
                        job.id(), job.sourceEventId(), job.allocationId(), job.resumeCursor(),
                        ex.getClass().getSimpleName(), safeMessage(ex), ex);
            }
        }
    }

    public CopySimulationWorkerOutcome process(CopySimulationJob job) {
        List<CapitalLeverageScenario> scenarios = simulator.simulate(job.inputSnapshot().toRequest());
        if (scenarios.size() != 40) {
            throw new IllegalStateException("capital/leverage matrix must contain exactly 40 scenarios");
        }
        int cursor = Math.max(0, Math.min(job.resumeCursor(), scenarios.size()));
        for (int index = cursor; index < scenarios.size(); index++) {
            if (store.isPauseRequested(job.id())) {
                store.markPaused(job.id());
                return CopySimulationWorkerOutcome.PAUSED;
            }
            store.saveScenario(job.id(), CopySimulationScenarioFact.from(index, scenarios.get(index)));
            yieldToHotWork();
        }
        store.markCompleted(job.id());
        return CopySimulationWorkerOutcome.COMPLETED;
    }

    private void yieldToHotWork() {
        if (yieldMillis == 0L) {
            Thread.yield();
            return;
        }
        try {
            Thread.sleep(yieldMillis);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("simulation worker interrupted", ex);
        }
    }

    private static String safeMessage(Throwable error) {
        String message = error.getMessage();
        if (message == null || message.isBlank()) {
            return error.getClass().getSimpleName();
        }
        return message.length() <= 1000 ? message : message.substring(0, 1000);
    }
}
