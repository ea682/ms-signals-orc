package com.apunto.engine.service.copy.simulation;

import com.apunto.copytarget.LiquiditySimulationAssumptions;
import com.apunto.copytarget.LiquiditySimulationEngine;
import com.apunto.copytarget.LiquiditySimulationRequest;
import com.apunto.copytarget.LiquiditySimulationResult;
import com.apunto.copytarget.OrderBookSnapshot;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;

@Slf4j
@Component
@ConditionalOnProperty(prefix = "copy.simulation.liquidity-worker", name = "enabled", havingValue = "true")
public class CopyLiquiditySimulationWorker {

    private final CopyLiquiditySimulationStore store;
    private final CopyOrderBookProvider orderBookProvider;
    private final LiquiditySimulationAssumptions assumptions;
    private final CopyLiquiditySimulationProperties properties;
    private final LiquiditySimulationEngine engine = new LiquiditySimulationEngine();

    @Autowired
    public CopyLiquiditySimulationWorker(CopyLiquiditySimulationStore store,
                                         CopyOrderBookProvider orderBookProvider,
                                         CopyLiquiditySimulationProperties properties) {
        this.store = store;
        this.orderBookProvider = orderBookProvider;
        this.properties = properties;
        this.assumptions = properties.assumptions();
    }

    CopyLiquiditySimulationWorker(CopyLiquiditySimulationStore store,
                                  CopyOrderBookProvider orderBookProvider,
                                  LiquiditySimulationAssumptions assumptions) {
        this.store = store;
        this.orderBookProvider = orderBookProvider;
        this.assumptions = assumptions;
        this.properties = new CopyLiquiditySimulationProperties();
    }

    @Scheduled(fixedDelayString = "${copy.simulation.liquidity-worker.fixed-delay-ms:5000}")
    public void tick() {
        store.requeueStale(OffsetDateTime.now().minus(properties.getStaleLock()));
        for (CopyLiquidityCandidate candidate : store.claimBatch(
                "cold-liquidity", Math.max(1, properties.getBatchSize()))) {
            try {
                process(candidate);
            } catch (RuntimeException ex) {
                store.markFailed(candidate, safeMessage(ex),
                        OffsetDateTime.now().plus(properties.getRetryDelay()));
                log.error("event=copy.liquidity.failed liquidityJobId={} capitalScenarioId={} symbol={} side={} attempt={} errorClass={} error={}",
                        candidate.id(), candidate.capitalScenarioId(), candidate.symbol(), candidate.side(), candidate.attempt(),
                        ex.getClass().getSimpleName(), safeMessage(ex), ex);
            }
        }
    }

    public CopyLiquidityWorkerOutcome process(CopyLiquidityCandidate candidate) {
        Optional<OrderBookSnapshot> snapshot = orderBookProvider.snapshot(
                candidate.symbol(), properties.getDepthLimit());
        if (snapshot.isEmpty()) {
            store.markNoBook(candidate, "NO_BOOK",
                    OffsetDateTime.now().plus(properties.getRetryDelay()));
            return CopyLiquidityWorkerOutcome.NO_BOOK;
        }
        LiquiditySimulationRequest request = new LiquiditySimulationRequest(
                snapshot.get(),
                candidate.side(),
                candidate.requestedNotionalUsd(),
                assumptions,
                "liquidity-v3"
        );
        List<LiquiditySimulationResult> results = engine.simulateAll(request);
        if (results.size() != 4 || results.stream().anyMatch(LiquiditySimulationResult::realValidated)) {
            throw new IllegalStateException("liquidity worker must create four simulation-only strategies");
        }
        store.saveResults(candidate, snapshot.get(), assumptions, results);
        return CopyLiquidityWorkerOutcome.COMPLETED;
    }

    private String safeMessage(Throwable error) {
        String message = error.getMessage();
        if (message == null || message.isBlank()) return error.getClass().getSimpleName();
        return message.length() <= 1000 ? message : message.substring(0, 1000);
    }
}
