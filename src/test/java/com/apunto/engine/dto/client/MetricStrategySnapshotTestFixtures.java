package com.apunto.engine.dto.client;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

public final class MetricStrategySnapshotTestFixtures {

    private MetricStrategySnapshotTestFixtures() {
    }

    public static MetricStrategySnapshotDto.SimulationMatrixDto completeMatrix(
            String strategyKey,
            String generationId
    ) {
        List<MetricStrategySnapshotDto.SimulationScenarioDto> scenarios = new ArrayList<>();
        int index = 0;
        for (String capital : List.of(
                "100", "250", "500", "1000", "5000", "10000", "50000",
                "100000", "250000", "500000", "1000000")) {
            for (String leverage : List.of("5", "10", "15", "20")) {
                scenarios.add(MetricStrategySnapshotDto.SimulationScenarioDto.builder()
                        .scenarioIndex(index++)
                        .capitalUsd(new BigDecimal(capital))
                        .targetLeverage(new BigDecimal(leverage))
                        .build());
            }
        }
        return MetricStrategySnapshotDto.SimulationMatrixDto.builder()
                .available(true)
                .status("COMPLETE")
                .strategyKey(strategyKey)
                .generationId(generationId)
                .jobId("test-simulation-job")
                .completedAt(OffsetDateTime.now(ZoneOffset.UTC).minusSeconds(1))
                .scenarioCount(44)
                .expectedScenarioCount(44)
                .scenarios(scenarios)
                .reasonCodes(List.of())
                .build();
    }
}
