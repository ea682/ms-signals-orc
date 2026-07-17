package com.apunto.engine.outbox.dto;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertTrue;

class MetricCopyOperationPersistedEventContractTest {

    @Test
    void calibrationLineageIsPartOfTheDurableEconomicEvent() {
        Set<String> components = Arrays.stream(MetricCopyOperationPersistedEvent.class.getRecordComponents())
                .map(component -> component.getName())
                .collect(Collectors.toSet());

        assertTrue(components.containsAll(Set.of(
                "sourceEventId", "scopeType", "scopeValue", "strategyKey", "generationId",
                "calibrationCapitalUsd", "targetLeverage", "calibrationTargetNotionalUsd",
                "copyAction", "notionalBand")));
    }
}
