package com.apunto.engine.hyperliquid;

import com.apunto.engine.dto.OperationMovementEventRecordCommand;
import com.apunto.engine.entity.OperationMovementEventEntity;
import com.apunto.engine.hyperliquid.dto.HyperliquidDeltaRequest;
import com.apunto.engine.outbox.dto.MetricMovementPersistedEvent;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.RecordComponent;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HyperliquidEconomicLedgerContractTest {

    private static final Set<String> FIELDS = Set.of(
            "economicEventKind", "economicEventVersion", "sourceEventId", "sourceSequence",
            "sourceFeeUsd", "fundingPnlUsd", "executionPriceBasis", "notionalBasis",
            "lifecycleQualityFlags", "sourceEstimated"
    );

    @Test
    void requestCommandEntityAndOutboxExposeTheSameVersionedEconomicFields() {
        assertRecordFields(HyperliquidDeltaRequest.class);
        assertRecordFields(MetricMovementPersistedEvent.class);
        assertBeanFields(OperationMovementEventRecordCommand.class);
        assertBeanFields(OperationMovementEventEntity.class);
    }

    @Test
    void forwardOnlyMigrationCreatesEveryEconomicColumn() throws IOException {
        String resource = "/db/migration/V202607120001__hyperliquid_economic_event_contract.sql";
        var stream = getClass().getResourceAsStream(resource);
        assertNotNull(stream, "economic contract migration must exist");
        String sql = new String(stream.readAllBytes(), StandardCharsets.UTF_8).toLowerCase();
        for (String column : Set.of(
                "economic_event_kind", "economic_event_version", "source_event_id", "source_sequence",
                "source_fee_usd", "funding_pnl_usd", "execution_price_basis", "notional_basis",
                "lifecycle_quality_flags", "source_estimated"
        )) {
            assertTrue(sql.contains(column), () -> "migration is missing " + column);
        }
        assertTrue(sql.contains("add column if not exists"));
    }

    private void assertRecordFields(Class<?> type) {
        Set<String> actual = Arrays.stream(type.getRecordComponents())
                .map(RecordComponent::getName)
                .collect(Collectors.toSet());
        assertTrue(actual.containsAll(FIELDS), () -> type.getSimpleName() + " is missing " + difference(actual));
    }

    private void assertBeanFields(Class<?> type) {
        Set<String> actual = Arrays.stream(type.getDeclaredFields())
                .map(field -> field.getName())
                .collect(Collectors.toSet());
        assertTrue(actual.containsAll(FIELDS), () -> type.getSimpleName() + " is missing " + difference(actual));
    }

    private Set<String> difference(Set<String> actual) {
        return FIELDS.stream().filter(field -> !actual.contains(field)).collect(Collectors.toSet());
    }
}
