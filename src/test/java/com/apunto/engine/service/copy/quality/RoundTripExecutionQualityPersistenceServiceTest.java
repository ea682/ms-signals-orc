package com.apunto.engine.service.copy.quality;

import com.apunto.engine.entity.CopyOperationEventEntity;
import com.apunto.engine.repository.CopyOperationEventRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class RoundTripExecutionQualityPersistenceServiceTest {

    @Test
    void completedLedgerCycleIsUpsertedWithRoundTripDragAndNeverUsedAsHardGate() {
        UUID cycle = UUID.randomUUID();
        AtomicReference<Object[]> parameters = new AtomicReference<>();
        CopyOperationEventRepository repository = (CopyOperationEventRepository) Proxy.newProxyInstance(
                CopyOperationEventRepository.class.getClassLoader(),
                new Class<?>[]{CopyOperationEventRepository.class},
                (proxy, method, args) -> "findAllByUserCopyAllocationIdAndEconomicCycleIdOrderByEventTimeAscDateCreationAsc"
                        .equals(method.getName())
                        ? List.of(event("OPEN", "100", "101", "1", cycle),
                        event("CLOSE", "110", "109", "1", cycle))
                        : null);
        JdbcTemplate jdbc = new JdbcTemplate() {
            @Override
            public int update(String sql, Object... args) {
                parameters.set(args);
                return 1;
            }
        };
        RoundTripExecutionQualityPersistenceService service = new RoundTripExecutionQualityPersistenceService(
                repository, new RoundTripExecutionQualityCalculator(), jdbc, new SimpleMeterRegistry());

        service.recalculate(7L, cycle);

        Object[] saved = parameters.get();
        assertNotNull(saved);
        assertEquals("COMPLETE", saved[5]);
        assertNotNull(saved[13]);
        assertEquals(null, saved[6]);
    }

    @Test
    void partialCloseRemainsDurablyIncompleteUntilCycleCloses() {
        UUID cycle = UUID.randomUUID();
        AtomicReference<Object[]> parameters = new AtomicReference<>();
        CopyOperationEventRepository repository = (CopyOperationEventRepository) Proxy.newProxyInstance(
                CopyOperationEventRepository.class.getClassLoader(),
                new Class<?>[]{CopyOperationEventRepository.class},
                (proxy, method, args) -> List.of(event("OPEN", "100", "101", "1", cycle),
                        event("REDUCE", "110", "109", "0.5", cycle)));
        JdbcTemplate jdbc = new JdbcTemplate() {
            @Override
            public int update(String sql, Object... args) {
                parameters.set(args);
                return 1;
            }
        };
        RoundTripExecutionQualityPersistenceService service = new RoundTripExecutionQualityPersistenceService(
                repository, new RoundTripExecutionQualityCalculator(), jdbc, new SimpleMeterRegistry());

        service.recalculate(7L, cycle);

        assertEquals("INCOMPLETE", parameters.get()[5]);
        assertEquals("ORIGIN_CYCLE_NOT_FULLY_CLOSED", parameters.get()[6]);
    }

    private static CopyOperationEventEntity event(String action, String expected, String actual,
                                                   String quantity, UUID cycle) {
        return CopyOperationEventEntity.builder()
                .userCopyAllocationId(7L)
                .economicCycleId(cycle)
                .executionMode("MICRO_LIVE")
                .positionSide("LONG")
                .eventType(action)
                .copyIntent(action)
                .expectedPrice(new BigDecimal(expected))
                .actualPrice(new BigDecimal(actual))
                .qtyExecuted(new BigDecimal(quantity))
                .feeUsd(new BigDecimal("0.01"))
                .endToEndLatencyMs(25L)
                .eventTime(OffsetDateTime.now())
                .dateCreation(OffsetDateTime.now())
                .build();
    }
}
