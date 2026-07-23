package com.apunto.engine.service.impl;

import com.apunto.engine.repository.OperationMovementEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.data.jpa.repository.Query;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OperationMovementEconomicOrderRegressionTest {

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void sixteenWorkersProduceTheSameTotalOrderForEqualTimestampFills() throws Exception {
        FillFixture[] fixtures;
        try (var input = getClass().getResourceAsStream(
                "/fixtures/economic/same-timestamp-user-fills-sanitized.json")) {
            assertNotNull(input);
            fixtures = new ObjectMapper().findAndRegisterModules()
                    .readValue(input, FillFixture[].class);
        }

        Class<?> orderType = Class.forName(
                "com.apunto.engine.service.movement.MovementEconomicOrder");
        Constructor<?> constructor = orderType.getConstructor(
                OffsetDateTime.class, Long.class, String.class);
        var pool = Executors.newFixedThreadPool(16);
        try {
            for (int iteration = 0; iteration < 16; iteration++) {
                List<FillFixture> physicalArrival = new ArrayList<>(
                        Arrays.asList(fixtures));
                Collections.rotate(physicalArrival, iteration % physicalArrival.size());
                if ((iteration & 1) == 1) {
                    Collections.reverse(physicalArrival);
                }
                List<Callable<Comparable>> tasks = physicalArrival.stream()
                        .<Callable<Comparable>>map(fixture -> () -> (Comparable)
                                constructor.newInstance(
                                        fixture.eventTime(),
                                        fixture.sourceSequence(),
                                        fixture.deterministicId()))
                        .toList();
                List<Comparable> ordered = new ArrayList<>();
                for (var future : pool.invokeAll(tasks)) {
                    ordered.add(future.get());
                }
                Collections.sort(ordered);

                List<Long> sequences = new ArrayList<>();
                for (Object value : ordered) {
                    sequences.add(sourceSequence(value));
                }
                assertEquals(List.of(
                        101349630974151L,
                        546777574496647L,
                        570400385456073L
                ), sequences);
            }
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void previousMovementQueryUsesEconomicOrderWithoutDateCreation() throws Exception {
        Method method = OperationMovementEventRepository.class.getMethod(
                "findPreviousByEconomicOrder",
                String.class,
                OffsetDateTime.class,
                Long.class,
                String.class
        );
        Query query = method.getAnnotation(Query.class);

        assertNotNull(query);
        String sql = query.value().toLowerCase();
        assertTrue(sql.contains("event_time"));
        assertTrue(sql.contains("source_sequence"));
        assertTrue(sql.contains("movement_key"));
        assertFalse(sql.contains("date_creation"));
    }

    private Long sourceSequence(Object value) throws Exception {
        return (Long) value.getClass().getMethod("sourceSequence").invoke(value);
    }

    private record FillFixture(
            OffsetDateTime eventTime,
            Long sourceSequence,
            String sourceEventId,
            String deterministicId,
            BigDecimal quantity,
            BigDecimal executionPrice
    ) {
    }
}
