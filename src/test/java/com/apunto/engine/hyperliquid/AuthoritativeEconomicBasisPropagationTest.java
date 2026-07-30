package com.apunto.engine.hyperliquid;

import com.apunto.engine.dto.OperationMovementEventRecordCommand;
import com.apunto.engine.entity.OperationMovementEventEntity;
import com.apunto.engine.hyperliquid.dto.HyperliquidDeltaRequest;
import com.apunto.engine.hyperliquid.dto.HyperliquidDirectCopyDispatchResult;
import com.apunto.engine.hyperliquid.dto.HyperliquidMappedDelta;
import com.apunto.engine.hyperliquid.mapper.HyperliquidDeltaOperacionMapper;
import com.apunto.engine.outbox.dto.MetricMovementPersistedEvent;
import com.apunto.engine.outbox.service.MetricMovementOutboxService;
import com.apunto.engine.outbox.service.impl.MetricMovementOutboxServiceImpl;
import com.apunto.engine.repository.OperationMovementEventRepository;
import com.apunto.engine.service.impl.OperationMovementEventServiceImpl;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.SimpleTransactionStatus;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AuthoritativeEconomicBasisPropagationTest {
    private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();

    @Test
    void reduceAndClosePreserveAuthoritativeBasisThroughSignalsKafkaPayload()
            throws Exception {
        JsonNode fixtures;
        try (var input = getClass().getResourceAsStream(
                "/fixtures/economic/authoritative-user-fills-sentinel-sanitized.json")) {
            assertNotNull(input);
            fixtures = mapper.readTree(input);
        }

        assertEquals(2, fixtures.size());
        List<MetricMovementPersistedEvent> serializedEvents =
                new ArrayList<>();
        for (JsonNode fixture : fixtures) {
            HyperliquidDeltaRequest request =
                    mapper.treeToValue(fixture, HyperliquidDeltaRequest.class);

            JsonNode requestRoundTrip = mapper.valueToTree(request);
            assertEquals(
                    fixture.path("economicFingerprint").asText(),
                    requestRoundTrip.path("economicFingerprint").asText(),
                    "Sentinel economic fingerprint at Signals ingress");

            HyperliquidMappedDelta mapped =
                    new HyperliquidDeltaOperacionMapper().map(
                            request, request.idempotencyKey());
            OperationMovementEventRecordCommand command =
                    command(service(), mapped);
            OperationMovementEventEntity previous =
                    previous(fixture.path("sourceSequence").longValue());
            OperationMovementEventEntity entity =
                    entity(service(), command, previous);

            assertEquals(0, command.getEffectiveCloseQty().compareTo(
                    fixture.path("effectiveCloseQty").decimalValue()));
            assertEquals(0, command.getEffectiveExitPrice().compareTo(
                    fixture.path("effectiveExitPrice").decimalValue()));
            assertEquals(fixture.path("sourceEventId").asText(),
                    command.getSourceEventId());
            assertEquals(fixture.path("sourceSequence").longValue(),
                    command.getSourceSequence());
            assertFalse(command.getSourceEstimated());
            assertEquals("USER_FILL", command.getEconomicEventKind());
            assertEquals(
                    fixture.path("sourceSequence").longValue() == 8103L
                            ? "REDUCE"
                            : "CLOSE",
                    entity.getEventType());
            assertEquals("COMPLETE", economicBasisStatus(service(), entity));
            assertEquals(
                    fixture.path("economicFingerprint").asText(),
                    entity.getRaw().path("request")
                            .path("economicFingerprint").asText());

            MetricMovementPersistedEvent kafkaEvent = kafkaEvent(entity);
            JsonNode kafkaPayload = mapper.valueToTree(kafkaEvent);
            assertEquals(0, kafkaPayload.path("effectiveCloseQty").decimalValue()
                    .compareTo(fixture.path("effectiveCloseQty").decimalValue()));
            assertEquals(0, kafkaPayload.path("effectiveExitPrice").decimalValue()
                    .compareTo(fixture.path("effectiveExitPrice").decimalValue()));
            assertEquals(fixture.path("sourceEventId").asText(),
                    kafkaPayload.path("sourceEventId").asText());
            assertEquals(fixture.path("sourceSequence").longValue(),
                    kafkaPayload.path("sourceSequence").longValue());
            assertEquals(fixture.path("economicFingerprint").asText(),
                    kafkaPayload.path("sourceEconomicFingerprint").asText());
            serializedEvents.add(kafkaEvent);
        }
        writeB2bPayloadWhenRequested(serializedEvents);
    }

    @Test
    void kafkaContractNamesSourceFingerprintExplicitly() {
        Set<String> components = Arrays.stream(
                        MetricMovementPersistedEvent.class.getRecordComponents())
                .map(component -> component.getName())
                .collect(Collectors.toSet());

        assertTrue(components.contains("sourceEconomicFingerprint"));
    }

    private void writeB2bPayloadWhenRequested(
            List<MetricMovementPersistedEvent> events
    ) throws Exception {
        String configured = System.getProperty("economicBasisB2bOutput");
        if (configured == null || configured.isBlank()) {
            return;
        }
        Path output = Path.of(configured).toAbsolutePath().normalize();
        Path parent = output.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        mapper.writerWithDefaultPrettyPrinter()
                .writeValue(output.toFile(), events);
    }

    private OperationMovementEventRecordCommand command(
            OperationMovementEventServiceImpl service,
            HyperliquidMappedDelta mapped
    ) throws Exception {
        Method method = OperationMovementEventServiceImpl.class.getDeclaredMethod(
                "fromMappedDelta",
                HyperliquidMappedDelta.class,
                HyperliquidDirectCopyDispatchResult.class,
                String.class);
        method.setAccessible(true);
        return (OperationMovementEventRecordCommand) method.invoke(
                service,
                mapped,
                HyperliquidDirectCopyDispatchResult.ok(
                        0, 0, 0, 0, false, "authoritative_user_fill"),
                "authoritative_user_fill");
    }

    private OperationMovementEventEntity entity(
            OperationMovementEventServiceImpl service,
            OperationMovementEventRecordCommand command,
            OperationMovementEventEntity previous
    ) throws Exception {
        Method method = OperationMovementEventServiceImpl.class.getDeclaredMethod(
                "toEntity",
                OperationMovementEventRecordCommand.class,
                OperationMovementEventEntity.class);
        method.setAccessible(true);
        return (OperationMovementEventEntity) method.invoke(
                service, command, previous);
    }

    private OperationMovementEventEntity previous(long sourceSequence) {
        return OperationMovementEventEntity.builder()
                .resultingSizeQty(sourceSequence == 8103L
                        ? new BigDecimal("2.0")
                        : new BigDecimal("1.5"))
                .entryPrice(new BigDecimal("1923.20"))
                .typeOperation("LONG")
                .eventTime(java.time.OffsetDateTime.parse(
                        "2026-07-23T05:18:52.601Z"))
                .sourceSequence(sourceSequence - 1L)
                .movementKey("movement|sha256:"
                        + "0".repeat(63) + (sourceSequence == 8103L ? "3" : "4"))
                .build();
    }

    private String economicBasisStatus(
            OperationMovementEventServiceImpl service,
            OperationMovementEventEntity entity
    ) throws Exception {
        Method method = OperationMovementEventServiceImpl.class
                .getDeclaredMethod(
                        "economicBasisStatus",
                        OperationMovementEventEntity.class);
        method.setAccessible(true);
        return (String) method.invoke(service, entity);
    }

    private MetricMovementPersistedEvent kafkaEvent(
            OperationMovementEventEntity entity
    ) throws Exception {
        MetricMovementOutboxServiceImpl outbox =
                new MetricMovementOutboxServiceImpl(null, mapper);
        Method method = MetricMovementOutboxServiceImpl.class.getDeclaredMethod(
                "toEvent", OperationMovementEventEntity.class);
        method.setAccessible(true);
        return (MetricMovementPersistedEvent) method.invoke(outbox, entity);
    }

    private OperationMovementEventServiceImpl service() {
        OperationMovementEventRepository repository =
                (OperationMovementEventRepository) Proxy.newProxyInstance(
                        OperationMovementEventRepository.class.getClassLoader(),
                        new Class<?>[]{OperationMovementEventRepository.class},
                        (proxy, method, args) -> defaultValue(
                                method.getReturnType()));
        MetricMovementOutboxService outbox = ignored -> { };
        return new OperationMovementEventServiceImpl(
                repository,
                mapper,
                outbox,
                new NoopTransactionManager(),
                new SimpleMeterRegistry(),
                false,
                1,
                1);
    }

    private Object defaultValue(Class<?> type) {
        if (!type.isPrimitive()) {
            return null;
        }
        if (type == boolean.class) {
            return false;
        }
        if (type == int.class || type == short.class || type == byte.class
                || type == long.class) {
            return 0;
        }
        if (type == float.class || type == double.class) {
            return 0.0;
        }
        return null;
    }

    private static final class NoopTransactionManager
            implements PlatformTransactionManager {
        @Override
        public TransactionStatus getTransaction(
                TransactionDefinition definition
        ) {
            return new SimpleTransactionStatus();
        }

        @Override
        public void commit(TransactionStatus status) {
        }

        @Override
        public void rollback(TransactionStatus status) {
        }
    }
}
