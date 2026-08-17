package com.apunto.engine.hyperliquid.listener;

import com.apunto.engine.hyperliquid.dto.HyperliquidDeltaAcceptedResponse;
import com.apunto.engine.hyperliquid.dto.HyperliquidDeltaRequest;
import com.apunto.engine.hyperliquid.dto.HyperliquidMappedDelta;
import com.apunto.engine.hyperliquid.mapper.HyperliquidDeltaOperacionMapper;
import com.apunto.engine.hyperliquid.metric.HyperliquidIngestTransportMetrics;
import com.apunto.engine.hyperliquid.service.HyperliquidDirectDeltaIngestService;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.validation.Validation;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HyperliquidPositionDeltaKafkaListenerTest {

    private SimpleMeterRegistry registry;
    private RecordingIngestService ingestService;
    private HyperliquidPositionDeltaKafkaListener listener;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        ingestService = new RecordingIngestService();
        listener = new HyperliquidPositionDeltaKafkaListener(
                new HyperliquidDeltaOperacionMapper(registry),
                ingestService,
                Validation.buildDefaultValidatorFactory().getValidator(),
                new HyperliquidIngestTransportMetrics(registry));
    }

    @Test
    void kafkaRecordUsesTheSameCanonicalMapperAndIngestServiceBeforeAck() throws Exception {
        HyperliquidDeltaRequest request = request("body-key");
        RecordingAcknowledgment acknowledgment = new RecordingAcknowledgment(ingestService.sequence);
        ConsumerRecord<String, HyperliquidDeltaRequest> record =
                new ConsumerRecord<>("hyperliquid-position-deltas", 1, 42L, "source-key", request);

        listener.listen(record, acknowledgment);

        assertEquals("source-key", ingestService.accepted.idempotencyKey());
        assertEquals(List.of("ingest", "ack"), ingestService.sequence);
        assertEquals(1.0d, registry.find("signals.hyperliquid.kafka_ingest.received.total")
                .counter().count());
        assertEquals(1.0d, registry.find("signals.hyperliquid.kafka_ingest.accepted.total")
                .tag("result", "accepted").counter().count());
    }

    @Test
    void ambiguousHttpAckKafkaReplayIsAcknowledgedAsDuplicateWithoutAnotherEngine() throws Exception {
        ingestService.duplicate = true;
        RecordingAcknowledgment acknowledgment = new RecordingAcknowledgment(ingestService.sequence);
        ConsumerRecord<String, HyperliquidDeltaRequest> record =
                new ConsumerRecord<>("hyperliquid-position-deltas", 0, 7L, "same-source-key", request("same-source-key"));

        listener.listen(record, acknowledgment);

        assertTrue(acknowledgment.acknowledged);
        assertEquals(1, ingestService.calls);
        assertEquals(1.0d, registry.find("signals.hyperliquid.kafka_ingest.accepted.total")
                .tag("result", "duplicate").counter().count());
    }

    @Test
    void processingFailureDoesNotCommitKafkaOffset() throws Exception {
        ingestService.failure = new IllegalStateException("ingest unavailable");
        RecordingAcknowledgment acknowledgment = new RecordingAcknowledgment(ingestService.sequence);
        ConsumerRecord<String, HyperliquidDeltaRequest> record =
                new ConsumerRecord<>("hyperliquid-position-deltas", 0, 8L, "source-key", request("source-key"));

        assertThrows(IllegalStateException.class, () -> listener.listen(record, acknowledgment));

        assertFalse(acknowledgment.acknowledged);
        assertEquals(1.0d, registry.find("signals.hyperliquid.kafka_ingest.failed.total")
                .tag("reason", "ingest_error").counter().count());
    }

    @Test
    void nullKafkaValueIsRejectedOnceAsValidationFailureWithoutAck() {
        RecordingAcknowledgment acknowledgment = new RecordingAcknowledgment(ingestService.sequence);
        ConsumerRecord<String, HyperliquidDeltaRequest> record =
                new ConsumerRecord<>("hyperliquid-position-deltas", 0, 9L, "source-key", null);

        assertThrows(IllegalArgumentException.class, () -> listener.listen(record, acknowledgment));

        assertFalse(acknowledgment.acknowledged);
        assertEquals(0, ingestService.calls);
        assertEquals(1.0d, registry.find("signals.hyperliquid.kafka_ingest.failed.total")
                .tag("reason", "validation_error").counter().count());
        assertNull(registry.find("signals.hyperliquid.kafka_ingest.failed.total")
                .tag("reason", "ingest_error").counter());
    }

    private HyperliquidDeltaRequest request(String idempotencyKey) throws Exception {
        return new ObjectMapper().findAndRegisterModules().readValue("""
                {
                  "eventId":"event-1",
                  "idempotencyKey":"%s",
                  "eventType":"HYPERLIQUID_POSITION_RESIZED",
                  "deltaType":"RESIZE",
                  "platform":"hyperliquid",
                  "wallet":"0xabc",
                  "symbol":"BTC",
                  "side":"LONG",
                  "status":"OPEN",
                  "sizeQty":1,
                  "notionalUsd":100,
                  "sourceTs":1785585600000,
                  "sourceSequence":991,
                  "economicEventKind":"USER_FILL",
                  "sourceEstimated":false,
                  "sourceDeliveryMode":"LIVE_USER_FILL"
                }
                """.formatted(idempotencyKey), HyperliquidDeltaRequest.class);
    }

    private static final class RecordingIngestService implements HyperliquidDirectDeltaIngestService {
        private final List<String> sequence = new ArrayList<>();
        private HyperliquidMappedDelta accepted;
        private boolean duplicate;
        private RuntimeException failure;
        private int calls;

        @Override
        public HyperliquidDeltaAcceptedResponse accept(HyperliquidMappedDelta mappedDelta) {
            calls++;
            sequence.add("ingest");
            if (failure != null) {
                throw failure;
            }
            accepted = mappedDelta;
            return HyperliquidDeltaAcceptedResponse.accepted(
                    mappedDelta.idempotencyKey(), mappedDelta.positionKey(), mappedDelta.wallet(),
                    mappedDelta.symbol(), mappedDelta.side(), mappedDelta.deltaType(), duplicate, 0);
        }
    }

    private static final class RecordingAcknowledgment implements Acknowledgment {
        private final List<String> sequence;
        private boolean acknowledged;

        private RecordingAcknowledgment(List<String> sequence) {
            this.sequence = sequence;
        }

        @Override
        public void acknowledge() {
            acknowledged = true;
            sequence.add("ack");
        }
    }
}
