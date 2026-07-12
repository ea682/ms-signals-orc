package com.apunto.engine.service.copy.recovery;

import com.apunto.engine.dto.OperacionDto;
import com.apunto.engine.events.OperacionEvent;
import com.apunto.engine.hyperliquid.dto.HyperliquidMappedDelta;
import com.apunto.engine.service.ShadowCopyTradingService;
import com.apunto.engine.shared.enums.PositionSide;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ShadowEventDeadLetterReplayWorkerTest {

    @Test
    void successfulReplayMarksDurableItemResolved() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();
        FakeStore store = new FakeStore(item(objectMapper));
        AtomicInteger calls = new AtomicInteger();
        ShadowEventDeadLetterReplayWorker worker = new ShadowEventDeadLetterReplayWorker(
                store,
                shadowService(event -> {
                    calls.incrementAndGet();
                    return 1;
                }),
                objectMapper,
                new SimpleMeterRegistry(),
                10,
                60_000L
        );

        worker.replay();

        assertEquals(1, calls.get());
        assertTrue(store.resolved);
        assertFalse(store.replayFailed);
    }

    @Test
    void failedReplayReturnsItemToRecoverableState() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();
        FakeStore store = new FakeStore(item(objectMapper));
        ShadowEventDeadLetterReplayWorker worker = new ShadowEventDeadLetterReplayWorker(
                store,
                shadowService(event -> {
                    throw new IllegalStateException("temporary database failure");
                }),
                objectMapper,
                new SimpleMeterRegistry(),
                10,
                60_000L
        );

        worker.replay();

        assertFalse(store.resolved);
        assertTrue(store.replayFailed);
    }

    private static ShadowEventDeadLetterStore.DeadLetterItem item(ObjectMapper objectMapper) throws Exception {
        OperacionEvent event = new OperacionEvent(
                OperacionEvent.Tipo.ABIERTA,
                OperacionDto.builder()
                        .idOperacion(java.util.UUID.randomUUID())
                        .idCuenta("0xabc")
                        .parSymbol("BTCUSDT")
                        .tipoOperacion(PositionSide.LONG)
                        .sizeQty(BigDecimal.ONE)
                        .precioEntrada(new BigDecimal("100"))
                        .fechaCreacion(Instant.parse("2026-07-11T12:00:00Z"))
                        .build()
        );
        event.setDeltaType("OPEN");
        HyperliquidMappedDelta mappedDelta = new HyperliquidMappedDelta(
                "shadow-replay-1", "0xabc|BTCUSDT|LONG", "0xabc", "BTCUSDT", "LONG", "OPEN", event, null);
        return new ShadowEventDeadLetterStore.DeadLetterItem(
                "shadow-replay-1", objectMapper.writeValueAsString(mappedDelta), 1);
    }

    private static ShadowCopyTradingService shadowService(Recorder recorder) {
        return new ShadowCopyTradingService() {
            @Override
            public void syncShadowAllocations(java.util.UUID idUser, List<com.apunto.engine.dto.client.MetricaWalletDto> candidates, int userMaxWallet, java.time.OffsetDateTime now) {
            }

            @Override
            public void linkLiveAllocations(java.util.UUID idUser, List<com.apunto.engine.entity.UserCopyAllocationEntity> liveAllocations) {
            }

            @Override
            public int recordShadowEvent(OperacionEvent event) {
                return recorder.record(event);
            }

            @Override
            public boolean isSeparateShadowEnabled() {
                return true;
            }

            @Override
            public boolean isLivePromotable(java.util.UUID idUser, com.apunto.engine.dto.client.MetricaWalletDto candidate) {
                return false;
            }
        };
    }

    private static final class FakeStore extends ShadowEventDeadLetterStore {
        private final List<DeadLetterItem> items = new ArrayList<>();
        private boolean resolved;
        private boolean replayFailed;

        private FakeStore(DeadLetterItem item) {
            super(new JdbcTemplate(), new ObjectMapper(), new SimpleMeterRegistry());
            items.add(item);
        }

        @Override
        public List<DeadLetterItem> claimRecoverable(int limit, long replayLeaseMs) {
            return List.copyOf(items);
        }

        @Override
        public void markResolved(String idempotencyKey) {
            resolved = true;
        }

        @Override
        public void markReplayFailed(String idempotencyKey, Throwable failure) {
            replayFailed = true;
        }
    }

    @FunctionalInterface
    private interface Recorder {
        int record(OperacionEvent event);
    }
}
