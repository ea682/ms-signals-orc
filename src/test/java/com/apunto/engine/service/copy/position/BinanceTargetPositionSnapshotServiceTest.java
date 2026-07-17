package com.apunto.engine.service.copy.position;

import com.apunto.copytarget.SourceSide;
import com.apunto.copytarget.TargetPositionSnapshotStatus;
import com.apunto.engine.dto.OperationDto;
import com.apunto.engine.dto.UserDetailDto;
import com.apunto.engine.dto.client.BinanceFuturesMarketPriceClientDto;
import com.apunto.engine.dto.client.BinanceFuturesOrderClientResponse;
import com.apunto.engine.dto.client.BinanceFuturesPositionClientDto;
import com.apunto.engine.dto.client.BinanceFuturesSymbolInfoClientDto;
import com.apunto.engine.entity.UserApiKeyEntity;
import com.apunto.engine.entity.UserEntity;
import com.apunto.engine.service.ProcesBinanceService;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BinanceTargetPositionSnapshotServiceTest {

    @Test
    void mapsHedgeAndOneWayPositionsAndIgnoresFlatRows() {
        FakeGateway gateway = new FakeGateway(List.of(
                position("BTCUSDT", "0.10", "LONG"),
                position("ETHUSDT", "-2.5", "SHORT"),
                position("SOLUSDT", "3", "BOTH"),
                position("XRPUSDT", "-4", "BOTH"),
                position("ADAUSDT", "0", "LONG")
        ));
        BinanceTargetPositionSnapshotService service = new BinanceTargetPositionSnapshotService(gateway);

        BinanceTargetPositionSnapshot snapshot = service.load(user(), "trace-1");

        assertEquals(TargetPositionSnapshotStatus.AUTHORITATIVE, snapshot.status());
        assertEquals(4, snapshot.positions().size());
        assertEquals(SourceSide.LONG, snapshot.positions().stream()
                .filter(value -> value.symbol().equals("SOLUSDT")).findFirst().orElseThrow().side());
        assertEquals(SourceSide.SHORT, snapshot.positions().stream()
                .filter(value -> value.symbol().equals("XRPUSDT")).findFirst().orElseThrow().side());
    }

    @Test
    void cacheAvoidsDuplicateReadsAndRealDispatchInvalidationForcesRefresh() {
        FakeGateway gateway = new FakeGateway(List.of());
        BinanceTargetPositionSnapshotService service = new BinanceTargetPositionSnapshotService(gateway);
        service.setCachePolicyForTest(10_000L, 10_000L);
        UserDetailDto user = user();

        service.load(user, "trace-1");
        service.load(user, "trace-2");
        assertEquals(1, gateway.calls.get());

        service.invalidate(user.getUser().getId().toString(), "REAL_DISPATCH_ATTEMPTED");
        service.load(user, "trace-3");
        assertEquals(2, gateway.calls.get());
    }

    @Test
    void failedFirstReadIsUnavailableAndFailedRefreshKeepsOnlyStaleExitEvidence() throws Exception {
        FakeGateway unavailableGateway = new FakeGateway(new IllegalStateException("down"));
        BinanceTargetPositionSnapshot unavailable =
                new BinanceTargetPositionSnapshotService(unavailableGateway).load(user(), "trace-down");
        assertEquals(TargetPositionSnapshotStatus.UNAVAILABLE, unavailable.status());
        assertEquals(BinanceTargetPositionSnapshotService.UNAVAILABLE, unavailable.reasonCode());

        FakeGateway staleGateway = new FakeGateway(List.of(position("BTCUSDT", "0.10", "LONG")));
        BinanceTargetPositionSnapshotService service = new BinanceTargetPositionSnapshotService(staleGateway);
        service.setCachePolicyForTest(0L, 0L);
        UserDetailDto user = user();
        service.load(user, "trace-ok");
        Thread.sleep(2L);
        staleGateway.failure = new IllegalStateException("refresh failed");

        BinanceTargetPositionSnapshot stale = service.load(user, "trace-stale");

        assertEquals(TargetPositionSnapshotStatus.STALE, stale.status());
        assertEquals(1, stale.positions().size());
        assertEquals(BinanceTargetPositionSnapshotService.STALE, stale.reasonCode());
    }

    private static BinanceFuturesPositionClientDto position(String symbol, String quantity, String side) {
        BinanceFuturesPositionClientDto value = new BinanceFuturesPositionClientDto();
        value.setSymbol(symbol);
        value.setPositionAmt(quantity);
        value.setPositionSide(side);
        value.setMarkPrice("100");
        value.setIsolatedMargin("0");
        return value;
    }

    private static UserDetailDto user() {
        UserEntity user = new UserEntity();
        user.setId(UUID.randomUUID());
        UserApiKeyEntity key = new UserApiKeyEntity();
        key.setApiKey("api-key");
        key.setApiSecret("secret");
        return new UserDetailDto(user, null, key);
    }

    private static final class FakeGateway implements ProcesBinanceService {
        private final AtomicInteger calls = new AtomicInteger();
        private final List<BinanceFuturesPositionClientDto> positions;
        private RuntimeException failure;

        private FakeGateway(List<BinanceFuturesPositionClientDto> positions) {
            this.positions = positions;
        }

        private FakeGateway(RuntimeException failure) {
            this.positions = List.of();
            this.failure = failure;
        }

        @Override
        public List<BinanceFuturesPositionClientDto> getPositions(String apiKey, String secret, String traceId) {
            calls.incrementAndGet();
            if (failure != null) throw failure;
            return positions;
        }

        @Override
        public BinanceFuturesOrderClientResponse operationPosition(OperationDto dto) {
            throw new UnsupportedOperationException("not used");
        }

        @Override
        public Optional<BinanceFuturesOrderClientResponse> findOrderByClientOrderId(OperationDto dto) {
            return Optional.empty();
        }

        @Override
        public List<BinanceFuturesSymbolInfoClientDto> getSymbols(String apiKey) {
            return List.of();
        }

        @Override
        public Optional<BinanceFuturesMarketPriceClientDto> getMarketPrice(
                String symbol, String usage, boolean allowStale
        ) {
            return Optional.empty();
        }
    }
}
