package com.apunto.engine.service.copy.dispatch;

import com.apunto.engine.dto.OperationDto;
import com.apunto.engine.dto.client.BinanceFuturesMarketPriceClientDto;
import com.apunto.engine.dto.client.BinanceFuturesOrderClientResponse;
import com.apunto.engine.dto.client.BinanceFuturesPositionClientDto;
import com.apunto.engine.dto.client.BinanceFuturesSymbolFilterDto;
import com.apunto.engine.dto.client.BinanceFuturesSymbolInfoClientDto;
import com.apunto.engine.entity.UserCopyAllocationEntity;
import com.apunto.engine.service.ProcesBinanceService;
import com.apunto.engine.service.copy.calibration.CopyNotionalBandPolicy;
import com.apunto.engine.service.copy.filter.BinanceOrderFilterPolicy;
import com.apunto.engine.shared.enums.OrderType;
import com.apunto.engine.shared.enums.PositionSide;
import com.apunto.engine.shared.enums.Side;
import com.apunto.engine.shared.exception.SkipExecutionException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BinanceOrderRuntimePreflightIntegrationTest {

    @ParameterizedTest
    @ValueSource(strings = {"MICRO_LIVE", "LIVE"})
    void blockedRealPolicyPersistsReasonAndNeverCallsOrderHttpInEitherMode(String mode) {
        RecordingStore store = new RecordingStore();
        StubBinanceGateway gateway = new StubBinanceGateway();
        BinanceOrderRuntimePreflight preflight = new BinanceOrderRuntimePreflight(
                gateway, new BinanceOrderFilterPolicy());
        CopyDispatchCoordinator coordinator = new CopyDispatchCoordinator(
                store, gateway, new BinanceOrderExecutionNormalizer(), new CopyIdempotencyKeyFactory(),
                new io.micrometer.core.instrument.simple.SimpleMeterRegistry(),
                new CopyNotionalBandPolicy(new BigDecimal("100"), new BigDecimal("1000"),
                        new BigDecimal("10000")), preflight);
        UUID accountId = UUID.randomUUID();
        OperationDto operation = operation(mode, accountId);
        UserCopyAllocationEntity allocation = allocation(mode, accountId);

        SkipExecutionException blocked = assertThrows(SkipExecutionException.class,
                () -> coordinator.dispatch(operation, allocation, new BigDecimal("1000"), "trace"));

        assertEquals("BINANCE_NOTIONAL_BELOW_MIN", blocked.getReasonCode());
        assertEquals("BINANCE_NOTIONAL_BELOW_MIN", store.rejectedReason);
        assertEquals(1, gateway.exchangeInfoCalls.get());
        assertEquals(0, gateway.orderCalls.get());
    }

    private static OperationDto operation(String mode, UUID accountId) {
        return OperationDto.builder()
                .symbol("ETHUSDC")
                .side(Side.BUY)
                .type(OrderType.MARKET)
                .positionSide(PositionSide.LONG)
                .quantity("0.001")
                .leverage(5)
                .reduceOnly(false)
                .configureAccountSettings(true)
                .originId("origin-hot-path")
                .sourceEventId("event-hot-path-" + mode)
                .copyIntent("OPEN")
                .requestedMarginUsd(new BigDecimal("0.2"))
                .requestedNotionalUsd(BigDecimal.ONE)
                .referencePrice(new BigDecimal("1000"))
                .reservePosition(true)
                .userId(UUID.randomUUID().toString())
                .walletId("0xabc")
                .apiKey("test-key")
                .secret("test-secret")
                .exchangeAccountId(accountId)
                .accountPurpose(mode)
                .fixedMarginMode("CROSS")
                .fixedPositionMode("ONE_WAY")
                .quoteAsset("USDC")
                .build();
    }

    private static UserCopyAllocationEntity allocation(String mode, UUID accountId) {
        return UserCopyAllocationEntity.builder()
                .id(77L)
                .idUser(UUID.randomUUID())
                .walletId("0xabc")
                .copyStrategyCode("MOVEMENT_ALL")
                .scopeType("ALL")
                .scopeValue("ALL")
                .strategyKey("0xabc|MOVEMENT_ALL|ALL|ALL")
                .metricGenerationId(UUID.randomUUID().toString())
                .executionMode(mode)
                .exchangeAccountId(accountId)
                .status(UserCopyAllocationEntity.Status.ACTIVE)
                .isActive(true)
                .build();
    }

    private static final class RecordingStore implements CopyDispatchIntentStore {
        private final UUID id = UUID.randomUUID();
        private String rejectedReason;

        @Override public CopyDispatchPermit acquire(CopyDispatchRequest request) { return CopyDispatchPermit.send(id); }
        @Override public void acknowledge(UUID intentId, NormalizedBinanceExecution execution,
                                          BinanceFuturesOrderClientResponse response) { }
        @Override public void markAmbiguous(UUID intentId, String reasonCode, String detail) { }
        @Override public void markRejected(UUID intentId, String reasonCode, String detail) {
            rejectedReason = reasonCode;
        }
        @Override public void markPersistencePending(String clientOrderId, String reasonCode, String detail) { }
    }

    private static final class StubBinanceGateway implements ProcesBinanceService {
        private final AtomicInteger exchangeInfoCalls = new AtomicInteger();
        private final AtomicInteger orderCalls = new AtomicInteger();

        @Override public BinanceFuturesOrderClientResponse operationPosition(OperationDto dto) {
            orderCalls.incrementAndGet();
            return new BinanceFuturesOrderClientResponse();
        }

        @Override public List<BinanceFuturesSymbolInfoClientDto> getSymbols(String apiKey) {
            exchangeInfoCalls.incrementAndGet();
            BinanceFuturesSymbolInfoClientDto symbol = new BinanceFuturesSymbolInfoClientDto();
            symbol.setSymbol("ETHUSDC");
            symbol.setStatus("TRADING");
            symbol.setQuoteAsset("USDC");
            symbol.setMarginAsset("USDC");
            symbol.setQuantityPrecision(3);
            symbol.setOrderTypes(List.of("MARKET"));
            BinanceFuturesSymbolFilterDto marketLot = new BinanceFuturesSymbolFilterDto();
            marketLot.setFilterType("MARKET_LOT_SIZE");
            marketLot.setStepSize("0.001");
            marketLot.setMinQty("0.001");
            marketLot.setMaxQty("100");
            BinanceFuturesSymbolFilterDto notional = new BinanceFuturesSymbolFilterDto();
            notional.setFilterType("MIN_NOTIONAL");
            notional.setNotional("5");
            symbol.setFilters(List.of(marketLot, notional));
            return List.of(symbol);
        }

        @Override public Optional<BinanceFuturesOrderClientResponse> findOrderByClientOrderId(OperationDto dto) {
            return Optional.empty();
        }
        @Override public List<BinanceFuturesPositionClientDto> getPositions(
                String apiKey, String secret, String traceId) { return List.of(); }
        @Override public Optional<BinanceFuturesMarketPriceClientDto> getMarketPrice(
                String symbol, String usage, boolean allowStale) { return Optional.empty(); }
    }
}
