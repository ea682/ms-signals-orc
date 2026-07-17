package com.apunto.engine.service.impl;

import com.apunto.engine.client.BinanceClient;
import com.apunto.engine.dto.OperationDto;
import com.apunto.engine.dto.client.BinanceFuturesMarketPriceClientDto;
import com.apunto.engine.dto.client.BinanceFuturesOrderClientResponse;
import com.apunto.engine.dto.client.BinanceFuturesPositionClientDto;
import com.apunto.engine.dto.client.BinanceFuturesSymbolInfoClientDto;
import com.apunto.engine.dto.client.BinanceOrderBookSnapshotClientDto;
import com.apunto.engine.dto.client.CloseOperationClientRequest;
import com.apunto.engine.dto.client.FuturesAssetBalanceClientResponse;
import com.apunto.engine.dto.client.FuturesConvertToBnbClientRequest;
import com.apunto.engine.dto.client.FuturesConvertToBnbClientResponse;
import com.apunto.engine.dto.client.NewOperationClientRequest;
import com.apunto.engine.dto.client.TradingConfigPreconfigureClientRequest;
import com.apunto.engine.dto.client.TradingConfigPreconfigureClientResponse;
import com.apunto.engine.shared.dto.ApiResponse;
import com.apunto.engine.shared.enums.OrderType;
import com.apunto.engine.shared.enums.PositionSide;
import com.apunto.engine.shared.enums.Side;
import com.apunto.engine.shared.exception.BinanceApiReadinessException;
import com.apunto.engine.shared.exception.SkipExecutionException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClientResponseException;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProcesBinanceServiceImplTest {

    @Test
    void reduceOnlyMarketOrderUsesDedicatedCloseEndpoint() {
        CapturingBinanceClient standardClient = new CapturingBinanceClient();
        CapturingBinanceClient closeClient = new CapturingBinanceClient();
        ProcesBinanceServiceImpl service = new ProcesBinanceServiceImpl(standardClient, closeClient, new ObjectMapper());
        service.setOrderSubmitEnabledForTest(true);

        service.operationPosition(OperationDto.builder()
                .symbol("BTCUSDT")
                .side(Side.SELL)
                .type(OrderType.MARKET)
                .positionSide(PositionSide.LONG)
                .quantity("0.01")
                .reduceOnly(true)
                .clientOrderId("close-123")
                .apiKey("api-key")
                .secret("secret")
                .build());

        assertTrue(closeClient.closeCalled);
        assertNull(standardClient.lastRequest);
        assertEquals("BTCUSDT", closeClient.lastCloseRequest.getSymbol());
        assertEquals(new BigDecimal("0.01"), closeClient.lastCloseRequest.getOperationQty());
        assertEquals(PositionSide.LONG, closeClient.lastCloseRequest.getPositionSide());
        assertEquals("close-123", closeClient.lastCloseRequest.getClientOrderId());
    }

    @Test
    void regularOpenKeepsAccountSettingsUnsetWhenCallerDoesNotOverride() {
        CapturingBinanceClient client = new CapturingBinanceClient();
        CapturingBinanceClient closeClient = new CapturingBinanceClient();
        ProcesBinanceServiceImpl service = new ProcesBinanceServiceImpl(client, closeClient, new ObjectMapper());
        service.setOrderSubmitEnabledForTest(true);

        service.operationPosition(OperationDto.builder()
                .symbol("BTCUSDT")
                .side(Side.BUY)
                .type(OrderType.MARKET)
                .positionSide(PositionSide.LONG)
                .quantity("0.01")
                .reduceOnly(false)
                .apiKey("api-key")
                .secret("secret")
                .build());

        assertEquals(Boolean.FALSE, client.lastRequest.getReduceOnly());
        assertNull(client.lastRequest.getConfigureAccountSettings());
        assertNull(closeClient.lastCloseRequest);
    }

    @Test
    void orderSubmitDisabledBlocksPrivateBinanceCalls() {
        CapturingBinanceClient client = new CapturingBinanceClient();
        CapturingBinanceClient closeClient = new CapturingBinanceClient();
        ProcesBinanceServiceImpl service = new ProcesBinanceServiceImpl(client, closeClient, new ObjectMapper());
        service.setOrderSubmitEnabledForTest(false);

        SkipExecutionException ex = assertThrows(SkipExecutionException.class, () ->
                service.operationPosition(OperationDto.builder()
                        .symbol("BTCUSDT")
                        .side(Side.BUY)
                        .type(OrderType.MARKET)
                        .positionSide(PositionSide.LONG)
                        .quantity("0.01")
                        .reduceOnly(false)
                        .apiKey("api-key")
                        .secret("secret")
                        .build()));

        assertEquals("binance_order_submit_disabled", ex.getReasonCode());
        assertNull(client.lastRequest);
        assertFalse(closeClient.closeCalled);
    }

    @Test
    void symbolsCanBeLoadedWithoutApiKey() {
        CapturingBinanceClient client = new CapturingBinanceClient();
        CapturingBinanceClient closeClient = new CapturingBinanceClient();
        ProcesBinanceServiceImpl service = new ProcesBinanceServiceImpl(client, closeClient, new ObjectMapper());

        List<BinanceFuturesSymbolInfoClientDto> symbols = service.getSymbols(null);

        assertNull(client.lastSymbolsApiKey);
        assertEquals(1, symbols.size());
        assertEquals("BTCUSDT", symbols.get(0).getSymbol());
    }

    @Test
    void authoritativePositionsPreserveSignedQuantityAndPositionSide() {
        CapturingBinanceClient client = new CapturingBinanceClient();
        CapturingBinanceClient closeClient = new CapturingBinanceClient();
        BinanceFuturesPositionClientDto position = new BinanceFuturesPositionClientDto();
        position.setSymbol("BTCUSDT");
        position.setPositionAmt("-0.125");
        position.setPositionSide("SHORT");
        position.setMarkPrice("65000");
        client.positionsResponse = List.of(position);
        ProcesBinanceServiceImpl service = new ProcesBinanceServiceImpl(client, closeClient, new ObjectMapper());

        List<BinanceFuturesPositionClientDto> result =
                service.getPositions("api-key", "secret", "trace-position");

        assertTrue(client.positionsCalled);
        assertEquals("trace-position", client.positionsTraceId);
        assertEquals("-0.125", result.getFirst().getPositionAmt());
        assertEquals("SHORT", result.getFirst().getPositionSide());
    }

    @Test
    void openInvalidApiKeyThrowsReadinessException() {
        CapturingBinanceClient client = new CapturingBinanceClient();
        CapturingBinanceClient closeClient = new CapturingBinanceClient();
        client.openFailure = invalidApiKeyResponse("/api/binance/futures/order");
        ProcesBinanceServiceImpl service = new ProcesBinanceServiceImpl(client, closeClient, new ObjectMapper());
        service.setOrderSubmitEnabledForTest(true);

        BinanceApiReadinessException ex = assertThrows(BinanceApiReadinessException.class, () ->
                service.operationPosition(OperationDto.builder()
                        .symbol("BTCUSDT")
                        .side(Side.BUY)
                        .type(OrderType.MARKET)
                        .positionSide(PositionSide.LONG)
                        .quantity("0.01")
                        .reduceOnly(false)
                        .apiKey("bad-api-key")
                        .secret("secret")
                        .build()));

        assertEquals(BinanceApiReadinessException.REASON_CODE, ex.getErrCode());
        assertEquals("-2015", ex.getDetails().get("binanceCode"));
    }

    @Test
    void openTimeoutReconcilesByClientOrderIdBeforeFailing() {
        CapturingBinanceClient client = new CapturingBinanceClient();
        CapturingBinanceClient closeClient = new CapturingBinanceClient();
        client.openFailure = new ResourceAccessException("read timed out");
        client.lookupResponse = filledLookupResponse("BTCUSDT", "open-abc", 789L);
        ProcesBinanceServiceImpl service = new ProcesBinanceServiceImpl(client, closeClient, new ObjectMapper());
        service.setOrderSubmitEnabledForTest(true);

        BinanceFuturesOrderClientResponse response = service.operationPosition(OperationDto.builder()
                .symbol("BTCUSDT")
                .side(Side.BUY)
                .type(OrderType.MARKET)
                .positionSide(PositionSide.LONG)
                .quantity("0.01")
                .reduceOnly(false)
                .clientOrderId("open-abc")
                .apiKey("api-key")
                .secret("secret")
                .build());

        assertTrue(client.lookupCalled);
        assertEquals("open-abc", client.lastLookupClientOrderId);
        assertEquals(789L, response.getOrderId());
    }

    @Test
    void explicitLookupDelegatesByBinanceOrderId() {
        CapturingBinanceClient client = new CapturingBinanceClient();
        CapturingBinanceClient closeClient = new CapturingBinanceClient();
        client.lookupResponse = filledLookupResponse("BTCUSDT", "open-abc", 789L);
        ProcesBinanceServiceImpl service = new ProcesBinanceServiceImpl(client, closeClient, new ObjectMapper());

        BinanceFuturesOrderClientResponse response = service.findOrderByOrderId(OperationDto.builder()
                        .symbol("BTCUSDT")
                        .side(Side.BUY)
                        .type(OrderType.MARKET)
                        .positionSide(PositionSide.LONG)
                        .quantity("0.01")
                        .clientOrderId("open-abc")
                        .apiKey("api-key")
                        .secret("secret")
                        .build(), 789L)
                .orElseThrow();

        assertTrue(client.orderIdLookupCalled);
        assertEquals(789L, client.lastLookupOrderId);
        assertFalse(client.lookupCalled);
        assertEquals(789L, response.getOrderId());
    }

    @Test
    void closeInvalidApiKeyThrowsReadinessException() {
        CapturingBinanceClient client = new CapturingBinanceClient();
        CapturingBinanceClient closeClient = new CapturingBinanceClient();
        closeClient.closeFailure = invalidApiKeyResponse("/api/binance/futures/close-position");
        ProcesBinanceServiceImpl service = new ProcesBinanceServiceImpl(client, closeClient, new ObjectMapper());
        service.setOrderSubmitEnabledForTest(true);

        BinanceApiReadinessException ex = assertThrows(BinanceApiReadinessException.class, () ->
                service.operationPosition(OperationDto.builder()
                        .symbol("BTCUSDT")
                        .side(Side.SELL)
                        .type(OrderType.MARKET)
                        .positionSide(PositionSide.LONG)
                        .quantity("0.01")
                        .reduceOnly(true)
                        .apiKey("bad-api-key")
                        .secret("secret")
                        .build()));

        assertEquals(BinanceApiReadinessException.REASON_CODE, ex.getErrCode());
        assertEquals("-2015", ex.getDetails().get("binanceCode"));
        assertTrue(closeClient.closeCalled);
    }

    private static RestClientResponseException invalidApiKeyResponse(String path) {
        String body = """
                {
                  "data": {
                    "errorCode": "BINANCE_CLIENT_ERROR",
                    "details": {
                      "binanceCode": "-2015",
                      "binanceMsg": "Invalid API-key, IP, or permissions for action"
                    }
                  },
                  "traceId": "trace-invalid-key",
                  "path": "%s"
                }
                """.formatted(path);
        return new RestClientResponseException(
                "Unauthorized",
                401,
                "Unauthorized",
                HttpHeaders.EMPTY,
                body.getBytes(StandardCharsets.UTF_8),
                StandardCharsets.UTF_8
        );
    }

    private static final class CapturingBinanceClient implements BinanceClient {
        private NewOperationClientRequest lastRequest;
        private CloseOperationClientRequest lastCloseRequest;
        private String lastSymbolsApiKey;
        private boolean closeCalled;
        private RuntimeException openFailure;
        private RuntimeException closeFailure;
        private boolean lookupCalled;
        private String lastLookupClientOrderId;
        private BinanceFuturesOrderClientResponse lookupResponse;
        private boolean orderIdLookupCalled;
        private Long lastLookupOrderId;
        private boolean positionsCalled;
        private String positionsTraceId;
        private List<BinanceFuturesPositionClientDto> positionsResponse = List.of();

        @Override
        public ApiResponse<BinanceFuturesOrderClientResponse> openPosition(
                String apiKey,
                String secret,
                String originId,
                String userId,
                String walletId,
                String traceId,
                NewOperationClientRequest request
        ) {
            if (openFailure != null) {
                throw openFailure;
            }
            this.lastRequest = request;
            return ApiResponse.<BinanceFuturesOrderClientResponse>builder()
                    .statusCode(200)
                    .data(orderResponse(request))
                    .build();
        }

        @Override
        public ApiResponse<BinanceFuturesOrderClientResponse> closePosition(
                String apiKey,
                String secret,
                String originId,
                String userId,
                String walletId,
                String traceId,
                CloseOperationClientRequest request
        ) {
            this.closeCalled = true;
            if (closeFailure != null) {
                throw closeFailure;
            }
            this.lastCloseRequest = request;
            return ApiResponse.<BinanceFuturesOrderClientResponse>builder()
                    .statusCode(200)
                    .data(orderResponse(request))
                    .build();
        }

        @Override
        public ApiResponse<BinanceFuturesOrderClientResponse> getOrderByOrderId(
                String apiKey,
                String secret,
                String traceId,
                String symbol,
                Long orderId
        ) {
            this.orderIdLookupCalled = true;
            this.lastLookupOrderId = orderId;
            return ApiResponse.<BinanceFuturesOrderClientResponse>builder()
                    .statusCode(200)
                    .data(lookupResponse)
                    .build();
        }

        @Override
        public ApiResponse<BinanceFuturesOrderClientResponse> getOrderByClientOrderId(
                String apiKey,
                String secret,
                String traceId,
                String symbol,
                String origClientOrderId
        ) {
            this.lookupCalled = true;
            this.lastLookupClientOrderId = origClientOrderId;
            return ApiResponse.<BinanceFuturesOrderClientResponse>builder()
                    .statusCode(200)
                    .data(lookupResponse)
                    .build();
        }

        @Override
        public ApiResponse<TradingConfigPreconfigureClientResponse> preconfigureTradingSettings(
                String apiKey,
                String secret,
                String userId,
                String walletId,
                String traceId,
                TradingConfigPreconfigureClientRequest request
        ) {
            throw new UnsupportedOperationException("not used");
        }

        @Override
        public ApiResponse<List<BinanceFuturesSymbolInfoClientDto>> symbols(String apiKey) {
            this.lastSymbolsApiKey = apiKey;
            BinanceFuturesSymbolInfoClientDto symbol = new BinanceFuturesSymbolInfoClientDto();
            symbol.setSymbol("BTCUSDT");
            symbol.setStatus("TRADING");
            symbol.setContractType("PERPETUAL");
            symbol.setQuoteAsset("USDT");
            symbol.setMarginAsset("USDT");
            symbol.setOrderTypes(List.of("MARKET"));
            return ApiResponse.<List<BinanceFuturesSymbolInfoClientDto>>builder()
                    .statusCode(200)
                    .data(List.of(symbol))
                    .build();
        }

        @Override
        public ApiResponse<BinanceFuturesMarketPriceClientDto> marketPrice(String symbol, String usage, boolean allowStale) {
            BinanceFuturesMarketPriceClientDto price = new BinanceFuturesMarketPriceClientDto();
            price.setSymbol(symbol);
            price.setUsage(usage);
            price.setAvailable(true);
            price.setPrice(new BigDecimal("100"));
            price.setSource("binance_ws_book_mid");
            return ApiResponse.<BinanceFuturesMarketPriceClientDto>builder()
                    .statusCode(200)
                    .data(price)
                    .build();
        }

        @Override
        public ApiResponse<List<BinanceFuturesPositionClientDto>> positions(
                String apiKey,
                String secret,
                String traceId
        ) {
            this.positionsCalled = true;
            this.positionsTraceId = traceId;
            return ApiResponse.<List<BinanceFuturesPositionClientDto>>builder()
                    .statusCode(200)
                    .data(positionsResponse)
                    .build();
        }

        @Override
        public ApiResponse<BinanceOrderBookSnapshotClientDto> orderBook(String symbol, int limit) {
            throw new UnsupportedOperationException("not used");
        }

        @Override
        public ApiResponse<FuturesAssetBalanceClientResponse> assetBalance(
                String apiKey,
                String secret,
                String originId,
                String userId,
                String walletId,
                String traceId,
                String asset
        ) {
            throw new UnsupportedOperationException("not used");
        }

        @Override
        public ApiResponse<FuturesConvertToBnbClientResponse> convertToBnb(
                String apiKey,
                String secret,
                String originId,
                String userId,
                String walletId,
                String traceId,
                FuturesConvertToBnbClientRequest request
        ) {
            throw new UnsupportedOperationException("not used");
        }

        private BinanceFuturesOrderClientResponse orderResponse(NewOperationClientRequest request) {
            BinanceFuturesOrderClientResponse response = new BinanceFuturesOrderClientResponse();
            response.setOrderId(123L);
            response.setSymbol(request.getSymbol());
            response.setStatus("FILLED");
            response.setAvgPrice(new BigDecimal("100"));
            response.setOrigQty(new BigDecimal(request.getQuantity()));
            response.setExecutedQty(new BigDecimal(request.getQuantity()));
            response.setSide(request.getSide().name());
            response.setPositionSide(request.getPositionSide().name());
            return response;
        }

        private BinanceFuturesOrderClientResponse orderResponse(CloseOperationClientRequest request) {
            BinanceFuturesOrderClientResponse response = new BinanceFuturesOrderClientResponse();
            response.setOrderId(456L);
            response.setSymbol(request.getSymbol());
            response.setStatus("FILLED");
            response.setAvgPrice(new BigDecimal("100"));
            response.setOrigQty(request.getOperationQty());
            response.setExecutedQty(request.getOperationQty());
            response.setSide("SELL");
            response.setPositionSide(request.getPositionSide().name());
            response.setClientOrderId(request.getClientOrderId());
            return response;
        }
    }

    private static BinanceFuturesOrderClientResponse filledLookupResponse(String symbol, String clientOrderId, long orderId) {
        BinanceFuturesOrderClientResponse response = new BinanceFuturesOrderClientResponse();
        response.setOrderId(orderId);
        response.setSymbol(symbol);
        response.setStatus("FILLED");
        response.setAvgPrice(new BigDecimal("100"));
        response.setOrigQty(new BigDecimal("0.01"));
        response.setExecutedQty(new BigDecimal("0.01"));
        response.setSide("BUY");
        response.setPositionSide("LONG");
        response.setClientOrderId(clientOrderId);
        response.setUpdateTime(1710000000000L);
        return response;
    }
}
