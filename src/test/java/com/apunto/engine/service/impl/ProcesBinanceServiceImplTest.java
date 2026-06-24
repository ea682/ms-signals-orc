package com.apunto.engine.service.impl;

import com.apunto.engine.client.BinanceClient;
import com.apunto.engine.dto.OperationDto;
import com.apunto.engine.dto.client.BinanceFuturesOrderClientResponse;
import com.apunto.engine.dto.client.BinanceFuturesSymbolInfoClientDto;
import com.apunto.engine.dto.client.CloseOperationClientRequest;
import com.apunto.engine.dto.client.FuturesAssetBalanceClientResponse;
import com.apunto.engine.dto.client.FuturesConvertToBnbClientRequest;
import com.apunto.engine.dto.client.FuturesConvertToBnbClientResponse;
import com.apunto.engine.dto.client.NewOperationClientRequest;
import com.apunto.engine.shared.dto.ApiResponse;
import com.apunto.engine.shared.enums.OrderType;
import com.apunto.engine.shared.enums.PositionSide;
import com.apunto.engine.shared.enums.Side;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

class ProcesBinanceServiceImplTest {

    @Test
    void reduceOnlyHedgeOrderOmitsReduceOnlyButDisablesAccountSettings() {
        CapturingBinanceClient client = new CapturingBinanceClient();
        ProcesBinanceServiceImpl service = new ProcesBinanceServiceImpl(client, new ObjectMapper());

        service.operationPosition(OperationDto.builder()
                .symbol("BTCUSDT")
                .side(Side.SELL)
                .type(OrderType.MARKET)
                .positionSide(PositionSide.LONG)
                .quantity("0.01")
                .reduceOnly(true)
                .apiKey("api-key")
                .secret("secret")
                .build());

        assertNull(client.lastRequest.getReduceOnly());
        assertFalse(client.lastRequest.getConfigureAccountSettings());
    }

    @Test
    void regularOpenKeepsAccountSettingsUnsetWhenCallerDoesNotOverride() {
        CapturingBinanceClient client = new CapturingBinanceClient();
        ProcesBinanceServiceImpl service = new ProcesBinanceServiceImpl(client, new ObjectMapper());

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
    }

    private static final class CapturingBinanceClient implements BinanceClient {
        private NewOperationClientRequest lastRequest;

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
                CloseOperationClientRequest request
        ) {
            throw new UnsupportedOperationException("not used");
        }

        @Override
        public ApiResponse<List<BinanceFuturesSymbolInfoClientDto>> symbols(String apiKey) {
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
    }
}
