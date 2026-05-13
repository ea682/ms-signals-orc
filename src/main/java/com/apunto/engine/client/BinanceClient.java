package com.apunto.engine.client;

import com.apunto.engine.dto.client.*;
import com.apunto.engine.shared.dto.ApiResponse;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.service.annotation.GetExchange;
import org.springframework.web.service.annotation.HttpExchange;
import org.springframework.web.service.annotation.PostExchange;

import java.util.List;


@HttpExchange(
        url = "/api/binance/futures",
        contentType = "application/json",
        accept = "application/json"
)
public interface BinanceClient {

    @PostExchange("/order")
    ApiResponse<BinanceFuturesOrderClientResponse> openPosition(
            @RequestHeader("X-BINANCE-APIKEY") String apiKey,
            @RequestHeader("X-BINANCE-SECRET") String secret,
            @RequestHeader(value = "X-COPY-ORIGIN-ID", required = false) String originId,
            @RequestHeader(value = "X-COPY-USER-ID", required = false) String userId,
            @RequestHeader(value = "X-COPY-WALLET-ID", required = false) String walletId,
            @RequestBody NewOperationClientRequest request
    );

    @PostExchange("/close-position")
    ApiResponse<BinanceFuturesOrderClientResponse> closePosition(
            @RequestHeader("X-BINANCE-APIKEY") String apiKey,
            @RequestHeader("X-BINANCE-SECRET") String secret,
            @RequestBody CloseOperationClientRequest request
    );

    @GetExchange("/symbols")
    ApiResponse<List<BinanceFuturesSymbolInfoClientDto>> symbols(
            @RequestHeader("X-BINANCE-APIKEY") String apiKey
    );
}