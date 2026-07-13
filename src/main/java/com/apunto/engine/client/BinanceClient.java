package com.apunto.engine.client;

import com.apunto.engine.dto.client.*;
import com.apunto.engine.shared.dto.ApiResponse;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
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
            @RequestHeader(value = "X-COPY-TRACE-ID", required = false) String traceId,
            @RequestBody NewOperationClientRequest request
    );

    @PostExchange("/close-position")
    ApiResponse<BinanceFuturesOrderClientResponse> closePosition(
            @RequestHeader("X-BINANCE-APIKEY") String apiKey,
            @RequestHeader("X-BINANCE-SECRET") String secret,
            @RequestHeader(value = "X-COPY-ORIGIN-ID", required = false) String originId,
            @RequestHeader(value = "X-COPY-USER-ID", required = false) String userId,
            @RequestHeader(value = "X-COPY-WALLET-ID", required = false) String walletId,
            @RequestHeader(value = "X-COPY-TRACE-ID", required = false) String traceId,
            @RequestBody CloseOperationClientRequest request
    );

    @GetExchange("/order")
    ApiResponse<BinanceFuturesOrderClientResponse> getOrderByClientOrderId(
            @RequestHeader("X-BINANCE-APIKEY") String apiKey,
            @RequestHeader("X-BINANCE-SECRET") String secret,
            @RequestHeader(value = "X-COPY-TRACE-ID", required = false) String traceId,
            @RequestParam("symbol") String symbol,
            @RequestParam("origClientOrderId") String origClientOrderId
    );

    @GetExchange("/order")
    ApiResponse<BinanceFuturesOrderClientResponse> getOrderByOrderId(
            @RequestHeader("X-BINANCE-APIKEY") String apiKey,
            @RequestHeader("X-BINANCE-SECRET") String secret,
            @RequestHeader(value = "X-COPY-TRACE-ID", required = false) String traceId,
            @RequestParam("symbol") String symbol,
            @RequestParam("orderId") Long orderId
    );

    @GetExchange("/positions")
    ApiResponse<List<BinanceFuturesPositionClientDto>> positions(
            @RequestHeader("X-BINANCE-APIKEY") String apiKey,
            @RequestHeader("X-BINANCE-SECRET") String secret,
            @RequestHeader(value = "X-COPY-TRACE-ID", required = false) String traceId
    );

    @PostExchange("/settings/preconfigure")
    ApiResponse<TradingConfigPreconfigureClientResponse> preconfigureTradingSettings(
            @RequestHeader("X-BINANCE-APIKEY") String apiKey,
            @RequestHeader("X-BINANCE-SECRET") String secret,
            @RequestHeader(value = "X-COPY-USER-ID", required = false) String userId,
            @RequestHeader(value = "X-COPY-WALLET-ID", required = false) String walletId,
            @RequestHeader(value = "X-COPY-TRACE-ID", required = false) String traceId,
            @RequestBody TradingConfigPreconfigureClientRequest request
    );

    @GetExchange("/symbols")
    ApiResponse<List<BinanceFuturesSymbolInfoClientDto>> symbols(
            @RequestHeader(value = "X-BINANCE-APIKEY", required = false) String apiKey
    );

    @GetExchange("/market/price")
    ApiResponse<BinanceFuturesMarketPriceClientDto> marketPrice(
            @RequestParam("symbol") String symbol,
            @RequestParam("usage") String usage,
            @RequestParam("allowStale") boolean allowStale
    );

    @GetExchange("/market/order-book")
    ApiResponse<BinanceOrderBookSnapshotClientDto> orderBook(
            @RequestParam("symbol") String symbol,
            @RequestParam("limit") int limit
    );

    @GetExchange("/wallet/asset-balance")
    ApiResponse<FuturesAssetBalanceClientResponse> assetBalance(
            @RequestHeader("X-BINANCE-APIKEY") String apiKey,
            @RequestHeader("X-BINANCE-SECRET") String secret,
            @RequestHeader(value = "X-COPY-ORIGIN-ID", required = false) String originId,
            @RequestHeader(value = "X-COPY-USER-ID", required = false) String userId,
            @RequestHeader(value = "X-COPY-WALLET-ID", required = false) String walletId,
            @RequestHeader(value = "X-COPY-TRACE-ID", required = false) String traceId,
            @RequestParam("asset") String asset
    );

    @PostExchange("/wallet/convert-to-bnb")
    ApiResponse<FuturesConvertToBnbClientResponse> convertToBnb(
            @RequestHeader("X-BINANCE-APIKEY") String apiKey,
            @RequestHeader("X-BINANCE-SECRET") String secret,
            @RequestHeader(value = "X-COPY-ORIGIN-ID", required = false) String originId,
            @RequestHeader(value = "X-COPY-USER-ID", required = false) String userId,
            @RequestHeader(value = "X-COPY-WALLET-ID", required = false) String walletId,
            @RequestHeader(value = "X-COPY-TRACE-ID", required = false) String traceId,
            @RequestBody FuturesConvertToBnbClientRequest request
    );
}
