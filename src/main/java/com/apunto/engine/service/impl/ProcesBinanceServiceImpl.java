package com.apunto.engine.service.impl;

import com.apunto.engine.client.BinanceClient;
import com.apunto.engine.dto.OperationDto;
import com.apunto.engine.dto.client.*;
import com.apunto.engine.service.ProcesBinanceService;
import com.apunto.engine.shared.dto.ApiResponse;
import com.apunto.engine.shared.exception.EngineException;
import com.apunto.engine.shared.exception.ErrorCode;
import com.apunto.engine.shared.exception.SkipExecutionException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

@Slf4j
@Service
@RequiredArgsConstructor
public class ProcesBinanceServiceImpl implements ProcesBinanceService {

    private static final String ERR_DTO_NULL = "OperationDto no puede ser null";
    private static final String ERR_SYMBOL_EMPTY = "symbol requerido";
    private static final String ERR_APIKEY_EMPTY = "apiKey requerido";
    private static final String ERR_SECRET_EMPTY = "secret requerido";
    private static final String ERR_QTY_EMPTY = "quantity requerido";

    private final BinanceClient binanceClient;

    @Override
    public BinanceFuturesOrderClientResponse operationPosition(OperationDto dto) {
        Objects.requireNonNull(dto, ERR_DTO_NULL);

        if (dto.getSymbol() == null || dto.getSymbol().isBlank()) {
            throw new SkipExecutionException(ERR_SYMBOL_EMPTY);
        }
        if (dto.getApiKey() == null || dto.getApiKey().isBlank()) {
            throw new SkipExecutionException(ERR_APIKEY_EMPTY);
        }
        if (dto.getSecret() == null || dto.getSecret().isBlank()) {
            throw new SkipExecutionException(ERR_SECRET_EMPTY);
        }
        if (dto.getQuantity() == null || dto.getQuantity().isBlank()) {
            throw new SkipExecutionException(ERR_QTY_EMPTY);
        }

        try {
            if (dto.isReduceOnly()) {
                // CLOSE
                BigDecimal qty = new BigDecimal(dto.getQuantity());

                CloseOperationClientRequest request = CloseOperationClientRequest.builder()
                        .symbol(dto.getSymbol())
                        .operationQty(qty)
                        .build();

                ApiResponse<BinanceFuturesOrderClientResponse> resp =
                        binanceClient.closePosition(dto.getApiKey(), dto.getSecret(), request);

                return unwrap(resp, "closePosition");

            } else {
                // OPEN
                NewOperationClientRequest request = NewOperationClientRequest.builder()
                        .symbol(dto.getSymbol())
                        .side(dto.getSide())
                        .type(dto.getType())
                        .quantity(dto.getQuantity())
                        .price(dto.getPrice())
                        .timeInForce(dto.getTimeInForce())
                        .leverage(dto.getLeverage())
                        .positionSide(dto.getPositionSide())
                        .reduceOnly(false)
                        .build();

                ApiResponse<BinanceFuturesOrderClientResponse> resp =
                        binanceClient.openPosition(dto.getApiKey(), dto.getSecret(), request);

                return unwrap(resp, "openPosition");
            }
        } catch (NumberFormatException nfe) {
            // Qty inválida: no sirve reintentar
            throw new SkipExecutionException("quantity inválida: " + dto.getQuantity());
        }
    }

    @Override
    public List<BinanceFuturesSymbolInfoClientDto> getSymbols(String apiKey) {
        if (apiKey == null || apiKey.isBlank()) {
            throw new SkipExecutionException("apiKey requerido para symbols()");
        }

        ApiResponse<List<BinanceFuturesSymbolInfoClientDto>> resp = binanceClient.symbols(apiKey);
        List<BinanceFuturesSymbolInfoClientDto> data = unwrap(resp, "symbols");

        return data == null ? Collections.emptyList() : data;
    }

    private <T> T unwrap(ApiResponse<T> resp, String op) {
        if (resp == null) {
            throw new EngineException(ErrorCode.EXTERNAL_SERVICE_ERROR, "Respuesta nula de Binance (" + op + ")");
        }

        int code = resp.getStatusCode();
        T data = resp.getData();

        // Heurística tolerante: si viene data, aceptamos aunque statusCode venga 0.
        if (data != null && (code == 0 || (code >= 200 && code < 300))) {
            return data;
        }

        // Clasificación por statusCode si viene seteado.
        if (code == 429) {
            throw new EngineException(ErrorCode.BINANCE_RATE_LIMIT,
                    "Binance rate limit (" + op + "): " + safeMsg(resp));
        }
        if (code >= 400 && code < 500) {
            // 4xx: normalmente no vale reintentar (validación/credenciales/saldo/etc.)
            throw new EngineException(ErrorCode.BINANCE_CLIENT_ERROR,
                    "Binance client error (" + op + ") code=" + code + " msg=" + safeMsg(resp));
        }
        if (code >= 500) {
            // 5xx: transitorio
            throw new EngineException(ErrorCode.EXTERNAL_SERVICE_ERROR,
                    "Binance server error (" + op + ") code=" + code + " msg=" + safeMsg(resp));
        }

        // Sin code útil y sin data => error
        throw new EngineException(ErrorCode.EXTERNAL_SERVICE_ERROR,
                "Respuesta inválida de Binance (" + op + ") code=" + code + " msg=" + safeMsg(resp));
    }

    private String safeMsg(ApiResponse<?> resp) {
        if (resp == null) return "null";
        String m = resp.getMessage();
        return (m == null || m.isBlank()) ? (resp.getStatus() != null ? resp.getStatus() : "sin mensaje") : m;
    }
}

