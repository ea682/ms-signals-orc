package com.apunto.engine.service.impl;

import com.apunto.engine.client.BinanceClient;
import com.apunto.engine.dto.OperationDto;
import com.apunto.engine.dto.client.BinanceFuturesOrderClientResponse;
import com.apunto.engine.dto.client.BinanceFuturesSymbolInfoClientDto;
import com.apunto.engine.dto.client.NewOperationClientRequest;
import com.apunto.engine.service.ProcesBinanceService;
import com.apunto.engine.shared.dto.ApiResponse;
import com.apunto.engine.shared.exception.EngineException;
import com.apunto.engine.shared.exception.ErrorCode;
import com.apunto.engine.shared.exception.SkipExecutionException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class ProcesBinanceServiceImpl implements ProcesBinanceService {

    private static final String ERR_DTO_NULL = "OperationDto no puede ser null";
    private static final String ERR_SYMBOL_EMPTY = "symbol requerido";
    private static final String ERR_APIKEY_EMPTY = "apiKey requerido";
    private static final String ERR_SECRET_EMPTY = "secret requerido";
    private static final String ERR_QTY_EMPTY = "quantity requerido";
    private static final String ERR_POSITIONSIDE_EMPTY = "positionSide requerido";

    private final BinanceClient binanceClient;

    @Override
    public BinanceFuturesOrderClientResponse operationPosition(OperationDto dto) {
        validateOperation(dto);

        NewOperationClientRequest request = NewOperationClientRequest.builder()
                .symbol(dto.getSymbol())
                .side(dto.getSide())
                .type(dto.getType())
                .quantity(dto.getQuantity())
                .price(dto.getPrice())
                .timeInForce(dto.getTimeInForce())
                .leverage(dto.getLeverage())
                .positionSide(dto.getPositionSide())
                .reduceOnly(dto.isReduceOnly())
                .build();

        log.info("event=binance.futures.order.send symbol={} side={} type={} positionSide={} reduceOnly={} qty={}",
                dto.getSymbol(), dto.getSide(), dto.getType(), dto.getPositionSide(), dto.isReduceOnly(), dto.getQuantity());

        ApiResponse<BinanceFuturesOrderClientResponse> resp = binanceClient.openPosition(dto.getApiKey(), dto.getSecret(), request);
        BinanceFuturesOrderClientResponse data = unwrap(resp, "futures.order");

        log.info("event=binance.futures.order.ok symbol={} orderId={} avgPrice={} origQty={}",
                data.getSymbol(), data.getOrderId(), data.getAvgPrice(), data.getOrigQty());

        return data;
    }

    @Override
    public List<BinanceFuturesSymbolInfoClientDto> getSymbols(String apiKey) {
        if (apiKey == null || apiKey.isBlank()) {
            throw new SkipExecutionException(ERR_APIKEY_EMPTY);
        }

        ApiResponse<List<BinanceFuturesSymbolInfoClientDto>> resp = binanceClient.symbols(apiKey);
        List<BinanceFuturesSymbolInfoClientDto> data = unwrap(resp, "symbols");
        return data == null ? Collections.emptyList() : data;
    }

    private void validateOperation(OperationDto dto) {
        if (dto == null) throw new SkipExecutionException(ERR_DTO_NULL);
        if (dto.getApiKey() == null || dto.getApiKey().isBlank()) throw new SkipExecutionException(ERR_APIKEY_EMPTY);
        if (dto.getSecret() == null || dto.getSecret().isBlank()) throw new SkipExecutionException(ERR_SECRET_EMPTY);
        if (dto.getSymbol() == null || dto.getSymbol().isBlank()) throw new SkipExecutionException(ERR_SYMBOL_EMPTY);
        if (dto.getSide() == null) throw new SkipExecutionException("side requerido");
        if (dto.getType() == null) throw new SkipExecutionException("type requerido");
        if (dto.getQuantity() == null || dto.getQuantity().isBlank()) throw new SkipExecutionException(ERR_QTY_EMPTY);
        if (dto.getPositionSide() == null) throw new SkipExecutionException(ERR_POSITIONSIDE_EMPTY);
    }

    private <T> T unwrap(ApiResponse<T> resp, String op) {
        if (resp == null) {
            throw new EngineException(ErrorCode.EXTERNAL_SERVICE_ERROR, "Respuesta nula de Binance (" + op + ")");
        }

        int code = resp.getStatusCode();
        T data = resp.getData();

        if (data != null && (code == 0 || (code >= 200 && code < 300))) {
            return data;
        }

        String msg = safeMsg(resp);

        if (code == 429) {
            throw new EngineException(ErrorCode.BINANCE_RATE_LIMIT, "Binance rate limit (" + op + "): " + msg);
        }
        if (code >= 400 && code < 500) {
            throw new EngineException(ErrorCode.BINANCE_CLIENT_ERROR, "Binance client error (" + op + ") code=" + code + " msg=" + msg);
        }
        if (code >= 500) {
            throw new EngineException(ErrorCode.EXTERNAL_SERVICE_ERROR, "Binance server error (" + op + ") code=" + code + " msg=" + msg);
        }

        throw new EngineException(ErrorCode.EXTERNAL_SERVICE_ERROR, "Respuesta inválida de Binance (" + op + ") code=" + code + " msg=" + msg);
    }

    private String safeMsg(ApiResponse<?> resp) {
        if (resp == null) return "null";
        String m = resp.getMessage();
        if (m != null && !m.isBlank()) return m;
        return resp.getStatus() != null ? resp.getStatus() : "sin mensaje";
    }
}
