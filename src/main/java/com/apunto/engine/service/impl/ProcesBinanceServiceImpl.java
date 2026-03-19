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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientResponseException;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
    private final ObjectMapper objectMapper;

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
                .clientOrderId(dto.getClientOrderId())
                .build();

        log.info("event=binance.futures.order.send symbol={} side={} type={} positionSide={} reduceOnly={} qty={}",
                dto.getSymbol(), dto.getSide(), dto.getType(), dto.getPositionSide(), dto.isReduceOnly(), dto.getQuantity());

        try {
            ApiResponse<BinanceFuturesOrderClientResponse> resp = binanceClient.openPosition(dto.getApiKey(), dto.getSecret(), request);
            BinanceFuturesOrderClientResponse data = unwrap(resp, "futures.order");

            log.info("event=binance.futures.order.ok symbol={} orderId={} avgPrice={} origQty={}",
                    data.getSymbol(), data.getOrderId(), data.getAvgPrice(), data.getOrigQty());

            return data;
        } catch (RestClientResponseException ex) {
            BinanceHttpError err = parseBinanceHttpError(ex);

            log.warn("event=binance.futures.order.fail symbol={} side={} type={} positionSide={} reduceOnly={} qty={} httpStatus={} errorCode={} binanceCode={} binanceMsg=\"{}\" traceId={} path={}",
                    dto.getSymbol(),
                    dto.getSide(),
                    dto.getType(),
                    dto.getPositionSide(),
                    dto.isReduceOnly(),
                    dto.getQuantity(),
                    err.httpStatus(),
                    err.errorCode(),
                    err.binanceCode(),
                    safeLog(err.binanceMsg()),
                    err.traceId(),
                    err.path());

            Map<String, Object> details = new LinkedHashMap<>();
            details.put("httpStatus", Integer.toString(err.httpStatus()));
            details.put("errorCode", safeNull(err.errorCode()));
            details.put("symbol", safeNull(dto.getSymbol()));
            details.put("side", dto.getSide() == null ? "" : dto.getSide().name());
            details.put("type", dto.getType() == null ? "" : dto.getType().name());
            details.put("positionSide", dto.getPositionSide() == null ? "" : dto.getPositionSide().name());
            details.put("quantity", safeNull(dto.getQuantity()));
            details.put("reduceOnly", Boolean.toString(dto.isReduceOnly()));
            details.put("binanceCode", safeNull(err.binanceCode()));
            details.put("binanceMsg", safeNull(safeLog(err.binanceMsg())));
            details.put("traceId", safeNull(err.traceId()));
            details.put("path", safeNull(err.path()));

            throw new EngineException(
                    ErrorCode.BINANCE_CLIENT_ERROR,
                    "Binance rechazó la orden: " + safeLog(err.binanceMsg()),
                    ex,
                    details
            );
        }
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


    private BinanceHttpError parseBinanceHttpError(RestClientResponseException ex) {
        String body = ex.getResponseBodyAsString();

        try {
            JsonNode root = objectMapper.readTree(body);
            JsonNode data = root.path("data");
            JsonNode details = data.path("details");

            return new BinanceHttpError(
                    ex.getRawStatusCode(),
                    text(root.path("data"), "errorCode"),
                    text(details, "binanceCode"),
                    text(details, "binanceMsg"),
                    text(root, "traceId"),
                    text(root, "path")
            );
        } catch (Exception parseEx) {
            return new BinanceHttpError(
                    ex.getRawStatusCode(),
                    "HTTP_ERROR",
                    null,
                    ex.getStatusText(),
                    null,
                    null
            );
        }
    }

    private String text(JsonNode node, String field) {
        JsonNode value = node.path(field);
        return value.isMissingNode() || value.isNull() ? null : value.asText();
    }

    private String safeLog(String s) {
        if (s == null) return "";
        String clean = s
                .replace("\n", " ")
                .replace("\r", " ")
                .replace("\t", " ")
                .replace('"', '\'');
        return clean.length() > 1000 ? clean.substring(0, 1000) : clean;
    }


    private String safeNull(String s) {
        return s == null ? "" : s;
    }

    private record BinanceHttpError(
            int httpStatus,
            String errorCode,
            String binanceCode,
            String binanceMsg,
            String traceId,
            String path
    ) {}

    private void validateOperation(OperationDto dto) {
        if (dto == null) throw new SkipExecutionException(ERR_DTO_NULL);
        if (dto.getApiKey() == null || dto.getApiKey().isBlank()) throw new SkipExecutionException(ERR_APIKEY_EMPTY);
        if (dto.getSecret() == null || dto.getSecret().isBlank()) throw new SkipExecutionException(ERR_SECRET_EMPTY);
        if (dto.getSymbol() == null || dto.getSymbol().isBlank()) throw new SkipExecutionException(ERR_SYMBOL_EMPTY);
        if (dto.getSide() == null) {
            throw new SkipExecutionException(
                    "order_side_missing",
                    "side requerido",
                    com.apunto.engine.shared.util.LogFmt.kv("symbol", dto.getSymbol())
            );
        }
        if (dto.getType() == null) {
            throw new SkipExecutionException(
                    "order_type_missing",
                    "type requerido",
                    com.apunto.engine.shared.util.LogFmt.kv("symbol", dto.getSymbol())
            );
        }
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
