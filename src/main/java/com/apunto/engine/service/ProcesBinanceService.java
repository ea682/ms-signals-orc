package com.apunto.engine.service;

import com.apunto.engine.dto.OperationDto;
import com.apunto.engine.dto.client.BinanceFuturesOrderClientResponse;
import com.apunto.engine.dto.client.BinanceFuturesSymbolInfoClientDto;

import java.util.List;

public interface ProcesBinanceService {

    /**
     * Ejecuta una orden en Binance (OPEN o CLOSE según reduceOnly).
     * - Si reduceOnly = false -> openPosition
     * - Si reduceOnly = true  -> closePosition
     */
    BinanceFuturesOrderClientResponse operationPosition(OperationDto dto);

    /**
     * Obtiene el catálogo de símbolos de Binance Futures.
     */
    List<BinanceFuturesSymbolInfoClientDto> getSymbols(String apiKey);
}
