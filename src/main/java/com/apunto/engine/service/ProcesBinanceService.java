package com.apunto.engine.service;

import com.apunto.engine.dto.OperationDto;
import com.apunto.engine.dto.client.BinanceFuturesOrderClientResponse;
import com.apunto.engine.dto.client.BinanceFuturesSymbolInfoClientDto;

import java.util.List;

public interface ProcesBinanceService {

    /**
     * Ejecuta una orden en Binance.
     * - reduceOnly=false se envía como false para aperturas.
     * - reduceOnly=true representa intención de cierre/reducción.
     * - En Hedge Mode (positionSide LONG/SHORT), ORC omite reduceOnly al llamar a ms-binance,
     *   porque Binance Futures rechaza ese parámetro con binanceCode=-1106.
     */
    BinanceFuturesOrderClientResponse operationPosition(OperationDto dto);

    /**
     * Obtiene el catálogo de símbolos de Binance Futures.
     */
    List<BinanceFuturesSymbolInfoClientDto> getSymbols(String apiKey);
}
