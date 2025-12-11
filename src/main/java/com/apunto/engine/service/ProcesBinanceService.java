package com.apunto.engine.service;

import com.apunto.engine.dto.CloseOperationDto;
import com.apunto.engine.dto.OperationDto;
import com.apunto.engine.dto.client.BinanceFuturesOrderClientResponse;
import com.apunto.engine.dto.client.BinanceFuturesSymbolInfoClientDto;

import java.util.List;

public interface ProcesBinanceService {

    BinanceFuturesOrderClientResponse operationPosition(OperationDto newOperationDto);
    BinanceFuturesOrderClientResponse closeOperation(CloseOperationDto closeOperationDto);
    List<BinanceFuturesSymbolInfoClientDto> getSymbols(String apiKey);
}
