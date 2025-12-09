package com.apunto.engine.service;

import com.apunto.engine.dto.CloseOperationDto;
import com.apunto.engine.dto.NewOperationDto;
import com.apunto.engine.dto.client.BinanceFuturesOrderClientResponse;
import com.apunto.engine.dto.client.BinanceFuturesSymbolInfoClientDto;

import java.util.List;

public interface ProcesBinanceService {

    BinanceFuturesOrderClientResponse newOperation(NewOperationDto newOperationDto);
    BinanceFuturesOrderClientResponse closeOperation(CloseOperationDto closeOperationDto);
    List<BinanceFuturesSymbolInfoClientDto> getSymbols(String apiKey);
}
