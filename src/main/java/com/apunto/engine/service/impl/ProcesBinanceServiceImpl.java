package com.apunto.engine.service.impl;

import com.apunto.engine.client.BinanceClient;
import com.apunto.engine.dto.CloseOperationDto;
import com.apunto.engine.dto.OperationDto;
import com.apunto.engine.dto.client.*;
import com.apunto.engine.service.ProcesBinanceService;
import com.apunto.engine.shared.dto.ApiResponse;
import lombok.AllArgsConstructor;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@AllArgsConstructor
public class ProcesBinanceServiceImpl implements ProcesBinanceService {

    private final BinanceClient binanceClient;

    @Override
    public BinanceFuturesOrderClientResponse operationPosition(OperationDto newOperationDto) {
        ApiResponse<BinanceFuturesOrderClientResponse> response = binanceClient.openPosition(newOperationDto.getApiKey(),
                newOperationDto.getSecret(),
                builderNewOperationClientRequest(newOperationDto));

        return response.getData();
    }

    @Override
    public BinanceFuturesOrderClientResponse closeOperation(CloseOperationDto closeOperationDto) {
        ApiResponse<BinanceFuturesOrderClientResponse> response = binanceClient.closePosition(closeOperationDto.getApiKey(),
                closeOperationDto.getSecret(),
                builderCloseOperationClientRequest(closeOperationDto));

        return response.getData();
    }

    @Override
    @Cacheable(value = "binanceSymbols", key = "#apiKey")
    public List<BinanceFuturesSymbolInfoClientDto> getSymbols(String apiKey) {
        try{
            ApiResponse<List<BinanceFuturesSymbolInfoClientDto>> response = binanceClient.symbols(apiKey);
            return response.getData();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    private NewOperationClientRequest builderNewOperationClientRequest(OperationDto newOperationDto){
        return  NewOperationClientRequest.builder()
                .symbol(newOperationDto.getSymbol())
                .side(newOperationDto.getSide())
                .type(newOperationDto.getType())
                .positionSide(newOperationDto.getPositionSide())
                .quantity(newOperationDto.getQuantity())
                .price(newOperationDto.getPrice())
                .leverage(newOperationDto.getLeverage())
                .timeInForce(newOperationDto.getTimeInForce())
                .build();
    }

    private CloseOperationClientRequest builderCloseOperationClientRequest(CloseOperationDto closeOperationDto){
        return CloseOperationClientRequest.builder()
                .symbol(closeOperationDto.getSymbol())
                .operationQty(closeOperationDto.getOperationQty())
                .build();
    }
}
