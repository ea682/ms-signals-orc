package com.apunto.engine.shared.exception;

import java.util.Map;

public class PositionNotFoundException extends EngineException {

    public PositionNotFoundException(String symbol) {
        super(ErrorCode.RESOURCE_NOT_FOUND,
                "No se encontró posición abierta para el símbolo " + symbol,
                Map.of("symbol", symbol));
    }
}