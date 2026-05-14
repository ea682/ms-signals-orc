package com.apunto.engine.hyperliquid.dto;

import com.apunto.engine.dto.OperacionDto;
import com.apunto.engine.events.OperacionEvent;

import java.util.UUID;

public record HyperliquidMappedDelta(
        String idempotencyKey,
        String positionKey,
        String wallet,
        String symbol,
        String side,
        String deltaType,
        OperacionEvent event,
        HyperliquidDeltaRequest request
) {
    public HyperliquidMappedDelta withOriginId(UUID originId) {
        if (originId == null) {
            throw new IllegalArgumentException("originId is required");
        }
        if (event == null || event.getOperacion() == null) {
            throw new IllegalArgumentException("mapped event operation is required");
        }
        OperacionDto source = event.getOperacion();
        OperacionDto reboundOperation = OperacionDto.builder()
                .idOperacion(originId)
                .idCuenta(source.getIdCuenta())
                .parSymbol(source.getParSymbol())
                .tipoOperacion(source.getTipoOperacion())
                .size(source.getSize())
                .sizeQty(source.getSizeQty())
                .notionalUsd(source.getNotionalUsd())
                .marginUsedUsd(source.getMarginUsedUsd())
                .precioEntrada(source.getPrecioEntrada())
                .precioCierre(source.getPrecioCierre())
                .precioMercado(source.getPrecioMercado())
                .fechaCreacion(source.getFechaCreacion())
                .fechaCierre(source.getFechaCierre())
                .operacionActiva(source.isOperacionActiva())
                .build();
        return new HyperliquidMappedDelta(
                idempotencyKey,
                positionKey,
                wallet,
                symbol,
                side,
                deltaType,
                new OperacionEvent(event.getTipo(), reboundOperation),
                request
        );
    }
}
