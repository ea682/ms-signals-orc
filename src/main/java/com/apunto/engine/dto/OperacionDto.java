package com.apunto.engine.dto;

import com.apunto.engine.shared.enums.PositionSide;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Data
@AllArgsConstructor
@NoArgsConstructor(force = true)
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class OperacionDto {
    private final UUID idOperacion;
    private final String idCuenta;
    private final String parSymbol;
    private final PositionSide tipoOperacion;
    private final BigDecimal size;
    private final BigDecimal precioEntrada;
    private final BigDecimal precioCierre;
    private final BigDecimal precioMercado;
    private final Instant fechaCreacion;
    private final Instant fechaCierre;
    private final boolean operacionActiva;
}
