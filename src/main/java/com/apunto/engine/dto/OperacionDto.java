package com.apunto.engine.dto;

import com.apunto.engine.shared.enums.PositionSide;
import com.fasterxml.jackson.annotation.JsonAlias;
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
    /** Compat: futures_position DTO usa "id" */
    @JsonAlias({"id"})
    private final UUID idOperacion;

    /** Compat: futures_position DTO usa "accountId" */
    @JsonAlias({"accountId"})
    private final String idCuenta;

    /** Compat: futures_position DTO usa "symbol" */
    @JsonAlias({"symbol"})
    private final String parSymbol;

    /** Compat: futures_position DTO usa "side" */
    @JsonAlias({"side"})
    private final PositionSide tipoOperacion;

    /** Compat: futures_position DTO usa "sizeLegacy" */
    @JsonAlias({"sizeLegacy"})
    private final BigDecimal size;

    /** Compat: futures_position DTO usa "entryPrice" */
    @JsonAlias({"entryPrice"})
    private final BigDecimal precioEntrada;

    /** Compat: futures_position DTO usa "exitPrice" */
    @JsonAlias({"exitPrice"})
    private final BigDecimal precioCierre;

    /** Compat: futures_position DTO usa "markPrice" */
    @JsonAlias({"markPrice"})
    private final BigDecimal precioMercado;

    /** Compat: futures_position DTO usa "createdAt" */
    @JsonAlias({"createdAt"})
    private final Instant fechaCreacion;

    /** Compat: futures_position DTO usa "closedAt" */
    @JsonAlias({"closedAt"})
    private final Instant fechaCierre;

    /** Compat: futures_position DTO usa "active" */
    @JsonAlias({"active"})
    private final boolean operacionActiva;
}
