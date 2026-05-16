package com.apunto.engine.events;

import com.apunto.engine.dto.OperacionDto;
import com.fasterxml.jackson.annotation.JsonAlias;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class OperacionEvent {

    public enum Tipo {
        ABIERTA,
        CERRADA
    }

    private Tipo tipo;

    @JsonAlias("position")
    private OperacionDto operacion;

    /**
     * Delta original del fast path Hyperliquid: OPEN, RESIZE, UPDATE, CLOSE, FLIP, etc.
     * Permite aplicar reglas de lifecycle sin consultar la BD en el hot path.
     */
    private String deltaType;

    public OperacionEvent(Tipo tipo, OperacionDto operacion) {
        this.tipo = tipo;
        this.operacion = operacion;
    }
}
