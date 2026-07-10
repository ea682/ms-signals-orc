package com.apunto.engine.events;

import com.apunto.engine.dto.OperacionDto;
import com.fasterxml.jackson.annotation.JsonAlias;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
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

    /** Immutable upstream delta identity, normally Hyperliquid's idempotency key. */
    private String sourceEventId;

    /** Upstream lifecycle position key, retained only for audit/context. */
    private String sourcePositionKey;

    public OperacionEvent(Tipo tipo, OperacionDto operacion) {
        this.tipo = tipo;
        this.operacion = operacion;
    }

    public OperacionEvent(Tipo tipo, OperacionDto operacion, String deltaType) {
        this(tipo, operacion, deltaType, null, null);
    }

    public OperacionEvent(Tipo tipo, OperacionDto operacion, String deltaType,
                          String sourceEventId, String sourcePositionKey) {
        this.tipo = tipo;
        this.operacion = operacion;
        this.deltaType = deltaType;
        this.sourceEventId = sourceEventId;
        this.sourcePositionKey = sourcePositionKey;
    }
}
