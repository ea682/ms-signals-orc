package com.apunto.engine.shared.enums;

/**
 * Politica para operaciones cuyo notional proporcional queda bajo el minimo de Binance.
 * SKIP es el default seguro. COPY_BY_BINANCE_MIN solo debe activarse explicitamente
 * por usuario o por wallet allocation.
 */
public enum CopyMinNotionalMode {
    /** Solo aplica en user_copy_allocation: hereda la politica de detail_user. */
    INHERIT,

    /** Comportamiento actual: no abrir si queda bajo el minimo de Binance. */
    SKIP,

    /** Eleva la orden al minimo permitido por Binance si pasa los filtros configurados. */
    COPY_BY_BINANCE_MIN;

    public boolean inherits() {
        return this == INHERIT;
    }

    public boolean copiesByBinanceMinimum() {
        return this == COPY_BY_BINANCE_MIN;
    }
}
