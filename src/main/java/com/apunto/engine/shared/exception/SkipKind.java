package com.apunto.engine.shared.exception;

/**
 * Clasificación de un SKIP.
 *
 * <p>Objetivo: permitir diferenciar reglas de negocio (esperadas) vs problemas
 * de validación/configuración/concurrencia/dependencias.
 *
 * <p>Importante: usar valores de baja cardinalidad para dashboards.
 */
public enum SkipKind {

    /**
     * Regla de negocio esperada (no es error).
     * Ej: notional demasiado pequeño, presupuesto insuficiente, copy_missing.
     */
    BUSINESS_RULE,

    /**
     * Configuración faltante o inválida.
     * Ej: apiKey/secret faltante, usuario no está en cache, no hay métrica.
     */
    CONFIG,

    /**
     * Validación de datos de entrada.
     * Ej: entryPrice <= 0, symbol vacío, qty inválida.
     */
    VALIDATION,

    /**
     * Problemas de lock/concurrencia.
     */
    CONCURRENCY,

    /**
     * Dependencia upstream/caches no disponibles.
     * Ej: rules del símbolo no están en cache.
     */
    UPSTREAM,

    /**
     * No clasificado.
     */
    UNKNOWN
}
