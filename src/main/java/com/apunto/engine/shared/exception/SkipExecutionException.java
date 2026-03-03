package com.apunto.engine.shared.exception;

import com.apunto.engine.shared.util.LogFmt;

/**
 * Excepción controlada para "no ejecutar" (skip) por reglas de negocio.
 *
 * <p>Diseñada para observabilidad:
 * <ul>
 *   <li><b>reasonCode</b>: estable y de baja cardinalidad (ideal para métricas/alertas)</li>
 *   <li><b>reason</b>: mensaje humano</li>
 *   <li><b>details</b>: pares k=v (logfmt) con base numérica</li>
 * </ul>
 */
public class SkipExecutionException extends RuntimeException {

    private final String reasonCode;
    private final String reason;
    private final String details;

    /**
     * Backward-compatible: si solo hay message, lo tratamos como reason.
     */
    public SkipExecutionException(String message) {
        this("skip", message, null);
    }

    public SkipExecutionException(String reasonCode, String reason, String details) {
        super(buildMessage(reasonCode, reason, details));
        this.reasonCode = (reasonCode == null || reasonCode.isBlank()) ? "skip" : reasonCode.trim();
        this.reason = LogFmt.sanitize(reason == null ? "" : reason);
        this.details = LogFmt.sanitize(details);
    }

    public String getReasonCode() {
        return reasonCode;
    }

    public String getReason() {
        return reason;
    }

    public String getDetails() {
        return details;
    }

    private static String buildMessage(String code, String reason, String details) {
        String c = (code == null || code.isBlank()) ? "skip" : code.trim();
        String r = reason == null ? "" : LogFmt.sanitize(reason);
        String d = details == null ? null : LogFmt.sanitize(details);
        if (d == null || d.isBlank()) return "[" + c + "] " + r;
        return "[" + c + "] " + r + " | " + d;
    }
}
