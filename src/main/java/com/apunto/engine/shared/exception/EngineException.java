package com.apunto.engine.shared.exception;

import lombok.Getter;
import org.springframework.lang.Nullable;

import java.util.Collections;
import java.util.Map;

/**
 * Excepción base de la app.
 * Extiende RuntimeException para no obligar a throws en todos lados.
 */
@Getter
public class EngineException extends RuntimeException {

    private final ErrorCode errorCode;

    /**
     * Datos extras que quieras exponer en el ApiResponse.data
     * (por ejemplo: campo inválido, symbol, apiKey mascarada, etc).
     */
    @Nullable
    private final Map<String, Object> details;

    public EngineException(ErrorCode errorCode) {
        this(errorCode, null, null, null);
    }

    public EngineException(ErrorCode errorCode, String message) {
        this(errorCode, message, null, null);
    }

    public EngineException(ErrorCode errorCode, String message, Throwable cause) {
        this(errorCode, message, cause, null);
    }

    public EngineException(ErrorCode errorCode,
                           String message,
                           Map<String, Object> details) {
        this(errorCode, message, null, details);
    }

    public EngineException(ErrorCode errorCode,
                           String message,
                           Throwable cause,
                           Map<String, Object> details) {
        super(message != null ? message : errorCode.getDefaultMessage(), cause);
        this.errorCode = errorCode;
        this.details = details != null ? Collections.unmodifiableMap(details) : null;
    }
}
