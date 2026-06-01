package com.apunto.engine.hyperliquid.exception;

import com.apunto.engine.shared.exception.EngineException;
import com.apunto.engine.shared.exception.ErrorCode;

import java.util.Map;

public class HyperliquidDirectIngestDedupeException extends EngineException {

    public HyperliquidDirectIngestDedupeException(String message, Throwable cause, Map<String, Object> details) {
        super(ErrorCode.EXTERNAL_SERVICE_ERROR, message, cause, details);
    }
}
