package com.apunto.engine.hyperliquid.exception;

import com.apunto.engine.shared.exception.EngineException;
import com.apunto.engine.shared.exception.ErrorCode;

import java.util.Map;

public class HyperliquidDirectIngestRejectedException extends EngineException {

    public HyperliquidDirectIngestRejectedException(String message, Map<String, Object> details) {
        super(ErrorCode.BACKPRESSURE, message, details);
    }
}
