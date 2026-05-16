package com.apunto.engine.hyperliquid.exception;

import com.apunto.engine.shared.exception.EngineException;
import com.apunto.engine.shared.exception.ErrorCode;

import java.util.Map;

public class HyperliquidOriginLifecycleException extends EngineException {

    public HyperliquidOriginLifecycleException(String message, Map<String, Object> details) {
        super(ErrorCode.BUSINESS_ERROR, message, details);
    }
}
