package com.apunto.engine.shared.exception;

import java.util.Map;

public class BinanceApiReadinessException extends CopyExecutionException {

    public static final String REASON_CODE = "BINANCE_API_KEY_INVALID_OR_FORBIDDEN";

    public BinanceApiReadinessException(String message, Throwable cause, Map<String, Object> details) {
        super(REASON_CODE, ErrorCode.BINANCE_CLIENT_ERROR, message, cause, details);
    }
}
