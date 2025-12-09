package com.apunto.engine.shared.exception;

import java.util.Map;


public class BinanceRateLimitException extends EngineException {

    public BinanceRateLimitException(String message) {
        super(ErrorCode.BINANCE_RATE_LIMIT, message);
    }

    public BinanceRateLimitException(String message, Map<String, Object> details) {
        super(ErrorCode.BINANCE_RATE_LIMIT, message, details);
    }

    public BinanceRateLimitException(String message, Throwable cause) {
        super(ErrorCode.BINANCE_RATE_LIMIT, message, cause);
    }

    public BinanceRateLimitException(String message,
                                     Throwable cause,
                                     Map<String, Object> details) {
        super(ErrorCode.BINANCE_RATE_LIMIT, message, cause, details);
    }
}
