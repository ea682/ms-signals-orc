package com.apunto.engine.shared.exception;

import java.util.Map;

public class BinanceNetworkException extends EngineException {

    public BinanceNetworkException(String message) {
        super(ErrorCode.BINANCE_CLIENT_ERROR, message);
    }

    public BinanceNetworkException(String message, Map<String, Object> details) {
        super(ErrorCode.BINANCE_CLIENT_ERROR, message, details);
    }

    public BinanceNetworkException(String message, Throwable cause) {
        super(ErrorCode.BINANCE_CLIENT_ERROR, message, cause);
    }

    public BinanceNetworkException(String message, Throwable cause, Map<String, Object> details) {
        super(ErrorCode.BINANCE_CLIENT_ERROR, message, cause, details);
    }
}

