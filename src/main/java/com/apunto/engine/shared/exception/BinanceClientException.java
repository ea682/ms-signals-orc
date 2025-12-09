package com.apunto.engine.shared.exception;

import java.util.Map;


public class BinanceClientException extends EngineException {

    public BinanceClientException(String message) {
        super(ErrorCode.BINANCE_CLIENT_ERROR, message);
    }

    public BinanceClientException(String message, Map<String, Object> details) {
        super(ErrorCode.BINANCE_CLIENT_ERROR, message, details);
    }

    public BinanceClientException(String message, Throwable cause) {
        super(ErrorCode.BINANCE_CLIENT_ERROR, message, cause);
    }

    public BinanceClientException(String message,
                                  Throwable cause,
                                  Map<String, Object> details) {
        super(ErrorCode.BINANCE_CLIENT_ERROR, message, cause, details);
    }
}
