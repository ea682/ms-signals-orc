package com.apunto.engine.shared.exception;

import java.util.Map;

public class FuturesWalletClientException extends EngineException {

    public FuturesWalletClientException(String message, Map<String, Object> details) {
        super(ErrorCode.BINANCE_CLIENT_ERROR, message, details);
    }

    public FuturesWalletClientException(String message, Throwable cause, Map<String, Object> details) {
        super(ErrorCode.BINANCE_CLIENT_ERROR, message, cause, details);
    }
}
