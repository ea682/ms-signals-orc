package com.apunto.engine.shared.exception;

import java.util.Map;

public class CopyBinanceClientException extends CopyExecutionException {

    public CopyBinanceClientException(String message) {
        super("copy_binance_client_error", ErrorCode.BINANCE_CLIENT_ERROR, message);
    }

    public CopyBinanceClientException(String message, Map<String, Object> details) {
        super("copy_binance_client_error", ErrorCode.BINANCE_CLIENT_ERROR, message, details);
    }

    public CopyBinanceClientException(String message, Throwable cause, Map<String, Object> details) {
        super("copy_binance_client_error", ErrorCode.BINANCE_CLIENT_ERROR, message, cause, details);
    }
}
