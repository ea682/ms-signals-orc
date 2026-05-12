package com.apunto.engine.shared.exception;

import java.util.Map;

public class CopyOrderRejectedException extends CopyExecutionException {

    public CopyOrderRejectedException(String message) {
        super("copy_order_rejected", ErrorCode.BINANCE_CLIENT_ERROR, message);
    }

    public CopyOrderRejectedException(String message, Map<String, Object> details) {
        super("copy_order_rejected", ErrorCode.BINANCE_CLIENT_ERROR, message, details);
    }

    public CopyOrderRejectedException(String message, Throwable cause, Map<String, Object> details) {
        super("copy_order_rejected", ErrorCode.BINANCE_CLIENT_ERROR, message, cause, details);
    }
}
