package com.apunto.engine.shared.exception;

import java.util.Map;

public class CopyNotionalTooSmallException extends CopyExecutionException {

    public CopyNotionalTooSmallException(String message) {
        super("copy_notional_too_small", ErrorCode.BUSINESS_ERROR, message);
    }

    public CopyNotionalTooSmallException(String message, Map<String, Object> details) {
        super("copy_notional_too_small", ErrorCode.BUSINESS_ERROR, message, details);
    }

    public CopyNotionalTooSmallException(String message, Throwable cause, Map<String, Object> details) {
        super("copy_notional_too_small", ErrorCode.BUSINESS_ERROR, message, cause, details);
    }
}
