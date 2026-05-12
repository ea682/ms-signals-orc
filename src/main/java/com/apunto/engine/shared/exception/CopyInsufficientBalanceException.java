package com.apunto.engine.shared.exception;

import java.util.Map;

public class CopyInsufficientBalanceException extends CopyExecutionException {

    public CopyInsufficientBalanceException(String message) {
        super("copy_insufficient_balance", ErrorCode.BUSINESS_ERROR, message);
    }

    public CopyInsufficientBalanceException(String message, Map<String, Object> details) {
        super("copy_insufficient_balance", ErrorCode.BUSINESS_ERROR, message, details);
    }

    public CopyInsufficientBalanceException(String message, Throwable cause, Map<String, Object> details) {
        super("copy_insufficient_balance", ErrorCode.BUSINESS_ERROR, message, cause, details);
    }
}
