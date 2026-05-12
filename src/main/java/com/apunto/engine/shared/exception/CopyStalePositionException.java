package com.apunto.engine.shared.exception;

import java.util.Map;

public class CopyStalePositionException extends CopyExecutionException {

    public CopyStalePositionException(String message) {
        super("copy_stale_position", ErrorCode.BUSINESS_ERROR, message);
    }

    public CopyStalePositionException(String message, Map<String, Object> details) {
        super("copy_stale_position", ErrorCode.BUSINESS_ERROR, message, details);
    }

    public CopyStalePositionException(String message, Throwable cause, Map<String, Object> details) {
        super("copy_stale_position", ErrorCode.BUSINESS_ERROR, message, cause, details);
    }
}
