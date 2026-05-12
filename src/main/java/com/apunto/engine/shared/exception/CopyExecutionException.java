package com.apunto.engine.shared.exception;

import java.util.Map;

public class CopyExecutionException extends EngineException {

    private final String errCode;

    public CopyExecutionException(String errCode, ErrorCode errorCode, String message) {
        super(errorCode, message);
        this.errCode = errCode;
    }

    public CopyExecutionException(String errCode, ErrorCode errorCode, String message, Map<String, Object> details) {
        super(errorCode, message, details);
        this.errCode = errCode;
    }

    public CopyExecutionException(String errCode, ErrorCode errorCode, String message, Throwable cause, Map<String, Object> details) {
        super(errorCode, message, cause, details);
        this.errCode = errCode;
    }

    public String getErrCode() {
        return errCode;
    }
}
