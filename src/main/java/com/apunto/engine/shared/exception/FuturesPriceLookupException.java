package com.apunto.engine.shared.exception;

import java.util.Map;

public class FuturesPriceLookupException extends EngineException {

    public FuturesPriceLookupException(String message, Map<String, Object> details) {
        super(ErrorCode.EXTERNAL_SERVICE_ERROR, message, details);
    }

    public FuturesPriceLookupException(String message, Throwable cause, Map<String, Object> details) {
        super(ErrorCode.EXTERNAL_SERVICE_ERROR, message, cause, details);
    }
}
