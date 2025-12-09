package com.apunto.engine.shared.exception;

import java.util.Map;


public class ExternalServiceException extends EngineException {

    public ExternalServiceException(String message) {
        super(ErrorCode.EXTERNAL_SERVICE_ERROR, message);
    }

    public ExternalServiceException(String message, Map<String, Object> details) {
        super(ErrorCode.EXTERNAL_SERVICE_ERROR, message, details);
    }

    public ExternalServiceException(String message, Throwable cause) {
        super(ErrorCode.EXTERNAL_SERVICE_ERROR, message, cause);
    }
}
