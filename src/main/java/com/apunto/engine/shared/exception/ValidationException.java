package com.apunto.engine.shared.exception;

import java.util.Map;


public class ValidationException extends EngineException {

    public ValidationException(String message) {
        super(ErrorCode.VALIDATION_ERROR, message);
    }

    public ValidationException(String message, Map<String, Object> details) {
        super(ErrorCode.VALIDATION_ERROR, message, details);
    }
}
