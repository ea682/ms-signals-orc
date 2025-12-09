package com.apunto.engine.shared.exception;

import java.util.Map;


public class ResourceNotFoundException extends EngineException {

    public ResourceNotFoundException(String message) {
        super(ErrorCode.RESOURCE_NOT_FOUND, message);
    }

    public ResourceNotFoundException(String message, Map<String, Object> details) {
        super(ErrorCode.RESOURCE_NOT_FOUND, message, details);
    }
}
