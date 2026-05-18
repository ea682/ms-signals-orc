package com.apunto.engine.shared.exception;

import java.util.Map;

public class CopyPersistenceConflictException extends EngineException {

    public CopyPersistenceConflictException(String message, Map<String, Object> details) {
        super(ErrorCode.INTERNAL_ERROR, message, details);
    }

    public CopyPersistenceConflictException(String message, Throwable cause, Map<String, Object> details) {
        super(ErrorCode.INTERNAL_ERROR, message, cause, details);
    }
}
