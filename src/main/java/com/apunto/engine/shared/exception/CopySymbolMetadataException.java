package com.apunto.engine.shared.exception;

import java.util.Map;

public class CopySymbolMetadataException extends CopyExecutionException {

    public CopySymbolMetadataException(String message) {
        super("copy_symbol_metadata_error", ErrorCode.EXTERNAL_SERVICE_ERROR, message);
    }

    public CopySymbolMetadataException(String message, Map<String, Object> details) {
        super("copy_symbol_metadata_error", ErrorCode.EXTERNAL_SERVICE_ERROR, message, details);
    }

    public CopySymbolMetadataException(String message, Throwable cause, Map<String, Object> details) {
        super("copy_symbol_metadata_error", ErrorCode.EXTERNAL_SERVICE_ERROR, message, cause, details);
    }
}
