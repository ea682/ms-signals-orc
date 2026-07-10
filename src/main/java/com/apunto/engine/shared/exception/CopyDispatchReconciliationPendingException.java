package com.apunto.engine.shared.exception;

public final class CopyDispatchReconciliationPendingException extends SkipExecutionException {

    public static final String REASON_CODE = "COPY_DISPATCH_RECONCILIATION_PENDING";

    public CopyDispatchReconciliationPendingException(String reason, String details) {
        super(REASON_CODE, reason, details);
    }

    public CopyDispatchReconciliationPendingException(String reason, String details, Throwable cause) {
        this(reason, details);
        if (cause != null) initCause(cause);
    }
}
