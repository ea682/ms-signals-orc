package com.apunto.engine.service.copy.account;

public record ExecutionAccountIsolationDecision(boolean isolated, String reasonCode) {

    static ExecutionAccountIsolationDecision allowed() {
        return new ExecutionAccountIsolationDecision(true, "EXECUTION_ACCOUNTS_ISOLATED");
    }

    static ExecutionAccountIsolationDecision blocked(String reasonCode) {
        return new ExecutionAccountIsolationDecision(false, reasonCode);
    }
}
