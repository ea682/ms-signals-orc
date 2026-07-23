package com.apunto.engine.service.copy.account;

import com.apunto.engine.entity.UserApiKeyEntity;

import java.util.UUID;

public record ExecutionAccountResolution(
        boolean allowed,
        String reasonCode,
        ExecutionAccountPurpose accountPurpose,
        UserApiKeyEntity account
) {
    public static ExecutionAccountResolution allowed(ExecutionAccountPurpose purpose,
                                                     UserApiKeyEntity account) {
        return new ExecutionAccountResolution(true, "EXECUTION_ACCOUNT_RESOLVED", purpose, account);
    }

    public static ExecutionAccountResolution blocked(ExecutionAccountPurpose purpose,
                                                     String reasonCode) {
        return new ExecutionAccountResolution(false, reasonCode, purpose, null);
    }

    public UUID exchangeAccountId() {
        return account == null ? null : account.getId();
    }
}
