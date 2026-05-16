package com.apunto.engine.service;

import com.apunto.engine.dto.CopyOperationDto;

import java.util.Set;

public interface ActiveCopyOperationCache {

    boolean isActive(String originId, String userId);

    Set<String> activeUserIds(String originId);

    Set<String> activeUserIdsByWallet(String walletId);

    Set<String> activeUserIdsByWalletAndSymbol(String walletId, String symbol);

    void markOpen(CopyOperationDto operation);

    void markClosed(String originId, String userId);

    int activeSize();
}
