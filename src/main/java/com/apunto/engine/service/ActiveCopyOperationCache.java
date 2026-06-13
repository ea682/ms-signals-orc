package com.apunto.engine.service;

import com.apunto.engine.dto.CopyOperationDto;

import java.util.List;
import java.util.Set;

public interface ActiveCopyOperationCache {

    boolean isActive(String originId, String userId);

    boolean isActive(String originId, String userId, String strategyCode);

    boolean isKnown(String originId, String userId);

    boolean isKnown(String originId, String userId, String strategyCode);

    CopyOperationDto activeOperation(String originId, String userId);

    CopyOperationDto activeOperation(String originId, String userId, String strategyCode);

    List<CopyOperationDto> activeOperations(String originId);

    List<CopyOperationDto> activeOperationsByUserAndWallet(String userId, String walletId);

    Set<String> activeUserIds(String originId);

    Set<String> activeUserIdsByWallet(String walletId);

    Set<String> activeUserIdsByWalletAndSymbol(String walletId, String symbol);

    Set<String> activeUserIdsByWalletAndBaseSymbol(String walletId, String symbol);

    String traceId(String originId, String userId, String walletId, String symbol);

    String traceId(String originId, String userId, String walletId, String symbol, String strategyCode);

    void markPendingOpen(String originId, String userId, String walletId, String symbol, String typeOperation, String traceId);

    void markPendingOpen(String originId, String userId, String walletId, String symbol, String typeOperation, String strategyCode, String traceId);

    void markOpen(CopyOperationDto operation);

    void markUncertain(CopyOperationDto operation, String traceId, String reasonCode);

    void forgetPending(String originId, String userId, String traceId, String reasonCode);

    void forgetPending(String originId, String userId, String strategyCode, String traceId, String reasonCode);

    void markClosed(String originId, String userId);

    void markClosed(String originId, String userId, String strategyCode);

    int activeSize();
}
