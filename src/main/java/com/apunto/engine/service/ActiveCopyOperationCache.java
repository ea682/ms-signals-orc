package com.apunto.engine.service;

import com.apunto.engine.dto.CopyOperationDto;

import java.util.List;
import java.util.Set;

public interface ActiveCopyOperationCache {

    boolean isActive(String originId, String userId);

    boolean isActive(String originId, String userId, Long allocationId, String strategyCode, String symbol, String typeOperation);

    boolean isKnown(String originId, String userId);

    boolean isKnown(String originId, String userId, Long allocationId, String strategyCode, String symbol, String typeOperation);

    CopyOperationDto activeOperation(String originId, String userId);

    CopyOperationDto activeOperation(String originId, String userId, Long allocationId, String strategyCode, String symbol, String typeOperation);

    List<CopyOperationDto> activeOperations(String originId, String userId);

    List<CopyOperationDto> activeOperationsByUserAndWallet(String userId, String walletId);

    Set<String> activeUserIds(String originId);

    Set<String> activeUserIdsByWallet(String walletId);

    Set<String> activeUserIdsByWalletAndSymbol(String walletId, String symbol);

    Set<String> activeUserIdsByWalletAndBaseSymbol(String walletId, String symbol);

    String traceId(String originId, String userId, String walletId, String symbol);

    String traceId(String originId, String userId, String walletId, String symbol, Long allocationId, String strategyCode);

    void markPendingOpen(String originId, String userId, String walletId, String symbol, String typeOperation, String traceId);

    void markPendingOpen(String originId, String userId, String walletId, String symbol, String typeOperation, Long allocationId, String strategyCode, String traceId);

    void markOpen(CopyOperationDto operation);

    void markUncertain(CopyOperationDto operation, String traceId, String reasonCode);

    void forgetPending(String originId, String userId, String traceId, String reasonCode);

    void markClosed(String originId, String userId);

    void markClosed(CopyOperationDto operation);

    int activeSize();
}
