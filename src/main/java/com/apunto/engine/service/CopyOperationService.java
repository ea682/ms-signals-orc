package com.apunto.engine.service;

import com.apunto.engine.dto.CopyOperationDto;
import com.apunto.engine.entity.CopyOperationEntity;

import java.util.List;
import java.util.Optional;

public interface CopyOperationService {
    void newOperation(CopyOperationDto operation);
    void closeOperation(CopyOperationDto operation);

    CopyOperationDto findOperation(String idOrden);

    List<CopyOperationDto> findOperationsByOrigin(String idOrderOrigin);
    Optional<CopyOperationEntity> findOperationByOrigin(String idOrderOrigin);
    CopyOperationDto findOperationForUser(String idOrderOrigin, String idUser);
    CopyOperationDto findOperationForUserAndStrategy(String idOrderOrigin, String idUser, String strategyCode);
    boolean existsByOriginAndUser(String idOrderOrigin, String idUser);
    List<CopyOperationDto> findActiveOperationsByUserAndWallet(String idUser, String walletId);
    void upsertActiveOperation(CopyOperationDto operation);
    CopyOperationDto findOperationForUserAndType(String idOrderOrigin, String idUser, String typeOperation);
    java.math.BigDecimal sumBufferedMarginActive(String idUser, String walletId, java.math.BigDecimal safety);
}
