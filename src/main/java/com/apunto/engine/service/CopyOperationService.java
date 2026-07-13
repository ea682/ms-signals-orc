package com.apunto.engine.service;

import com.apunto.engine.dto.CopyOperationDto;
import com.apunto.engine.entity.CopyOperationEntity;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface CopyOperationService {
    void newOperation(CopyOperationDto operation);
    void closeOperation(CopyOperationDto operation);

    CopyOperationDto findOperation(String idOrden);
    CopyOperationDto findOperationById(UUID idOperation);
    CopyOperationDto updateExecutionPriceEvidence(UUID idOperation, java.math.BigDecimal price,
                                                  String priceStatus, boolean closingPrice);

    List<CopyOperationDto> findOperationsByOrigin(String idOrderOrigin);
    Optional<CopyOperationEntity> findOperationByOrigin(String idOrderOrigin);
    CopyOperationDto findOperationForUser(String idOrderOrigin, String idUser);
    CopyOperationDto findOperationForAllocation(String idOrderOrigin, String idUser, Long allocationId, String strategyCode, String typeOperation);
    CopyOperationDto findLatestOperationForAllocation(String idOrderOrigin, String idUser, Long allocationId, String strategyCode, String typeOperation);
    boolean existsByOriginAndUser(String idOrderOrigin, String idUser);
    List<CopyOperationDto> findActiveOperationsForUserOrigin(String idOrderOrigin, String idUser);
    List<CopyOperationDto> findActiveOperationsByUserAndWallet(String idUser, String walletId);
    void upsertActiveOperation(CopyOperationDto operation);
    CopyOperationDto findOperationForUserAndType(String idOrderOrigin, String idUser, String typeOperation);
    java.math.BigDecimal sumBufferedMarginActive(String idUser, String walletId, java.math.BigDecimal safety);
    java.math.BigDecimal sumBufferedMarginActiveForUser(String idUser, java.math.BigDecimal safety);
}
