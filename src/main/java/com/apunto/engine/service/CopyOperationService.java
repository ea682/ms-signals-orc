package com.apunto.engine.service;

import com.apunto.engine.dto.CopyOperationDto;

import java.util.List;

public interface CopyOperationService {
    void newOperation(CopyOperationDto operation);
    void closeOperation(CopyOperationDto operation);

    CopyOperationDto findOperation(String idOrden);

    List<CopyOperationDto> findOperationsByOrigin(String idOrderOrigin);
    CopyOperationDto findOperationForUser(String idOrderOrigin, String idUser);
    boolean existsByOriginAndUser(String idOrderOrigin, String idUser);

    /**
     * Suma de margen usado (buffer incluido) para posiciones activas de un usuario por wallet.
     * Se usa como base para presupuesto en entornos con múltiples réplicas.
     */
    java.math.BigDecimal sumBufferedMarginActive(String idUser, String walletId, java.math.BigDecimal safety);
}
