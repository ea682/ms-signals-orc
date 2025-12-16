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
}
