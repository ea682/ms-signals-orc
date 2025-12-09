package com.apunto.engine.service;

import com.apunto.engine.dto.CopyOperationDto;

public interface CopyOperationService {
    void newOperation(CopyOperationDto operation);
    void closeOperation(CopyOperationDto operation);
    CopyOperationDto findOperation(String idOperacion);
}
