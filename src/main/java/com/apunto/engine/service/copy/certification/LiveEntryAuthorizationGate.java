package com.apunto.engine.service.copy.certification;

import com.apunto.engine.dto.OperationDto;
import com.apunto.engine.entity.UserCopyAllocationEntity;

@FunctionalInterface
public interface LiveEntryAuthorizationGate {
    LiveEntryAuthorizationDecision evaluate(OperationDto operation, UserCopyAllocationEntity allocation);
}
