package com.apunto.engine.service.copy.decision;

import com.apunto.engine.dto.client.CopyDecisionDto;

public interface CopyDecisionGateway {

    CopyDecisionDto getFullDecisionExact(CopyDecisionRequest request);
}
