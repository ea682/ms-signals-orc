package com.apunto.engine.service.copy.decision;

import com.apunto.engine.client.MetricWalletsInfoClient;
import com.apunto.engine.dto.client.CopyDecisionDto;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class MetricCopyDecisionGateway implements CopyDecisionGateway {

    private final MetricWalletsInfoClient metricWalletsInfoClient;

    @Override
    public CopyDecisionDto getFullDecisionExact(CopyDecisionRequest request) {
        return metricWalletsInfoClient.copyDecision(
                request.walletId(),
                request.strategyCode(),
                request.scopeType(),
                request.scopeValue(),
                request.mode(),
                request.simulation(),
                request.minHistoryDays(),
                request.simulationLookbackDays(),
                request.maxFactsPerUnit(),
                request.timeoutMs(),
                request.debug()
        );
    }
}
