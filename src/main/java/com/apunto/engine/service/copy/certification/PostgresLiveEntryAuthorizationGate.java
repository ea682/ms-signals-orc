package com.apunto.engine.service.copy.certification;

import com.apunto.engine.dto.OperationDto;
import com.apunto.engine.entity.UserCopyAllocationEntity;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.OffsetDateTime;

@Component
@RequiredArgsConstructor
@Slf4j
public class PostgresLiveEntryAuthorizationGate implements LiveEntryAuthorizationGate {

    private final LiveEntryAuthorizationRequestFactory requestFactory;
    private final LiveCertificationReadStore readStore;

    @Override
    public LiveEntryAuthorizationDecision evaluate(OperationDto operation, UserCopyAllocationEntity allocation) {
        LiveEntryAuthorizationContext context = requestFactory.create(operation, allocation);
        if (!context.valid()) {
            return LiveEntryAuthorizationDecision.blocked(context.reasonCode());
        }
        try {
            LiveEntryAuthorizationDecision decision =
                    new LiveEntryAuthorizationService(readStore).evaluate(context.request(), OffsetDateTime.now());
            log.info("event=copy.live.certification_gate decision={} reasonCode={} certificationId={} userId={} walletId={} allocationId={} strategyCode={} scopeType={} scopeValue={} capitalUsd={} leverage={} quoteAsset={}",
                    decision.allowed() ? "ALLOW" : "BLOCK", decision.reasonCode(), decision.certificationId(),
                    context.request().userId(), context.request().walletId(), context.request().allocationId(),
                    context.request().strategyCode(), context.request().scopeType(), context.request().scopeValue(),
                    context.request().allocatedCapitalUsd(), context.request().targetLeverage(),
                    context.request().quoteAsset());
            return decision;
        } catch (RuntimeException ex) {
            log.error("event=copy.live.certification_gate decision=BLOCK reasonCode=LIVE_CERTIFICATION_UNAVAILABLE userId={} walletId={} allocationId={} errorClass={} errorMessage=\"{}\"",
                    context.request().userId(), context.request().walletId(), context.request().allocationId(),
                    ex.getClass().getSimpleName(), safe(ex.getMessage()));
            return LiveEntryAuthorizationDecision.blocked("LIVE_CERTIFICATION_UNAVAILABLE");
        }
    }

    private static String safe(String value) {
        if (value == null) return "";
        String clean = value.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ').replace('"', '\'');
        return clean.length() > 300 ? clean.substring(0, 300) : clean;
    }
}
