package com.apunto.engine.service.copy.certification;

import com.apunto.engine.entity.DetailUserEntity;
import com.apunto.engine.entity.MicroLiveRecertificationRequestEntity;
import com.apunto.engine.entity.UserCopyAllocationEntity;
import com.apunto.engine.repository.MicroLiveRecertificationRequestRepository;
import com.apunto.engine.repository.UserCopyAllocationRepository;
import com.apunto.engine.service.copy.account.MicroLiveCapacityProperties;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class MicroLiveRecertificationAdmissionStore {

    private static final BigDecimal MICRO_LEVERAGE = new BigDecimal("5");

    private final MicroLiveRecertificationRequestRepository requestRepository;
    private final UserCopyAllocationRepository allocationRepository;
    private final MicroLiveCapacityProperties capacityProperties;

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public Long admit(UUID requestId, DetailUserEntity detail, LiveCertificationIdentity identity) {
        MicroLiveRecertificationRequestEntity request = requestRepository.findById(requestId).orElseThrow();
        if (request.getStatus() != MicroLiveRecertificationRequestEntity.Status.CLAIMED) {
            return request.getUserCopyAllocationId();
        }
        UserCopyAllocationEntity existing = allocationRepository
                .findOpenAllocationForUserWalletStrategyScopeAndMode(
                        request.getUserId(), identity.walletId(), identity.strategyCode(),
                        identity.scopeType(), identity.scopeValue(), "MICRO_LIVE")
                .orElse(null);
        UserCopyAllocationEntity allocation = existing;
        if (allocation == null) {
            OffsetDateTime now = OffsetDateTime.now();
            allocation = allocationRepository.saveAndFlush(UserCopyAllocationEntity.builder()
                    .idUser(request.getUserId())
                    .walletId(identity.walletId())
                    .copyStrategyCode(identity.strategyCode())
                    .copyMode(identity.strategyCode())
                    .sizingMode("FIXED_CAPITAL")
                    .allocationPctSource("FIXED_MICRO_BUDGET")
                    .status(UserCopyAllocationEntity.Status.ACTIVE)
                    .isActive(true)
                    .executionMode("MICRO_LIVE")
                    .statusReason("MICRO_LIVE_RECERTIFICATION_ACTIVE")
                    .statusUpdatedAt(now)
                    .updatedAt(now)
                    .activationAt(now)
                    .leverageOverride(MICRO_LEVERAGE)
                    .scopeType(identity.scopeType())
                    .scopeValue(identity.scopeValue())
                    .promotedFromShadowAt(now)
                    .capitalAsset(detail.getCapitalAsset())
                    .resolvedQuoteAsset(identity.quoteAsset())
                    .liveCertificationId(request.getCertificationId())
                    .exchangeAccountId(request.getExecutionAccountId())
                    .reservedCapitalUsd(capacityProperties.getBudgetPerAllocationUsdc())
                    .build());
        }
        request.setStatus(MicroLiveRecertificationRequestEntity.Status.ADMITTED);
        request.setReasonCode("MICRO_LIVE_RECERTIFICATION_ADMITTED");
        request.setUserCopyAllocationId(allocation.getId());
        request.setCompletedAt(OffsetDateTime.now());
        requestRepository.saveAndFlush(request);
        return allocation.getId();
    }
}
