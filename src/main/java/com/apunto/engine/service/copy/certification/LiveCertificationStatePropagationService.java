package com.apunto.engine.service.copy.certification;

import com.apunto.engine.entity.DetailUserEntity;
import com.apunto.engine.entity.UserApiKeyEntity;
import com.apunto.engine.entity.UserCopyAllocationEntity;
import com.apunto.engine.repository.DetailUserRepository;
import com.apunto.engine.repository.UserApiKeyRepository;
import com.apunto.engine.repository.UserCopyAllocationRepository;
import com.apunto.engine.repository.UserWalletCopyPreferenceRepository;
import com.apunto.engine.service.copy.allocation.LiveAllocationPercentageRequest;
import com.apunto.engine.service.copy.allocation.LiveAllocationPercentageResolution;
import com.apunto.engine.service.copy.allocation.LiveAllocationPercentageResolver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;
import com.apunto.engine.service.copy.account.ExecutionAccountPurpose;

@Service
@RequiredArgsConstructor
@Slf4j
public class LiveCertificationStatePropagationService implements LiveCertificationStatePropagation {

    private final UserCopyAllocationRepository allocationRepository;
    private final DetailUserRepository detailRepository;
    private final UserApiKeyRepository apiKeyRepository;
    private final UserWalletCopyPreferenceRepository walletPreferenceRepository;
    private final LiveCertificationCatalogStore certificationCatalogStore;
    private final LiveAllocationPercentageResolver percentageResolver;
    private final LiveAllocationSafetyTransitionService safetyTransitionService;
    private final MicroLiveRecertificationQueue recertificationQueue;

    @Override
    @Transactional
    public void propagate(UUID certificationId, LiveCertificationStatus nextStatus, String reasonCode) {
        if (certificationId == null || nextStatus == null) return;
        OffsetDateTime now = OffsetDateTime.now();
        if (nextStatus == LiveCertificationStatus.LIVE_DEGRADED
                || nextStatus == LiveCertificationStatus.SUSPENDED
                || nextStatus == LiveCertificationStatus.REVOKED) {
            safetyTransitionService.markExitOnly(
                    certificationId, "LIVE_CERTIFICATION_" + nextStatus.name(), now);
            if (nextStatus != LiveCertificationStatus.REVOKED) {
                provisionEligibleMicroRecertificationParticipants(certificationId, now);
            }
            return;
        }
        if (nextStatus == LiveCertificationStatus.LIVE_APPROVED) {
            safetyTransitionService.markPendingRevalidation(
                    certificationId, "LIVE_RECERTIFICATION_USER_REVALIDATION_REQUIRED", now);
            provisionEligibleAutoFollowers(certificationId, now);
        }
    }

    private void provisionEligibleMicroRecertificationParticipants(UUID certificationId, OffsetDateTime now) {
        LiveCertificationIdentity identity = certificationCatalogStore.findIdentityById(certificationId).orElse(null);
        if (identity == null) {
            log.warn("event=copy.live_recertification.skipped certificationId={} reasonCode=LIVE_CERTIFICATION_IDENTITY_MISSING",
                    certificationId);
            return;
        }
        for (DetailUserEntity detail : detailRepository.findEligibleMicroLiveUsers()) {
            createMicroRecertificationIfEligible(detail, identity, certificationId, now);
        }
    }

    private void provisionEligibleAutoFollowers(UUID certificationId, OffsetDateTime now) {
        LiveCertificationIdentity identity = certificationCatalogStore.findIdentityById(certificationId).orElse(null);
        if (identity == null) {
            log.warn("event=copy.live_auto_follow.skipped certificationId={} reasonCode=LIVE_CERTIFICATION_IDENTITY_MISSING",
                    certificationId);
            return;
        }
        for (DetailUserEntity detail : detailRepository.findEligibleAutoFollowCertifiedLiveUsers()) {
            provisionEligibleAutoFollower(detail, certificationId, identity, now);
        }
    }

    private void provisionEligibleAutoFollower(DetailUserEntity detail,
                                                UUID certificationId,
                                                LiveCertificationIdentity identity,
                                                OffsetDateTime now) {
        if (detail == null || detail.getUser() == null || detail.getUser().getId() == null) return;
        UUID userId = detail.getUser().getId();
        UserApiKeyEntity key = executionAccount(userId, ExecutionAccountPurpose.LIVE);
        if (key == null || blank(key.getApiKey()) || blank(key.getApiSecret())) return;
        if (walletPreferenceRepository.isBlocked(userId, identity.walletId())) return;
        if (allocationRepository.findOpenAllocationForUserWalletStrategyScopeAndMode(
                userId, identity.walletId(), identity.strategyCode(), identity.scopeType(),
                identity.scopeValue(), "LIVE").isPresent()) return;
        // A current Micro-live participant is completed by the Micro promotion transaction.
        // Creating a second row here would race that transaction and could bind adoption to the wrong allocation.
        if (allocationRepository.findOpenAllocationForUserWalletStrategyScopeAndMode(
                userId, identity.walletId(), identity.strategyCode(), identity.scopeType(),
                identity.scopeValue(), "MICRO_LIVE").isPresent()) return;

        LiveAllocationPercentageResolution resolution = percentageResolver.resolve(
                new LiveAllocationPercentageRequest(userId, identity.walletId(), identity.strategyCode(),
                        identity.scopeType(), identity.scopeValue(), now));
        if (!resolution.validForLive()) {
            log.info("event=copy.live_auto_follow.skipped certificationId={} userId={} walletId={} strategyCode={} reasonCode={}",
                    certificationId, userId, identity.walletId(), identity.strategyCode(), resolution.reasonCode());
            return;
        }

        UserCopyAllocationEntity live = allocationRepository.saveAndFlush(UserCopyAllocationEntity.builder()
                .idUser(userId)
                .walletId(identity.walletId())
                .copyStrategyCode(identity.strategyCode())
                .copyMode(identity.strategyCode())
                .sizingMode("PERCENTAGE")
                .allocationPct(resolution.percentage())
                .allocationPctSource(resolution.source())
                .allocationPctSourceId(resolution.sourceId())
                .allocationPctCalculatedAt(resolution.calculatedAt())
                .allocationPctValidUntil(resolution.validUntil())
                .walletTotalAllocationPct(resolution.walletTotalPercentage())
                .status(UserCopyAllocationEntity.Status.PAUSED)
                .isActive(true)
                .executionMode("LIVE")
                .statusReason("LIVE_ADOPTION_VALIDATION_REQUIRED")
                .statusUpdatedAt(now)
                .updatedAt(now)
                .activationAt(now)
                .leverageOverride(identity.targetLeverage())
                .scopeType(identity.scopeType())
                .scopeValue(identity.scopeValue())
                .capitalAsset(detail.getCapitalAsset())
                .resolvedQuoteAsset(identity.quoteAsset())
                .liveCertificationId(certificationId)
                .exchangeAccountId(key.getId())
                .build());
        log.info("event=copy.live_auto_follow.provisioned certificationId={} allocationId={} userId={} walletId={} strategyCode={} status=PAUSED reasonCode=LIVE_ADOPTION_VALIDATION_REQUIRED",
                certificationId, live.getId(), userId, identity.walletId(), identity.strategyCode());
    }

    private void createMicroRecertificationIfEligible(DetailUserEntity detail,
                                                       LiveCertificationIdentity identity,
                                                       UUID certificationId,
                                                       OffsetDateTime now) {
        if (detail == null || detail.getUser() == null || detail.getUser().getId() == null
                || !detail.getUser().isActivo() || !detail.isUserActive() || !detail.isParticipateInMicroLive()
                || !detail.isApiKeyBinar() || detail.getCapital() == null || detail.getCapital() < 100) return;
        UUID userId = detail.getUser().getId();
        UserApiKeyEntity key = executionAccount(userId, ExecutionAccountPurpose.MICRO_LIVE);
        if (key == null || blank(key.getApiKey()) || blank(key.getApiSecret())) return;
        if (walletPreferenceRepository.isBlocked(userId, identity.walletId())) return;
        if (allocationRepository.findOpenAllocationForUserWalletStrategyScopeAndMode(
                userId, identity.walletId(), identity.strategyCode(),
                identity.scopeType(), identity.scopeValue(), "MICRO_LIVE").isPresent()) return;

        recertificationQueue.enqueue(new MicroLiveRecertificationRequest(
                certificationId, identity.walletId(), identity.strategyCode(), identity.strategyVersion(),
                userId, key.getId(), 100, "MICRO_LIVE_RECERTIFICATION_PENDING_CAPACITY"));
    }

    private UserApiKeyEntity executionAccount(UUID userId, ExecutionAccountPurpose purpose) {
        return apiKeyRepository.findByUser_IdAndExchangeIgnoreCaseAndAccountPurposeAndActiveTrue(
                userId, "BINANCE", purpose).orElse(null);
    }

    private static boolean blank(String value) {
        return value == null || value.isBlank();
    }
}
