package com.apunto.engine.service.copy.certification;

import com.apunto.engine.entity.DetailUserEntity;
import com.apunto.engine.entity.MicroLiveRecertificationRequestEntity;
import com.apunto.engine.entity.UserApiKeyEntity;
import com.apunto.engine.repository.DetailUserRepository;
import com.apunto.engine.repository.UserApiKeyRepository;
import com.apunto.engine.repository.UserWalletCopyPreferenceRepository;
import com.apunto.engine.service.copy.account.ExecutionAccountPurpose;
import com.apunto.engine.service.copy.account.MicroLiveAdmissionPriority;
import com.apunto.engine.service.copy.account.MicroLiveCapacityDecision;
import com.apunto.engine.service.copy.account.MicroLiveCapacityGate;
import com.apunto.engine.service.copy.account.MicroLiveCapacityProperties;
import com.apunto.engine.service.copy.account.MicroLivePreemptionPolicy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
@RequiredArgsConstructor
@Slf4j
public class MicroLiveRecertificationProcessor {

    private final MicroLiveRecertificationQueueService queue;
    private final MicroLiveRecertificationAdmissionStore admissionStore;
    private final MicroLiveIdlePreemptionService idlePreemptionService;
    private final MicroLiveCapacityGate capacityGate;
    private final MicroLiveCapacityProperties capacityProperties;
    private final UserApiKeyRepository accountRepository;
    private final DetailUserRepository detailRepository;
    private final UserWalletCopyPreferenceRepository walletPreferenceRepository;
    private final LiveCertificationCatalogStore certificationCatalogStore;

    public boolean processNext() {
        Optional<MicroLiveRecertificationRequestEntity> claimed = queue.claimNext();
        if (claimed.isEmpty()) return false;
        MicroLiveRecertificationRequestEntity request = claimed.get();
        try {
            UserApiKeyEntity account = accountRepository.findById(request.getExecutionAccountId()).orElse(null);
            if (!usable(account, request)) {
                queue.finish(request.getId(), MicroLiveRecertificationRequestEntity.Status.INELIGIBLE,
                        "MICRO_LIVE_RECERTIFICATION_ACCOUNT_INELIGIBLE", null);
                return true;
            }
            DetailUserEntity detail = detailRepository.findByUser_Id(request.getUserId());
            if (!eligible(detail)) {
                queue.finish(request.getId(), MicroLiveRecertificationRequestEntity.Status.INELIGIBLE,
                        "MICRO_LIVE_RECERTIFICATION_USER_INELIGIBLE", null);
                return true;
            }
            if (walletPreferenceRepository.isBlocked(request.getUserId(), request.getWalletId())) {
                queue.finish(request.getId(), MicroLiveRecertificationRequestEntity.Status.CANCELLED,
                        "MICRO_LIVE_RECERTIFICATION_WALLET_BLOCKED", null);
                return true;
            }
            LiveCertificationIdentity identity = certificationCatalogStore
                    .findIdentityById(request.getCertificationId()).orElse(null);
            if (!matches(request, identity)) {
                queue.finish(request.getId(), MicroLiveRecertificationRequestEntity.Status.CANCELLED,
                        "MICRO_LIVE_RECERTIFICATION_IDENTITY_CHANGED", null);
                return true;
            }

            MicroLiveCapacityDecision capacity = capacityGate.evaluate(
                    account, request.getUserId(), detail.getCapitalAsset(),
                    MicroLiveAdmissionPriority.RECERTIFICATION);
            if (!capacity.allowed() && capacityProperties.getPreemptionPolicy() == MicroLivePreemptionPolicy.IDLE_ONLY
                    && "MICRO_LIVE_CAPACITY_EXHAUSTED".equals(capacity.reasonCode())
                    && idlePreemptionService.releaseOneIdle(account.getId())) {
                capacity = capacityGate.evaluate(account, request.getUserId(), detail.getCapitalAsset(),
                        MicroLiveAdmissionPriority.RECERTIFICATION);
            }
            if (!capacity.allowed()) {
                queue.defer(request.getId(), capacity.reasonCode());
                return true;
            }
            Long allocationId = admissionStore.admit(request.getId(), detail, identity);
            log.info("event=copy.micro_live.recertification.admitted requestId={} certificationId={} allocationId={} userId={} exchangeAccountId={} reasonCode=MICRO_LIVE_RECERTIFICATION_ADMITTED",
                    request.getId(), request.getCertificationId(), allocationId, request.getUserId(),
                    request.getExecutionAccountId());
            return true;
        } catch (RuntimeException error) {
            String reason = sqlReason(error);
            queue.defer(request.getId(), reason);
            log.warn("event=copy.micro_live.recertification.deferred requestId={} certificationId={} userId={} exchangeAccountId={} reasonCode={} errorClass={}",
                    request.getId(), request.getCertificationId(), request.getUserId(),
                    request.getExecutionAccountId(), reason, error.getClass().getSimpleName());
            return true;
        }
    }

    private static boolean usable(UserApiKeyEntity account, MicroLiveRecertificationRequestEntity request) {
        return account != null && account.isActive()
                && account.getAccountPurpose() == ExecutionAccountPurpose.MICRO_LIVE
                && account.getId().equals(request.getExecutionAccountId())
                && account.getUser() != null && request.getUserId().equals(account.getUser().getId())
                && account.getApiKey() != null && !account.getApiKey().isBlank()
                && account.getApiSecret() != null && !account.getApiSecret().isBlank();
    }

    private static boolean eligible(DetailUserEntity detail) {
        return detail != null && detail.getUser() != null && detail.getUser().isActivo()
                && detail.isUserActive() && detail.isApiKeyBinar() && detail.isParticipateInMicroLive();
    }

    private static boolean matches(MicroLiveRecertificationRequestEntity request,
                                   LiveCertificationIdentity identity) {
        return identity != null
                && request.getWalletId().equalsIgnoreCase(identity.walletId())
                && request.getStrategyCode().equalsIgnoreCase(identity.strategyCode())
                && request.getStrategyVersion().equalsIgnoreCase(
                        identity.strategyVersion() == null ? "UNVERSIONED" : identity.strategyVersion());
    }

    private static String sqlReason(Throwable error) {
        String message = error == null ? null : error.getMessage();
        for (String reason : new String[]{"MICRO_LIVE_SLOT_CONCURRENCY_LOST",
                "MICRO_LIVE_CAPACITY_EXHAUSTED", "MICRO_LIVE_ACCOUNT_UNDER_RESERVED",
                "MICRO_LIVE_RESERVED_CAPITAL_INSUFFICIENT",
                "MICRO_LIVE_CAPACITY_ACCOUNT_BALANCE_UNAVAILABLE"}) {
            if (message != null && message.contains(reason)) return reason;
        }
        return "MICRO_LIVE_SLOT_CONCURRENCY_LOST";
    }
}
