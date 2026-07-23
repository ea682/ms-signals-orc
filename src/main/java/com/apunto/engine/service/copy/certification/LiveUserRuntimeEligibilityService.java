package com.apunto.engine.service.copy.certification;

import com.apunto.engine.entity.DetailUserEntity;
import com.apunto.engine.entity.UserApiKeyEntity;
import com.apunto.engine.entity.UserCopyAllocationEntity;
import com.apunto.engine.repository.DetailUserRepository;
import com.apunto.engine.repository.UserApiKeyRepository;
import com.apunto.engine.repository.UserWalletCopyPreferenceRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import com.apunto.engine.service.copy.account.ExecutionAccountPurpose;

import java.time.OffsetDateTime;

@Service
@RequiredArgsConstructor
public class LiveUserRuntimeEligibilityService implements LiveUserRuntimeEligibilityGate {

    private final DetailUserRepository detailRepository;
    private final UserApiKeyRepository apiKeyRepository;
    private final UserWalletCopyPreferenceRepository walletPreferenceRepository;

    @Override
    public Decision evaluate(UserCopyAllocationEntity allocation) {
        return evaluate(allocation, true);
    }

    @Override
    public Decision evaluateForActivation(UserCopyAllocationEntity allocation) {
        return evaluate(allocation, false);
    }

    private Decision evaluate(UserCopyAllocationEntity allocation, boolean requireCurrentlyOpenable) {
        if (allocation == null || allocation.getIdUser() == null || allocation.getWalletId() == null) {
            return Decision.block("LIVE_USER_ALLOCATION_INVALID");
        }
        if (requireCurrentlyOpenable && !allocation.allowsNewEntries(OffsetDateTime.now())) {
            return Decision.block("LIVE_ALLOCATION_NOT_OPENABLE");
        }
        if (allocation.getLiveCertificationId() == null) {
            return Decision.block("LIVE_ALLOCATION_CERTIFICATION_ID_MISSING");
        }
        if (walletPreferenceRepository.isBlocked(allocation.getIdUser(), allocation.getWalletId())) {
            return Decision.block("COPY_WALLET_BLOCKED_BY_USER");
        }
        DetailUserEntity detail = detailRepository.findByUser_Id(allocation.getIdUser());
        if (detail == null || !detail.isUserActive()) {
            return Decision.block("LIVE_USER_INACTIVE");
        }
        if (!detail.isAutoFollowCertifiedLive() && !detail.isContinueInLiveAfterCertification()) {
            return Decision.block("LIVE_USER_OPT_IN_REQUIRED");
        }
        if (!detail.isApiKeyBinar()) {
            return Decision.block("LIVE_BINANCE_API_DISABLED");
        }
        UserApiKeyEntity key = apiKeyRepository
                .findByUser_IdAndExchangeIgnoreCaseAndAccountPurposeAndActiveTrue(
                        allocation.getIdUser(), "BINANCE", ExecutionAccountPurpose.LIVE)
                .orElse(null);
        if (key == null || blank(key.getApiKey()) || blank(key.getApiSecret())) {
            return Decision.block("LIVE_BINANCE_API_INVALID");
        }
        if (detail.getCapital() == null || detail.getCapital() <= 0
                || detail.getLeverage() == null || detail.getLeverage() <= 0
                || detail.getMaxWallet() == null || detail.getMaxWallet() <= 0) {
            return Decision.block("LIVE_USER_RISK_CONFIGURATION_INVALID");
        }
        return Decision.permit();
    }

    private static boolean blank(String value) {
        return value == null || value.isBlank();
    }
}
