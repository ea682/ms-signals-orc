package com.apunto.engine.service.copy.certification;

import com.apunto.engine.client.BinanceClient;
import com.apunto.engine.dto.client.BinanceFuturesPositionClientDto;
import com.apunto.engine.dto.client.FuturesAssetBalanceClientResponse;
import com.apunto.engine.entity.CopyOperationEntity;
import com.apunto.engine.entity.DetailUserEntity;
import com.apunto.engine.entity.UserApiKeyEntity;
import com.apunto.engine.entity.UserCopyAllocationEntity;
import com.apunto.engine.repository.CopyOperationRepository;
import com.apunto.engine.repository.DetailUserRepository;
import com.apunto.engine.repository.UserApiKeyRepository;
import com.apunto.engine.repository.UserWalletCopyPreferenceRepository;
import com.apunto.engine.shared.dto.ApiResponse;
import com.apunto.engine.service.copy.account.ExecutionAccountPurpose;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

@Service
@Slf4j
public class AutomaticLiveAdoptionService {

    private final BinanceClient binanceClient;
    private final DetailUserRepository detailRepository;
    private final UserApiKeyRepository apiKeyRepository;
    private final UserWalletCopyPreferenceRepository walletPreferenceRepository;
    private final CopyOperationRepository copyOperationRepository;
    private final LiveUserAdoptionApplicationService adoptionService;
    private final ManualLiveAllocationActivationService activationService;
    private final String requiredMarginMode;
    private final Duration adoptionValidity;

    public AutomaticLiveAdoptionService(
            @Qualifier("binanceInfoClient") BinanceClient binanceClient,
            DetailUserRepository detailRepository,
            UserApiKeyRepository apiKeyRepository,
            UserWalletCopyPreferenceRepository walletPreferenceRepository,
            CopyOperationRepository copyOperationRepository,
            LiveUserAdoptionApplicationService adoptionService,
            ManualLiveAllocationActivationService activationService,
            @Value("${binance.trading-config-preconfigure.margin-type:CROSSED}") String requiredMarginMode,
            @Value("${copy.live-adoption.validity:24h}") Duration adoptionValidity
    ) {
        this.binanceClient = binanceClient;
        this.detailRepository = detailRepository;
        this.apiKeyRepository = apiKeyRepository;
        this.walletPreferenceRepository = walletPreferenceRepository;
        this.copyOperationRepository = copyOperationRepository;
        this.adoptionService = adoptionService;
        this.activationService = activationService;
        this.requiredMarginMode = marginMode(requiredMarginMode);
        this.adoptionValidity = adoptionValidity == null || adoptionValidity.isNegative() || adoptionValidity.isZero()
                ? Duration.ofHours(24) : adoptionValidity;
    }

    public LiveAllocationActivationResult reconcile(UserCopyAllocationEntity allocation) {
        if (allocation == null || allocation.getId() == null || allocation.getIdUser() == null
                || allocation.getLiveCertificationId() == null || !"LIVE".equals(allocation.getExecutionMode())) {
            return LiveAllocationActivationResult.blocked("LIVE_ADOPTION_RECONCILIATION_CONTRACT_INVALID",
                    allocation == null ? null : allocation.getId());
        }
        DetailUserEntity detail = detailRepository.findByUser_Id(allocation.getIdUser());
        UserApiKeyEntity key = apiKeyRepository
                .findByUser_IdAndExchangeIgnoreCaseAndAccountPurposeAndActiveTrue(
                        allocation.getIdUser(), "BINANCE", ExecutionAccountPurpose.LIVE)
                .orElse(null);
        OffsetDateTime observedAt = OffsetDateTime.now();
        boolean preferenceValid = detail != null && detail.isUserActive() && detail.isApiKeyBinar()
                && (detail.isAutoFollowCertifiedLive() || detail.isContinueInLiveAfterCertification())
                && detail.getCapital() != null && detail.getCapital() > 0
                && detail.getMaxWallet() != null && detail.getMaxWallet() > 0
                && !walletPreferenceRepository.isBlocked(allocation.getIdUser(), allocation.getWalletId());
        boolean credentialsPresent = key != null && text(key.getApiKey()) && text(key.getApiSecret());
        BigDecimal assignedCapital = assignedCapital(detail, allocation);
        boolean distributionValid = validDistribution(allocation, observedAt);

        AccountObservation account = credentialsPresent
                ? observeAccount(allocation, key)
                : AccountObservation.unavailable();
        boolean riskPolicyValid = preferenceValid && distributionValid && assignedCapital != null;
        LiveUserAdoptionResult adoption = adoptionService.validateAndPersist(new LiveUserAdoptionCommand(
                allocation.getLiveCertificationId(), allocation.getIdUser(), allocation.getId(),
                account.balanceUsd(), assignedCapital, allocation.getLeverageOverride(),
                firstCode(allocation.getResolvedQuoteAsset(), allocation.getCapitalAsset(), "USDC"),
                account.observedMarginMode(), requiredMarginMode,
                account.apiPermissionsValid(), account.manualPositionsValid(), riskPolicyValid,
                observedAt, observedAt.plus(adoptionValidity)));
        if (!adoption.persisted() || adoption.decision() == null || !adoption.decision().valid()) {
            String reason = adoption.decision() == null
                    ? adoption.reasonCode()
                    : String.join(",", adoption.decision().reasonCodes());
            log.info("event=copy.live_adoption.reconciliation_blocked allocationId={} certificationId={} userId={} reasonCode={}",
                    allocation.getId(), allocation.getLiveCertificationId(), allocation.getIdUser(), reason);
            return LiveAllocationActivationResult.blocked(
                    reason == null || reason.isBlank() ? "LIVE_ADOPTION_REJECTED" : reason, allocation.getId());
        }

        LiveAllocationActivationResult activation = activationService.activate(new LiveAllocationActivationCommand(
                allocation.getId(), allocation.getLiveCertificationId(), "automatic-live-adoption",
                "validated account, preferences, distribution and risk",
                "automatic-adoption:" + allocation.getLiveCertificationId() + ":" + allocation.getId()));
        log.info("event=copy.live_adoption.reconciled allocationId={} certificationId={} userId={} activated={} idempotent={} reasonCode={}",
                allocation.getId(), allocation.getLiveCertificationId(), allocation.getIdUser(),
                activation.activated(), activation.idempotent(), activation.reasonCode());
        return activation;
    }

    private AccountObservation observeAccount(UserCopyAllocationEntity allocation, UserApiKeyEntity key) {
        try {
            String traceId = "live-adoption-" + allocation.getId();
            ApiResponse<FuturesAssetBalanceClientResponse> balanceResponse = binanceClient.assetBalance(
                    key.getApiKey(), key.getApiSecret(), null, allocation.getIdUser().toString(),
                    allocation.getWalletId(), traceId,
                    firstCode(allocation.getResolvedQuoteAsset(), allocation.getCapitalAsset(), "USDC"));
            ApiResponse<List<BinanceFuturesPositionClientDto>> positionsResponse = binanceClient.positions(
                    key.getApiKey(), key.getApiSecret(), traceId);
            FuturesAssetBalanceClientResponse balance = successful(balanceResponse) ? balanceResponse.getData() : null;
            List<BinanceFuturesPositionClientDto> positions = successful(positionsResponse)
                    && positionsResponse.getData() != null ? positionsResponse.getData() : List.of();
            boolean apiValid = balance != null && successful(positionsResponse);
            if (!apiValid) return AccountObservation.unavailable();

            Set<String> trackedSymbols = new HashSet<>();
            for (CopyOperationEntity copy : copyOperationRepository
                    .findAllByIdUserAndActiveTrue(allocation.getIdUser().toString())) {
                if (copy != null && text(copy.getParsymbol())) {
                    trackedSymbols.add(copy.getParsymbol().trim().toUpperCase(Locale.ROOT));
                }
            }
            boolean manualPositionsValid = true;
            boolean marginModeValid = true;
            for (BinanceFuturesPositionClientDto position : positions) {
                if (position == null || !nonZero(position.getPositionAmt())) continue;
                String symbol = code(position.getSymbol(), "");
                if (!trackedSymbols.contains(symbol)) manualPositionsValid = false;
                if (!requiredMarginMode.equals(marginMode(position.getMarginType()))) marginModeValid = false;
            }
            return new AccountObservation(balance(balance), apiValid, manualPositionsValid,
                    marginModeValid ? requiredMarginMode : "MISMATCH");
        } catch (RuntimeException ex) {
            log.warn("event=copy.live_adoption.account_observation_failed allocationId={} userId={} errClass={} reasonCode=LIVE_ADOPTION_API_OBSERVATION_FAILED",
                    allocation.getId(), allocation.getIdUser(), ex.getClass().getSimpleName());
            return AccountObservation.unavailable();
        }
    }

    private static boolean successful(ApiResponse<?> response) {
        if (response == null || response.getData() == null) return false;
        int code = response.getStatusCode();
        return code == 0 || (code >= 200 && code < 300);
    }

    private static BigDecimal assignedCapital(DetailUserEntity detail, UserCopyAllocationEntity allocation) {
        if (detail == null || detail.getCapital() == null || detail.getCapital() <= 0
                || allocation.getAllocationPct() == null || allocation.getAllocationPct().signum() <= 0) return null;
        return BigDecimal.valueOf(detail.getCapital()).multiply(allocation.getAllocationPct());
    }

    private static boolean validDistribution(UserCopyAllocationEntity allocation, OffsetDateTime now) {
        return allocation.getAllocationPct() != null && allocation.getAllocationPct().signum() > 0
                && allocation.getAllocationPct().compareTo(BigDecimal.ONE) <= 0
                && text(allocation.getAllocationPctSource())
                && allocation.getAllocationPctSourceId() != null
                && allocation.getAllocationPctCalculatedAt() != null
                && allocation.getAllocationPctValidUntil() != null
                && allocation.getAllocationPctValidUntil().isAfter(now);
    }

    private static BigDecimal balance(FuturesAssetBalanceClientResponse value) {
        for (String candidate : List.of(
                nullToEmpty(value.getMarginBalance()), nullToEmpty(value.getTotalWalletBalance()),
                nullToEmpty(value.getWalletBalance()), nullToEmpty(value.getAvailableBalance()))) {
            try {
                if (!candidate.isBlank()) return new BigDecimal(candidate);
            } catch (NumberFormatException ignored) {
                // Try the next authoritative balance field.
            }
        }
        return null;
    }

    private static boolean nonZero(String value) {
        try {
            return value != null && new BigDecimal(value).signum() != 0;
        } catch (NumberFormatException ex) {
            return true;
        }
    }

    private static String firstCode(String... values) {
        for (String value : values) if (text(value)) return code(value, "USDC");
        return "USDC";
    }

    private static String code(String value, String fallback) {
        return text(value) ? value.trim().toUpperCase(Locale.ROOT).replace('-', '_') : fallback;
    }

    private static String marginMode(String value) {
        String normalized = code(value, "UNOBSERVED");
        return "CROSS".equals(normalized) ? "CROSSED" : normalized;
    }

    private static boolean text(String value) {
        return value != null && !value.isBlank();
    }

    private static String nullToEmpty(String value) {
        return value == null ? "" : value;
    }

    private record AccountObservation(BigDecimal balanceUsd, boolean apiPermissionsValid,
                                      boolean manualPositionsValid, String observedMarginMode) {
        static AccountObservation unavailable() {
            return new AccountObservation(null, false, false, "UNOBSERVED");
        }
    }
}
