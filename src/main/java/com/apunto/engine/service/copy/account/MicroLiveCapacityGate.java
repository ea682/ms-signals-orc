package com.apunto.engine.service.copy.account;

import com.apunto.engine.client.BinanceClient;
import com.apunto.engine.dto.client.FuturesAssetBalanceClientResponse;
import com.apunto.engine.entity.UserApiKeyEntity;
import com.apunto.engine.repository.UserCopyAllocationRepository;
import com.apunto.engine.shared.dto.ApiResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.OffsetDateTime;
import java.util.UUID;

@Service
@Slf4j
public class MicroLiveCapacityGate {

    private final BinanceClient binanceClient;
    private final UserCopyAllocationRepository allocationRepository;
    private final MicroLiveCapacityProperties properties;
    private final MicroLiveCapacitySnapshotStore snapshotStore;

    @Autowired
    public MicroLiveCapacityGate(@Qualifier("binanceInfoClient") BinanceClient binanceClient,
                                 UserCopyAllocationRepository allocationRepository,
                                 MicroLiveCapacityProperties properties,
                                 MicroLiveCapacitySnapshotStore snapshotStore) {
        this.binanceClient = binanceClient;
        this.allocationRepository = allocationRepository;
        this.properties = properties;
        this.snapshotStore = snapshotStore;
    }

    MicroLiveCapacityGate(BinanceClient binanceClient,
                          UserCopyAllocationRepository allocationRepository,
                          MicroLiveCapacityProperties properties) {
        this(binanceClient, allocationRepository, properties, ignored -> { });
    }

    @Transactional
    public MicroLiveCapacityDecision evaluate(UserApiKeyEntity account, UUID userId, String asset) {
        return evaluate(account, userId, asset, MicroLiveAdmissionPriority.SHADOW_PROMOTION);
    }

    @Transactional
    public MicroLiveCapacityDecision evaluate(UserApiKeyEntity account, UUID userId, String asset,
                                               MicroLiveAdmissionPriority priority) {
        BigDecimal reserved = positive(properties.getBudgetPerAllocationUsdc(), new BigDecimal("100"));
        BigDecimal safetyBuffer = nonNegative(properties.getSafetyBufferUsdc());
        int configuredMax = Math.max(0, properties.getMaxConcurrentAllocations());
        if (!usable(account)) {
            return rejected("EXECUTION_ACCOUNT_CREDENTIALS_INVALID", 0, 0, null, null, reserved);
        }

        long occupied = allocationRepository.countOccupiedMicroLiveSlots(account.getId());
        ApiResponse<FuturesAssetBalanceClientResponse> response;
        try {
            response = binanceClient.assetBalance(
                    account.getApiKey(), account.getApiSecret(), null,
                    userId == null ? "unknown" : userId.toString(), null,
                    "micro-capacity-" + UUID.randomUUID(), code(asset, "USDC"));
        } catch (RuntimeException error) {
            log.warn("event=copy.micro_live.capacity.blocked userId={} exchangeAccountId={} accountPurpose=MICRO_LIVE decision=BLOCK reasonCode=MICRO_LIVE_ACCOUNT_BALANCE_UNAVAILABLE errorClass={}",
                    userId, account.getId(), error.getClass().getSimpleName());
            return rejected("MICRO_LIVE_ACCOUNT_BALANCE_UNAVAILABLE", occupied, 0,
                    null, null, reserved.multiply(BigDecimal.valueOf(occupied + 1)));
        }

        if (response == null || response.getStatusCode() == 401 || response.getStatusCode() == 403) {
            return rejected("EXECUTION_ACCOUNT_CREDENTIALS_INVALID", occupied, 0,
                    null, null, reserved.multiply(BigDecimal.valueOf(occupied + 1)));
        }
        if (!successful(response) || response.getData() == null) {
            return rejected("MICRO_LIVE_ACCOUNT_BALANCE_UNAVAILABLE", occupied, 0,
                    null, null, reserved.multiply(BigDecimal.valueOf(occupied + 1)));
        }

        FuturesAssetBalanceClientResponse balance = response.getData();
        BigDecimal wallet = decimal(balance.getWalletBalance());
        if (wallet == null) wallet = decimal(balance.getTotalWalletBalance());
        if (wallet == null) wallet = decimal(balance.getMarginBalance());
        BigDecimal available = decimal(balance.getAvailableBalance());
        if (wallet == null || available == null || Boolean.FALSE.equals(balance.getMarginAvailable())) {
            return rejected("MICRO_LIVE_ACCOUNT_BALANCE_UNAVAILABLE", occupied, 0,
                    wallet, available, reserved.multiply(BigDecimal.valueOf(occupied + 1)));
        }

        BigDecimal eligibleCapital = wallet.subtract(safetyBuffer).max(BigDecimal.ZERO);
        int balanceSlots = capacity(eligibleCapital, reserved);
        int effectiveMax = configuredMax > 0 ? Math.min(configuredMax, balanceSlots) : balanceSlots;
        int recertificationReserve = Math.min(Math.max(0, properties.getReservedRecertificationSlots()), effectiveMax);
        int admissionMax = priority != null && priority.recertification()
                ? effectiveMax : Math.max(0, effectiveMax - recertificationReserve);
        BigDecimal requiredTotal = reserved.multiply(BigDecimal.valueOf(occupied + 1));
        OffsetDateTime observedAt = OffsetDateTime.now();
        snapshotStore.save(new MicroLiveCapacitySnapshot(
                account.getId(), code(asset, "USDC"), wallet, available, safetyBuffer, eligibleCapital,
                reserved, balanceSlots, effectiveMax, configuredMax, recertificationReserve,
                observedAt, observedAt.plusSeconds(Math.max(1, properties.getSnapshotTtlSeconds()))));

        if (eligibleCapital.compareTo(reserved.multiply(BigDecimal.valueOf(occupied))) < 0) {
            return rejected("MICRO_LIVE_ACCOUNT_UNDER_RESERVED", occupied, effectiveMax,
                    wallet, available, requiredTotal);
        }
        if (occupied >= admissionMax) {
            String reasonCode = configuredMax > 0 && effectiveMax < configuredMax
                    ? "MICRO_LIVE_ACCOUNT_UNDER_RESERVED"
                    : "MICRO_LIVE_CAPACITY_EXHAUSTED";
            return rejected(reasonCode, occupied, effectiveMax,
                    wallet, available, requiredTotal);
        }
        return new MicroLiveCapacityDecision(true, "MICRO_LIVE_CAPACITY_AVAILABLE", occupied,
                effectiveMax, wallet, available, requiredTotal);
    }

    private static MicroLiveCapacityDecision rejected(String reasonCode, long occupied, int max,
                                                       BigDecimal wallet, BigDecimal available,
                                                       BigDecimal required) {
        return new MicroLiveCapacityDecision(false, reasonCode, occupied, max, wallet, available, required);
    }

    private static boolean successful(ApiResponse<?> response) {
        int code = response.getStatusCode();
        return code == 0 || (code >= 200 && code < 300);
    }

    private static boolean usable(UserApiKeyEntity account) {
        return account != null
                && account.getId() != null
                && account.isActive()
                && account.getAccountPurpose() == ExecutionAccountPurpose.MICRO_LIVE
                && text(account.getApiKey())
                && text(account.getApiSecret());
    }

    private static BigDecimal decimal(String value) {
        try {
            return value == null || value.isBlank() ? null : new BigDecimal(value.trim());
        } catch (NumberFormatException error) {
            return null;
        }
    }

    private static BigDecimal positive(BigDecimal value, BigDecimal fallback) {
        return value == null || value.signum() <= 0 ? fallback : value;
    }

    private static BigDecimal nonNegative(BigDecimal value) {
        return value == null || value.signum() < 0 ? BigDecimal.ZERO : value;
    }

    private static int capacity(BigDecimal eligibleCapital, BigDecimal budget) {
        BigDecimal slots = eligibleCapital.divide(budget, 0, RoundingMode.FLOOR);
        return slots.compareTo(BigDecimal.valueOf(Integer.MAX_VALUE)) > 0
                ? Integer.MAX_VALUE : Math.max(0, slots.intValue());
    }

    private static String code(String value, String fallback) {
        return value == null || value.isBlank() ? fallback : value.trim().toUpperCase();
    }

    private static boolean text(String value) {
        return value != null && !value.isBlank();
    }
}
