package com.apunto.engine.dto;

import java.util.ArrayList;
import java.util.List;

public record CopyExecutionAccountsDiagnostics(
        boolean hasActiveUsers,
        boolean hasActiveBinanceKeys,
        boolean hasNewDispatchEnabled,
        boolean hasMicroLiveEnabled,
        boolean hasLiveEnabled,
        boolean liveDryRun,
        boolean liveCanaryEnabled,
        long activeUsers,
        long activeDetailUsers,
        long activeBinanceFlagUsers,
        long usableApiKeyUsers,
        long activeCapitalUsers,
        long activeMaxWalletUsers,
        long activeMicroLiveAllocations,
        long activeLiveAllocations,
        long eligibleMicroLiveUsers,
        long eligibleLiveUsers,
        long eligibleExecutionUsers,
        List<String> reasonsIfZero
) {

    public CopyExecutionAccountsDiagnostics {
        reasonsIfZero = reasonsIfZero == null ? List.of() : List.copyOf(reasonsIfZero);
    }

    public boolean hasExecutableAccounts() {
        return eligibleExecutionUsers > 0;
    }

    public long activeExecutableAllocations() {
        return activeMicroLiveAllocations + activeLiveAllocations;
    }

    public static CopyExecutionAccountsDiagnostics fromCounts(
            boolean newDispatchEnabled,
            boolean microLiveEnabled,
            boolean liveEnabled,
            boolean liveDryRun,
            boolean liveCanaryEnabled,
            boolean liveCanaryHasEligibleUsers,
            long activeUsers,
            long activeDetailUsers,
            long activeBinanceFlagUsers,
            long usableApiKeyUsers,
            long activeCapitalUsers,
            long activeMaxWalletUsers,
            long activeMicroLiveAllocations,
            long activeLiveAllocations,
            long eligibleMicroLiveUsers,
            long eligibleLiveUsers
    ) {
        long configuredEligibleUsers = Math.max(0L, eligibleMicroLiveUsers) + Math.max(0L, eligibleLiveUsers);
        long eligibleExecutionUsers = newDispatchEnabled ? configuredEligibleUsers : 0L;
        List<String> reasons = new ArrayList<>();
        if (activeUsers <= 0) reasons.add("NO_ACTIVE_USERS");
        if (activeDetailUsers <= 0) reasons.add("NO_ACTIVE_USER_DETAIL");
        if (activeBinanceFlagUsers <= 0 || usableApiKeyUsers <= 0) reasons.add("NO_ACTIVE_BINANCE_API_KEY");
        if (activeCapitalUsers <= 0) reasons.add("NO_CAPITAL_CONFIG");
        if (activeMaxWalletUsers <= 0) reasons.add("NO_MAX_WALLET_CONFIG");
        if (!newDispatchEnabled) reasons.add("COPY_NEW_DISPATCH_DISABLED");
        if (!microLiveEnabled) reasons.add("MICRO_LIVE_DISABLED");
        if (!liveEnabled) reasons.add("LIVE_DISABLED");
        if (liveDryRun) reasons.add("LIVE_DRY_RUN");
        if (liveEnabled && liveCanaryEnabled && !liveCanaryHasEligibleUsers) reasons.add("USER_NOT_IN_CANARY");
        if (activeMicroLiveAllocations + activeLiveAllocations <= 0) reasons.add("NO_ACTIVE_EXECUTION_ALLOCATION");
        if (eligibleExecutionUsers <= 0 && reasons.isEmpty()) reasons.add("NO_EXECUTABLE_COPY_ACCOUNTS");

        return new CopyExecutionAccountsDiagnostics(
                activeUsers > 0,
                activeBinanceFlagUsers > 0 && usableApiKeyUsers > 0,
                newDispatchEnabled,
                microLiveEnabled,
                liveEnabled,
                liveDryRun,
                liveCanaryEnabled,
                Math.max(0L, activeUsers),
                Math.max(0L, activeDetailUsers),
                Math.max(0L, activeBinanceFlagUsers),
                Math.max(0L, usableApiKeyUsers),
                Math.max(0L, activeCapitalUsers),
                Math.max(0L, activeMaxWalletUsers),
                Math.max(0L, activeMicroLiveAllocations),
                Math.max(0L, activeLiveAllocations),
                Math.max(0L, eligibleMicroLiveUsers),
                Math.max(0L, eligibleLiveUsers),
                eligibleExecutionUsers,
                eligibleExecutionUsers > 0 ? List.of() : reasons
        );
    }
}
