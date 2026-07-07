package com.apunto.engine.service.impl;

import com.apunto.engine.dto.CopyExecutionAccountsDiagnostics;
import com.apunto.engine.repository.DetailUserRepository;
import com.apunto.engine.repository.UserApiKeyRepository;
import com.apunto.engine.repository.UserCopyAllocationRepository;
import com.apunto.engine.repository.UserRepository;
import com.apunto.engine.service.CopyExecutionAccountsDiagnosticsService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;

@Service
@Slf4j
public class CopyExecutionAccountsDiagnosticsServiceImpl implements CopyExecutionAccountsDiagnosticsService {

    private final UserRepository userRepository;
    private final DetailUserRepository detailUserRepository;
    private final UserApiKeyRepository userApiKeyRepository;
    private final UserCopyAllocationRepository allocationRepository;
    private final boolean microLiveEnabled;
    private final boolean liveEnabled;
    private final boolean liveDryRun;
    private final boolean liveCanaryEnabled;
    private final List<UUID> liveWhitelistUserIds;

    public CopyExecutionAccountsDiagnosticsServiceImpl(
            UserRepository userRepository,
            DetailUserRepository detailUserRepository,
            UserApiKeyRepository userApiKeyRepository,
            UserCopyAllocationRepository allocationRepository,
            @Value("${copy.micro-live.enabled:true}") boolean microLiveEnabled,
            @Value("${copy.live.enabled:false}") boolean liveEnabled,
            @Value("${copy.live.dry-run:true}") boolean liveDryRun,
            @Value("${copy.live.canary-enabled:false}") boolean liveCanaryEnabled,
            @Value("${copy.live.whitelist.user-ids:}") String liveWhitelistUserIds
    ) {
        this.userRepository = userRepository;
        this.detailUserRepository = detailUserRepository;
        this.userApiKeyRepository = userApiKeyRepository;
        this.allocationRepository = allocationRepository;
        this.microLiveEnabled = microLiveEnabled;
        this.liveEnabled = liveEnabled;
        this.liveDryRun = liveDryRun;
        this.liveCanaryEnabled = liveCanaryEnabled;
        this.liveWhitelistUserIds = parseUuidList(liveWhitelistUserIds);
    }

    @Override
    public CopyExecutionAccountsDiagnostics snapshot() {
        long activeUsers = userRepository.countActiveUsers();
        long activeDetailUsers = detailUserRepository.countActiveDetailUsers();
        long activeBinanceFlagUsers = detailUserRepository.countActiveBinanceEnabledUsers();
        long usableApiKeyUsers = userApiKeyRepository.countUsersWithUsableApiKeys();
        long activeCapitalUsers = detailUserRepository.countActiveBinanceUsersWithCapital();
        long activeMaxWalletUsers = detailUserRepository.countActiveBinanceUsersWithCapitalAndMaxWallet();

        long activeMicroLiveAllocations = allocationRepository.countActiveExecutableAllocationsByMode("MICRO_LIVE", "active");
        long activeLiveAllocations = allocationRepository.countActiveExecutableAllocationsByMode("LIVE", "active");

        long eligibleMicroLiveUsers = microLiveEnabled
                ? allocationRepository.countEligibleExecutionUsersByMode("MICRO_LIVE")
                : 0L;

        boolean liveExecutable = liveEnabled && !liveDryRun;
        long eligibleLiveUsers = 0L;
        boolean canaryHasEligibleUsers = true;
        if (liveExecutable) {
            if (liveCanaryEnabled) {
                canaryHasEligibleUsers = !liveWhitelistUserIds.isEmpty();
                eligibleLiveUsers = canaryHasEligibleUsers
                        ? allocationRepository.countEligibleLiveExecutionUsersIn(liveWhitelistUserIds)
                        : 0L;
            } else {
                eligibleLiveUsers = allocationRepository.countEligibleExecutionUsersByMode("LIVE");
            }
        }

        CopyExecutionAccountsDiagnostics diagnostics = CopyExecutionAccountsDiagnostics.fromCounts(
                microLiveEnabled,
                liveEnabled,
                liveDryRun,
                liveCanaryEnabled,
                canaryHasEligibleUsers,
                activeUsers,
                activeDetailUsers,
                activeBinanceFlagUsers,
                usableApiKeyUsers,
                activeCapitalUsers,
                activeMaxWalletUsers,
                activeMicroLiveAllocations,
                activeLiveAllocations,
                eligibleMicroLiveUsers,
                eligibleLiveUsers
        );

        log.info(
                "event=copy.execution_accounts.diagnostics hasActiveUsers={} hasActiveBinanceKeys={} hasMicroLiveEnabled={} hasLiveEnabled={} liveDryRun={} liveCanaryEnabled={} activeUsers={} activeDetailUsers={} activeBinanceFlagUsers={} usableApiKeyUsers={} activeCapitalUsers={} activeMaxWalletUsers={} activeMicroLiveAllocations={} activeLiveAllocations={} eligibleMicroLiveUsers={} eligibleLiveUsers={} eligibleExecutionUsers={} reasonsIfZero={}",
                diagnostics.hasActiveUsers(),
                diagnostics.hasActiveBinanceKeys(),
                diagnostics.hasMicroLiveEnabled(),
                diagnostics.hasLiveEnabled(),
                diagnostics.liveDryRun(),
                diagnostics.liveCanaryEnabled(),
                diagnostics.activeUsers(),
                diagnostics.activeDetailUsers(),
                diagnostics.activeBinanceFlagUsers(),
                diagnostics.usableApiKeyUsers(),
                diagnostics.activeCapitalUsers(),
                diagnostics.activeMaxWalletUsers(),
                diagnostics.activeMicroLiveAllocations(),
                diagnostics.activeLiveAllocations(),
                diagnostics.eligibleMicroLiveUsers(),
                diagnostics.eligibleLiveUsers(),
                diagnostics.eligibleExecutionUsers(),
                diagnostics.reasonsIfZero()
        );

        return diagnostics;
    }

    private static List<UUID> parseUuidList(String raw) {
        if (raw == null || raw.isBlank()) {
            return List.of();
        }
        return java.util.Arrays.stream(raw.split(","))
                .map(String::trim)
                .filter(s -> !s.isBlank())
                .map(CopyExecutionAccountsDiagnosticsServiceImpl::parseUuidOrNull)
                .filter(java.util.Objects::nonNull)
                .distinct()
                .toList();
    }

    private static UUID parseUuidOrNull(String raw) {
        try {
            return UUID.fromString(raw);
        } catch (RuntimeException ex) {
            log.warn("event=copy.execution_accounts.diagnostics.invalid_whitelist_user_id value={}", raw);
            return null;
        }
    }
}
