package com.apunto.engine.service.copy.dispatch;

import com.apunto.engine.dto.OperationDto;
import com.apunto.engine.entity.UserCopyAllocationEntity;
import com.apunto.engine.service.copy.certification.LiveEntryAuthorizationDecision;
import com.apunto.engine.service.copy.certification.LiveEntryAuthorizationGate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

@Component
@Slf4j
public class CopyRealExecutionGate {

    @Value("${copy.new-dispatch.enabled:false}")
    private boolean newDispatchEnabled;
    @Value("${copy.derisk-dispatch.enabled:true}")
    private boolean deriskExecutionEnabled;
    @Value("${copy.micro-live.enabled:false}")
    private boolean microLiveEnabled;
    @Value("${copy.live.enabled:false}")
    private boolean liveEnabled;
    @Value("${copy.live.canary-enabled:false}")
    private boolean liveCanaryEnabled;
    @Value("${copy.live.dry-run:true}")
    private boolean liveDryRun;
    @Value("${copy.live.whitelist.user-ids:}")
    private String liveWhitelistUserIds;
    @Value("${copy.live.whitelist.wallet-ids:}")
    private String liveWhitelistWalletIds;
    @Value("${copy.live.whitelist.symbols:}")
    private String liveWhitelistSymbols;
    @Value("${copy.live.whitelist.allocation-ids:}")
    private String liveWhitelistAllocationIds;
    @Value("${copy.live.whitelist.strategy-codes:}")
    private String liveWhitelistStrategyCodes;

    @Autowired(required = false)
    private LiveEntryAuthorizationGate liveEntryAuthorizationGate;

    @PostConstruct
    void logEffectiveConfiguration() {
        log.info("event=copy.real_execution_gate.config newDispatchEnabled={} deriskExecutionEnabled={} microLiveEnabled={} liveEnabled={} liveDryRun={} liveCanaryEnabled={} whitelistUsers={} whitelistWallets={} whitelistSymbols={} whitelistAllocations={} whitelistStrategies={} reasonCode=EFFECTIVE_COPY_SWITCHES",
                newDispatchEnabled, deriskExecutionEnabled, microLiveEnabled, liveEnabled, liveDryRun, liveCanaryEnabled,
                parseWhitelist(liveWhitelistUserIds).size(), parseWhitelist(liveWhitelistWalletIds).size(),
                parseWhitelist(liveWhitelistSymbols).size(), parseWhitelist(liveWhitelistAllocationIds).size(),
                parseWhitelist(liveWhitelistStrategyCodes).size());
    }

    public Decision evaluate(OperationDto operation, UserCopyAllocationEntity allocation) {
        String mode = executionMode(allocation);
        boolean reduceOnly = operation != null && operation.isReduceOnly();
        if (!"MICRO_LIVE".equals(mode) && !"LIVE".equals(mode)) {
            return blocked(mode, "COPY_REAL_EXECUTION_MODE_UNKNOWN");
        }
        if (reduceOnly) {
            return deriskExecutionEnabled
                    ? allowed(mode, mode + "_DERISK_ALLOWED")
                    : blocked(mode, "COPY_DERISK_DISPATCH_DISABLED");
        }
        if (!newDispatchEnabled) {
            return blocked(mode, "COPY_NEW_DISPATCH_DISABLED");
        }
        if ("MICRO_LIVE".equals(mode)) {
            return microLiveEnabled
                    ? allowed(mode, reduceOnly ? "MICRO_LIVE_REDUCTION_ALLOWED" : "MICRO_LIVE_ENABLED")
                    : blocked(mode, "MICRO_LIVE_DISABLED");
        }
        if (!liveEnabled) {
            return blocked(mode, "LIVE_DISABLED");
        }
        if (liveDryRun) {
            return blocked(mode, "LIVE_DRY_RUN");
        }
        if (!liveCanaryEnabled) {
            return blocked(mode, "LIVE_CANARY_DISABLED");
        }
        if (!liveWhitelisted(operation, allocation)) {
            return blocked(mode, "LIVE_WHITELIST_BLOCKED");
        }
        if (liveEntryAuthorizationGate == null) {
            return blocked(mode, "LIVE_CERTIFICATION_UNAVAILABLE");
        }
        LiveEntryAuthorizationDecision certification = liveEntryAuthorizationGate.evaluate(operation, allocation);
        if (certification == null || !certification.allowed()) {
            return blocked(mode, certification == null
                    ? "LIVE_CERTIFICATION_UNAVAILABLE"
                    : certification.reasonCode());
        }
        return allowed(mode, "LIVE_CANARY_WHITELIST_CERTIFIED");
    }

    private boolean liveWhitelisted(OperationDto operation, UserCopyAllocationEntity allocation) {
        boolean anyConfigured = false;
        Set<String> users = parseWhitelist(liveWhitelistUserIds);
        if (!users.isEmpty()) {
            anyConfigured = true;
            if (!users.contains(normalize(operation == null ? null : operation.getUserId()))) return false;
        }
        Set<String> wallets = parseWhitelist(liveWhitelistWalletIds);
        if (!wallets.isEmpty()) {
            anyConfigured = true;
            if (!wallets.contains(normalize(operation == null ? null : operation.getWalletId()))) return false;
        }
        Set<String> symbols = parseWhitelist(liveWhitelistSymbols);
        if (!symbols.isEmpty()) {
            anyConfigured = true;
            if (!symbols.contains(normalize(operation == null ? null : operation.getSymbol()))) return false;
        }
        Set<String> allocations = parseWhitelist(liveWhitelistAllocationIds);
        if (!allocations.isEmpty()) {
            anyConfigured = true;
            if (!allocations.contains(normalize(allocation == null || allocation.getId() == null
                    ? null : allocation.getId().toString()))) return false;
        }
        Set<String> strategies = parseWhitelist(liveWhitelistStrategyCodes);
        if (!strategies.isEmpty()) {
            anyConfigured = true;
            if (!strategies.contains(normalize(allocation == null ? null : allocation.getCopyStrategyCode()))) return false;
        }
        return anyConfigured;
    }

    private Set<String> parseWhitelist(String raw) {
        if (raw == null || raw.isBlank()) return Set.of();
        return Arrays.stream(raw.split(","))
                .map(this::normalize)
                .filter(value -> value != null && !value.isBlank())
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private String executionMode(UserCopyAllocationEntity allocation) {
        return normalize(allocation == null ? null : allocation.getExecutionMode());
    }

    private String normalize(String value) {
        if (value == null || value.isBlank()) return null;
        return value.trim().toUpperCase(Locale.ROOT).replace('-', '_');
    }

    private Decision allowed(String mode, String reasonCode) {
        return new Decision(true, reasonCode, mode, true);
    }

    private Decision blocked(String mode, String reasonCode) {
        return new Decision(false, reasonCode, mode, false);
    }

    public record Decision(boolean allowed, String reasonCode, String executionMode, boolean realBinanceAllowed) {
    }
}
