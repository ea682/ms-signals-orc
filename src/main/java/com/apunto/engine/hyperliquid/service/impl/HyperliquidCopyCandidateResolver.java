package com.apunto.engine.hyperliquid.service.impl;

import com.apunto.engine.dto.OperacionDto;
import com.apunto.engine.dto.UserDetailDto;
import com.apunto.engine.events.OperacionEvent;
import com.apunto.engine.entity.UserCopyAllocationEntity;
import com.apunto.engine.hyperliquid.dto.HyperliquidMappedDelta;
import com.apunto.engine.hyperliquid.model.HyperliquidDeltaType;
import com.apunto.engine.jobs.model.CopyJobAction;
import com.apunto.engine.service.ActiveCopyOperationCache;
import com.apunto.engine.service.UserCopyAllocationService;
import com.apunto.engine.service.UserDetailCachedService;
import com.apunto.engine.service.copy.CopyStrategyRuntimeRouter;
import com.apunto.engine.service.copy.CopyStrategyGuardDecision;
import com.apunto.engine.service.copy.CopyStrategyGuardRuntimeCache;
import com.apunto.engine.service.copy.CopyRuntimeGuardPolicy;
import com.apunto.engine.shared.util.CopyLogAdvice;
import lombok.extern.slf4j.Slf4j;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@Service
public class HyperliquidCopyCandidateResolver {

    private final UserDetailCachedService userDetailCachedService;
    private final UserCopyAllocationService userCopyAllocationService;
    private final ActiveCopyOperationCache activeCopyOperationCache;
    private final CopyStrategyRuntimeRouter copyStrategyRuntimeRouter;
    private final CopyStrategyGuardRuntimeCache copyStrategyGuardRuntimeCache;
    private final CopyRuntimeGuardPolicy copyRuntimeGuardPolicy;
    private final MeterRegistry meterRegistry;

    @Value("${operation.job.ingest.filter-by-wallet-allocation:${copy.job.ingest.filter-by-wallet-allocation:true}}")
    private boolean filterByWalletAllocation = true;

    @Value("${operation.job.ingest.fallback-all-users-on-empty-allocation:${copy.job.ingest.fallback-all-users-on-empty-allocation:false}}")
    private boolean fallbackAllUsersOnEmptyAllocation;

    @Value("${metric-wallet.copy-guard.cooldown-hours:48}")
    private long copyGuardCooldownHours;

    public HyperliquidCopyCandidateResolver(
            UserDetailCachedService userDetailCachedService,
            UserCopyAllocationService userCopyAllocationService,
            ActiveCopyOperationCache activeCopyOperationCache,
            CopyStrategyRuntimeRouter copyStrategyRuntimeRouter,
            CopyStrategyGuardRuntimeCache copyStrategyGuardRuntimeCache,
            CopyRuntimeGuardPolicy copyRuntimeGuardPolicy,
            MeterRegistry meterRegistry
    ) {
        this.userDetailCachedService = userDetailCachedService;
        this.userCopyAllocationService = userCopyAllocationService;
        this.activeCopyOperationCache = activeCopyOperationCache;
        this.copyStrategyRuntimeRouter = copyStrategyRuntimeRouter;
        this.copyStrategyGuardRuntimeCache = copyStrategyGuardRuntimeCache;
        this.copyRuntimeGuardPolicy = copyRuntimeGuardPolicy;
        this.meterRegistry = meterRegistry;
    }

    public CandidateUsers resolve(HyperliquidMappedDelta mappedDelta, CopyJobAction action) {
        OperacionEvent event = mappedDelta.event();
        OperacionDto operation = event.getOperacion();
        List<UserDetailDto> usersCached = safeUsers(userDetailCachedService.getUsersCachedOnly());
        HyperliquidDeltaType deltaType = HyperliquidDeltaType.from(mappedDelta.deltaType() == null ? event.getDeltaType() : mappedDelta.deltaType());

        if (action == CopyJobAction.CLOSE) {
            return activeCopyUsers(operation, usersCached, "active_copy_close");
        }

        if (action == CopyJobAction.OPEN && deltaType.canAdjustExistingCopy()) {
            if (deltaType == HyperliquidDeltaType.FLIP) {
                return flipUsers(operation, usersCached, action, deltaType);
            }
            return activeCopyUsers(operation, usersCached, "active_copy_adjustment");
        }

        return allocationUsers(operation, usersCached, action, deltaType);
    }

    private CandidateUsers activeCopyUsers(OperacionDto operation, List<UserDetailDto> usersCached, String source) {
        String originId = operation.getIdOperacion() == null ? null : operation.getIdOperacion().toString();
        Set<String> activeUserIds = activeCopyOperationCache.activeUserIds(originId);
        List<UserDetailDto> eligible = filterUsersById(usersCached, activeUserIds);
        String reasonCode = eligible.isEmpty() ? ("active_copy_close".equals(source) ? "close_without_open_copy" : "resize_without_open_copy") : "active_copy_users_found";
        if (eligible.isEmpty()) {
            meterRegistry.counter("copy.orphan.movement.total",
                    "type", "active_copy_close".equals(source) ? "close" : "resize",
                    "reason", metricTag(reasonCode)).increment();
        }
        String diagnostic = eligible.isEmpty()
                ? CopyLogAdvice.fields(reasonCode, CopyLogAdvice.context(activeUserIds.size(), eligible.size(), 0, 0, null, null, activeUserIds.size(), source))
                : "";
        log.info("event=hyperliquid.direct_copy.user_filter source={} originId={} wallet={} symbol={} activeCopyUsers={} usersCached={} eligibleUsers={} eligibleUserIds={} reasonCode={} {}",
                source, safeLog(originId), safeLog(operation.getIdCuenta()), safeLog(operation.getParSymbol()), activeUserIds.size(), usersCached.size(), eligible.size(), userIdsCsv(eligible), reasonCode, diagnostic);
        return new CandidateUsers(usersCached, eligible, source);
    }

    private CandidateUsers activeCopyUsersByWalletAndSymbol(OperacionDto operation, List<UserDetailDto> usersCached, String source) {
        Set<String> activeUserIds = activeCopyOperationCache.activeUserIdsByWalletAndBaseSymbol(operation.getIdCuenta(), operation.getParSymbol());
        List<UserDetailDto> eligible = filterUsersById(usersCached, activeUserIds);
        String reasonCode = eligible.isEmpty() ? "flip_without_open_copy_same_base_asset" : "active_copy_users_found_same_base_asset";
        String diagnostic = eligible.isEmpty()
                ? CopyLogAdvice.fields(reasonCode, CopyLogAdvice.context(activeUserIds.size(), eligible.size(), 0, 0, null, null, activeUserIds.size(), source))
                : "";
        log.info("event=hyperliquid.direct_copy.user_filter source={} originId={} wallet={} symbol={} matchMode=wallet_and_base_asset activeCopyUsers={} usersCached={} eligibleUsers={} eligibleUserIds={} reasonCode={} {}",
                source,
                operation.getIdOperacion(),
                safeLog(operation.getIdCuenta()),
                safeLog(operation.getParSymbol()),
                activeUserIds.size(),
                usersCached.size(),
                eligible.size(),
                userIdsCsv(eligible),
                reasonCode,
                diagnostic);
        return new CandidateUsers(usersCached, eligible, source);
    }


    private CandidateUsers flipUsers(OperacionDto operation, List<UserDetailDto> usersCached, CopyJobAction action, HyperliquidDeltaType deltaType) {
        CandidateUsers activeUsers = activeCopyUsersByWalletAndSymbol(operation, usersCached, "active_copy_flip");
        CandidateUsers allocationUsers = allocationUsers(operation, usersCached, action, deltaType);

        Set<UUID> mergedIds = new java.util.LinkedHashSet<>();
        if (activeUsers.eligibleUsers() != null) {
            activeUsers.eligibleUsers().stream()
                    .filter(Objects::nonNull)
                    .filter(u -> u.getUser() != null && u.getUser().getId() != null)
                    .map(u -> u.getUser().getId())
                    .forEach(mergedIds::add);
        }
        if (allocationUsers.eligibleUsers() != null) {
            allocationUsers.eligibleUsers().stream()
                    .filter(Objects::nonNull)
                    .filter(u -> u.getUser() != null && u.getUser().getId() != null)
                    .map(u -> u.getUser().getId())
                    .forEach(mergedIds::add);
        }

        List<UserDetailDto> eligible = usersCached.stream()
                .filter(Objects::nonNull)
                .filter(u -> u.getUser() != null && u.getUser().getId() != null)
                .filter(u -> mergedIds.contains(u.getUser().getId()))
                .toList();

        log.info("event=hyperliquid.direct_copy.user_filter source=flip_merged wallet={} symbol={} activeEligible={} allocationEligible={} mergedEligible={} eligibleUserIds={}",
                safeLog(operation == null ? null : operation.getIdCuenta()),
                safeLog(operation == null ? null : operation.getParSymbol()),
                activeUsers.eligibleUsers().size(),
                allocationUsers.eligibleUsers().size(),
                eligible.size(),
                userIdsCsv(eligible));
        return new CandidateUsers(usersCached, eligible, "flip_merged");
    }

    private CandidateUsers allocationUsers(OperacionDto operation, List<UserDetailDto> usersCached, CopyJobAction action, HyperliquidDeltaType deltaType) {
        long startedNs = System.nanoTime();
        String walletId = operation.getIdCuenta();
        String side = operation.getTipoOperacion() == null ? null : operation.getTipoOperacion().name();
        String symbol = operation.getParSymbol();
        log.info("event=copy.candidate.resolve.started wallet={} symbol={} side={} eventType={} deltaType={}",
                safeLog(walletId), safeLog(symbol), safeLog(side), action, deltaType);
        if (!filterByWalletAllocation) {
            return new CandidateUsers(usersCached, usersCached, "all_users_config");
        }
        if (walletId == null || walletId.isBlank()) {
            log.warn("event=hyperliquid.direct_copy.user_filter_skipped reasonCode=wallet_missing usersCached={} {}", usersCached.size(),
                    CopyLogAdvice.fields("wallet_missing", CopyLogAdvice.context(null, 0, 0, 0, null, null, null, "allocation_wallet_missing")));
            List<UserDetailDto> eligible = fallbackAllUsersOnEmptyAllocation ? usersCached : List.of();
            return new CandidateUsers(usersCached, eligible, "allocation_wallet_missing");
        }

        List<UserCopyAllocationEntity> activeAllocations = userCopyAllocationService.getActiveAllocationsByWalletCachedOnly(walletId);
        Map<String, CopyStrategyGuardDecision> guardByProfile = new HashMap<>();
        Map<String, Integer> excluded = new LinkedHashMap<>();
        Set<UUID> activeUserIds = new java.util.LinkedHashSet<>();
        int matchingAllocations = 0;
        for (UserCopyAllocationEntity allocation : activeAllocations) {
            if (allocation == null) {
                exclude(excluded, "ALLOCATION_MISSING", "unknown");
                continue;
            }
            log.info("event=copy.candidate.resolve.allocation_seen userCopyAllocationId={} executionMode={} status={} isActive={} endsAt={} strategyCode={}",
                    allocation.getId(),
                    safeLog(allocation.getExecutionMode()),
                    allocation.getStatus(),
                    allocation.isActive(),
                    allocation.getEndsAt(),
                    safeLog(allocation.getCopyStrategyCode()));
            if (!copyStrategyRuntimeRouter.allocationAppliesToEvent(allocation, action, deltaType, side, symbol)) {
                String reasonCode = scopeBlockReason(allocation);
                exclude(excluded, reasonCode, allocation.getExecutionMode());
                log.info("event=copy.candidate.resolve.allocation_filtered userCopyAllocationId={} executionMode={} reasonCode={} sourceReasonCode=STRATEGY_SCOPE_NOT_MATCHED",
                        allocation.getId(), safeLog(allocation.getExecutionMode()), reasonCode);
                continue;
            }
            CopyRuntimeGuardPolicy.Decision guardDecision = guardAllowsNewEntry(allocation, guardByProfile);
            if (!guardDecision.allowed()) {
                String reasonCode = guardBlockReason(allocation, guardDecision.reasonCode());
                exclude(excluded, reasonCode, allocation.getExecutionMode());
                log.info("event=copy.candidate.resolve.allocation_filtered userCopyAllocationId={} executionMode={} reasonCode={} guardReasonCode={}",
                        allocation.getId(), safeLog(allocation.getExecutionMode()), reasonCode, safeLog(guardDecision.reasonCode()));
                continue;
            }
            if (allocation.getIdUser() != null) {
                activeUserIds.add(allocation.getIdUser());
            }
            matchingAllocations++;
            meterRegistry.counter("copy.eligibility.total",
                    "mode", metricTag(allocation.getExecutionMode()),
                    "result", "eligible",
                    "reason", "allowed").increment();
            log.info("event=copy.candidate.resolve.allocation_matched userCopyAllocationId={} executionMode={} reasonCode={}",
                    allocation.getId(), safeLog(allocation.getExecutionMode()), safeLog(guardDecision.reasonCode()));
        }
        if (activeUserIds.isEmpty()) {
            String primaryReason = primaryExclusionReason(activeAllocations, excluded);
            EligibilitySummary summary = new EligibilitySummary(activeAllocations.size(), 0, Map.copyOf(excluded));
            log.info("event=hyperliquid.direct_copy.user_filter source=allocation wallet={} side={} deltaType={} action={} activeAllocations={} matchingAllocationUsers=0 usersCached={} eligibleUsers=0 fallbackAllUsers={} reasonCode={} eligibilitySummary={} {}",
                    safeLog(walletId), safeLog(side), deltaType, action, activeAllocations.size(), usersCached.size(), fallbackAllUsersOnEmptyAllocation,
                    primaryReason, summary,
                    CopyLogAdvice.fields(primaryReason, CopyLogAdvice.context(activeAllocations.size(), 0, 0, 0, null, null, 0, "allocation")));
            log.info("event=copy.candidate.resolve.finished usersCached={} activeAllocations={} eligibleUsers=0 eligibleUserIds= elapsedMs={}",
                    usersCached.size(), activeAllocations.size(), elapsedMs(startedNs));
            List<UserDetailDto> eligible = fallbackAllUsersOnEmptyAllocation ? usersCached : List.of();
            return new CandidateUsers(usersCached, eligible, "allocation_empty", primaryReason, summary);
        }

        List<UserDetailDto> eligible = usersCached.stream()
                .filter(Objects::nonNull)
                .filter(u -> u.getUser() != null && u.getUser().getId() != null)
                .filter(u -> activeUserIds.contains(u.getUser().getId()))
                .toList();

        int usersMissingFromSnapshot = Math.max(0, activeUserIds.size() - eligible.size());
        if (usersMissingFromSnapshot > 0) {
            excluded.merge("USER_NOT_IN_RUNTIME_CACHE", usersMissingFromSnapshot, Integer::sum);
            meterRegistry.counter("copy.eligibility.total",
                    "mode", "unknown", "result", "excluded", "reason", "user_not_in_runtime_cache")
                    .increment(usersMissingFromSnapshot);
        }
        EligibilitySummary summary = new EligibilitySummary(activeAllocations.size(), matchingAllocations, Map.copyOf(excluded));

        log.info("event=hyperliquid.direct_copy.user_filter source=allocation wallet={} side={} deltaType={} action={} activeAllocations={} matchingHealthyAllocationUsers={} usersCached={} eligibleUsers={} eligibleUserIds={}",
                safeLog(walletId), safeLog(side), deltaType, action, activeAllocations.size(), activeUserIds.size(), usersCached.size(), eligible.size(), userIdsCsv(eligible));
        log.info("event=copy.candidate.resolve.finished usersCached={} activeAllocations={} eligibleUsers={} eligibleUserIds={} eligibilitySummary={} elapsedMs={}",
                usersCached.size(), activeAllocations.size(), eligible.size(), userIdsCsv(eligible), summary, elapsedMs(startedNs));
        String reasonCode = eligible.isEmpty() ? firstReason(excluded, "USER_NOT_IN_RUNTIME_CACHE") : null;
        return new CandidateUsers(usersCached, eligible, "allocation", reasonCode, summary);
    }

    private void exclude(Map<String, Integer> excluded, String reasonCode, String executionMode) {
        String reason = reasonCode == null || reasonCode.isBlank() ? "UNKNOWN_EXCLUSION" : reasonCode;
        excluded.merge(reason, 1, Integer::sum);
        meterRegistry.counter("copy.eligibility.total",
                "mode", metricTag(executionMode),
                "result", "excluded",
                "reason", metricTag(reason)).increment();
    }

    private String primaryExclusionReason(List<UserCopyAllocationEntity> allocations, Map<String, Integer> excluded) {
        if (allocations == null || allocations.isEmpty()) return "ALLOCATION_EMPTY";
        return firstReason(excluded, "ALLOCATION_FILTERED");
    }

    private String firstReason(Map<String, Integer> excluded, String fallback) {
        if (excluded == null || excluded.isEmpty()) return fallback;
        return excluded.keySet().iterator().next();
    }

    private CopyRuntimeGuardPolicy.Decision guardAllowsNewEntry(UserCopyAllocationEntity allocation, Map<String, CopyStrategyGuardDecision> guardByProfile) {
        long startedNs = System.nanoTime();
        if (allocation == null) {
            return new CopyRuntimeGuardPolicy.Decision(false, "ALLOCATION_MISSING", "runtime_allocation");
        }
        String profileKey = copyStrategyRuntimeRouter.allocationKey(allocation);
        if (profileKey == null) {
            profileKey = safeLog(allocation.getWalletId()) + "|" + safeLog(allocation.getCopyStrategyCode());
        }
        CopyStrategyGuardDecision decision = guardByProfile.computeIfAbsent(profileKey, ignored ->
                copyStrategyGuardRuntimeCache.evaluateCached(
                        allocation.getWalletId(),
                        allocation.getCopyStrategyCode(),
                        allocation.getScopeType(),
                        allocation.getScopeValue()
                )
        );
        CopyRuntimeGuardPolicy.Decision runtimeDecision = copyRuntimeGuardPolicy.decide(allocation, decision);
        if ("PROMOTED_FULL_DECISION".equals(runtimeDecision.guardSource()) && decision != null) {
            meterRegistry.counter("copy.guard.conflict.total",
                    "stored_source", "promoted_full_decision",
                    "incoming_source", metricTag(decision.decisionSource())).increment();
        }
        meterRegistry.timer("copy_runtime_guard_duration",
                        "execution_mode", metricTag(allocation.getExecutionMode()),
                        "result", runtimeDecision.allowed() ? "allowed" : "blocked")
                .record(System.nanoTime() - startedNs, java.util.concurrent.TimeUnit.NANOSECONDS);
        meterRegistry.counter("copy.guard.decision.total",
                "mode", metricTag(allocation.getExecutionMode()),
                "status", runtimeDecision.allowed() ? "allowed" : "blocked",
                "source", metricTag(runtimeDecision.guardSource())).increment();
        log.info("event=copy.guard.decision userCopyAllocationId={} executionMode={} decision={} reasonCode={} guardSource={} decisionVersion={} computedAt={} expiresAt={} decisionFinal={} materializationStatus={} inputFingerprint={} elapsedMs={}",
                allocation.getId(),
                safeLog(allocation.getExecutionMode()),
                runtimeDecision.allowed() ? "ALLOW" : "BLOCK",
                safeLog(runtimeDecision.reasonCode()),
                safeLog(runtimeDecision.guardSource()),
                safeLog(runtimeDecision.decisionVersion()),
                runtimeDecision.computedAt(),
                runtimeDecision.expiresAt(),
                runtimeDecision.decisionFinal(),
                safeLog(runtimeDecision.materializationStatus()),
                safeLog(runtimeDecision.inputFingerprint()),
                elapsedMs(startedNs));
        if (!runtimeDecision.allowed()) {
            String reasonCode = guardBlockReason(allocation, runtimeDecision.reasonCode());
            log.warn("event=hyperliquid.direct_copy.guard_block allocationId={} userId={} wallet={} strategy={} status={} reason={} detail={} decision=BLOCK reasonCode={} guardReasonCode={} mutation=none hotPathDbWrite=false",
                    allocation.getId(), allocation.getIdUser(), safeLog(allocation.getWalletId()),
                    safeLog(allocation.getCopyStrategyCode()), safeLog(decision.statusWhenBlocked()),
                    safeLog(decision.reason()), safeLog(decision.detail()), reasonCode, safeLog(runtimeDecision.reasonCode()));
        }
        return runtimeDecision;
    }

    private String guardBlockReason(UserCopyAllocationEntity allocation, String guardReasonCode) {
        return isMicroLive(allocation) ? "MICRO_LIVE_GUARD_BLOCKED" : safeLog(guardReasonCode);
    }

    private String scopeBlockReason(UserCopyAllocationEntity allocation) {
        boolean symbolScope = allocation != null && allocation.getScopeType() != null
                && "SYMBOL".equalsIgnoreCase(allocation.getScopeType());
        return isMicroLive(allocation) && symbolScope
                ? "MICRO_LIVE_SYMBOL_NOT_ALLOWED"
                : "STRATEGY_SCOPE_NOT_MATCHED";
    }

    private boolean isMicroLive(UserCopyAllocationEntity allocation) {
        return allocation != null && "MICRO_LIVE".equals(
                UserCopyAllocationEntity.normalizeExecutionMode(allocation.getExecutionMode()));
    }

    private List<UserDetailDto> filterUsersById(List<UserDetailDto> usersCached, Set<String> userIds) {
        if (userIds == null || userIds.isEmpty() || usersCached == null || usersCached.isEmpty()) {
            return List.of();
        }
        return usersCached.stream()
                .filter(Objects::nonNull)
                .filter(u -> u.getUser() != null && u.getUser().getId() != null)
                .filter(u -> userIds.contains(u.getUser().getId().toString()))
                .toList();
    }

    private List<UserDetailDto> safeUsers(List<UserDetailDto> users) {
        return users == null ? Collections.emptyList() : users;
    }

    private String userIdsCsv(List<UserDetailDto> users) {
        if (users == null || users.isEmpty()) {
            return "";
        }
        return users.stream()
                .filter(Objects::nonNull)
                .filter(u -> u.getUser() != null && u.getUser().getId() != null)
                .map(u -> u.getUser().getId().toString())
                .sorted()
                .limit(20)
                .collect(Collectors.joining(","));
    }

    private String safeLog(String value) {
        if (value == null || value.isBlank()) {
            return "NA";
        }
        String clean = value.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ').replace('"', '\'');
        return clean.length() > 1000 ? clean.substring(0, 1000) : clean;
    }

    private long elapsedMs(long startedNs) {
        return (System.nanoTime() - startedNs) / 1_000_000L;
    }

    private String metricTag(String value) {
        if (value == null || value.isBlank()) return "unknown";
        String normalized = value.trim().toLowerCase(java.util.Locale.ROOT).replace('-', '_').replace(' ', '_');
        return normalized.length() > 40 ? normalized.substring(0, 40) : normalized;
    }

    public record CandidateUsers(
            List<UserDetailDto> usersCached,
            List<UserDetailDto> eligibleUsers,
            String source,
            String reasonCode,
            EligibilitySummary eligibilitySummary
    ) {
        public CandidateUsers(List<UserDetailDto> usersCached, List<UserDetailDto> eligibleUsers, String source) {
            this(usersCached, eligibleUsers, source, null,
                    new EligibilitySummary(0, eligibleUsers == null ? 0 : eligibleUsers.size(), Map.of()));
        }

        public CandidateUsers(List<UserDetailDto> usersCached, List<UserDetailDto> eligibleUsers, String source, String reasonCode) {
            this(usersCached, eligibleUsers, source, reasonCode,
                    new EligibilitySummary(0, eligibleUsers == null ? 0 : eligibleUsers.size(), Map.of()));
        }
    }

    public record EligibilitySummary(int totalAllocations, int eligibleAllocations, Map<String, Integer> excluded) {
        public EligibilitySummary {
            excluded = excluded == null ? Map.of() : Map.copyOf(excluded);
        }
    }
}
