package com.apunto.engine.hyperliquid.service.impl;

import com.apunto.engine.dto.CopyOperationDto;
import com.apunto.engine.dto.OperacionDto;
import com.apunto.engine.dto.UserDetailDto;
import com.apunto.engine.entity.UserCopyAllocationEntity;
import com.apunto.engine.events.OperacionEvent;
import com.apunto.engine.hyperliquid.dto.HyperliquidMappedDelta;
import com.apunto.engine.hyperliquid.model.HyperliquidDeltaType;
import com.apunto.engine.jobs.model.CopyJobAction;
import com.apunto.engine.service.ActiveCopyOperationCache;
import com.apunto.engine.service.MetricWalletService;
import com.apunto.engine.service.UserCopyAllocationService;
import com.apunto.engine.service.UserDetailCachedService;
import com.apunto.engine.service.copy.CopyStrategyGuardDecision;
import com.apunto.engine.service.copy.CopyStrategyRuntimeRouter;
import com.apunto.engine.shared.util.CopyLogAdvice;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@Service
public class HyperliquidCopyCandidateResolver {

    private final UserDetailCachedService userDetailCachedService;
    private final UserCopyAllocationService userCopyAllocationService;
    private final ActiveCopyOperationCache activeCopyOperationCache;
    private final CopyStrategyRuntimeRouter copyStrategyRuntimeRouter;
    private final MetricWalletService metricWalletService;

    @Value("${operation.job.ingest.filter-by-wallet-allocation:${copy.job.ingest.filter-by-wallet-allocation:true}}")
    private boolean filterByWalletAllocation;

    @Value("${operation.job.ingest.fallback-all-users-on-empty-allocation:${copy.job.ingest.fallback-all-users-on-empty-allocation:false}}")
    private boolean fallbackAllUsersOnEmptyAllocation;

    @Value("${metric-wallet.copy-guard.cooldown-hours:48}")
    private long copyGuardCooldownHours;

    public HyperliquidCopyCandidateResolver(
            UserDetailCachedService userDetailCachedService,
            UserCopyAllocationService userCopyAllocationService,
            ActiveCopyOperationCache activeCopyOperationCache,
            CopyStrategyRuntimeRouter copyStrategyRuntimeRouter,
            MetricWalletService metricWalletService
    ) {
        this.userDetailCachedService = userDetailCachedService;
        this.userCopyAllocationService = userCopyAllocationService;
        this.activeCopyOperationCache = activeCopyOperationCache;
        this.copyStrategyRuntimeRouter = copyStrategyRuntimeRouter;
        this.metricWalletService = metricWalletService;
    }

    public CandidateUsers resolve(HyperliquidMappedDelta mappedDelta, CopyJobAction action) {
        OperacionEvent event = mappedDelta.event();
        OperacionDto operation = event.getOperacion();
        List<UserDetailDto> usersCached = safeUsers(userDetailCachedService.getUsers());
        Map<UUID, UserDetailDto> usersById = usersById(usersCached);
        HyperliquidDeltaType deltaType = HyperliquidDeltaType.from(mappedDelta.deltaType() == null ? event.getDeltaType() : mappedDelta.deltaType());

        if (action == CopyJobAction.CLOSE) {
            return activeCopyTargets(operation, usersCached, usersById, "active_copy_close");
        }

        if (action == CopyJobAction.OPEN && deltaType.canAdjustExistingCopy()) {
            if (deltaType == HyperliquidDeltaType.FLIP) {
                return flipTargets(operation, usersCached, usersById, action, deltaType);
            }
            return activeCopyTargets(operation, usersCached, usersById, "active_copy_adjustment");
        }

        return allocationTargets(operation, usersCached, usersById, action, deltaType);
    }

    private CandidateUsers activeCopyTargets(OperacionDto operation, List<UserDetailDto> usersCached, Map<UUID, UserDetailDto> usersById, String source) {
        String originId = operation.getIdOperacion() == null ? null : operation.getIdOperacion().toString();
        List<CandidateCopyTarget> targets = activeCopyOperationCache.activeOperations(originId).stream()
                .map(copy -> targetFromCopy(copy, usersById, operation, source))
                .filter(Objects::nonNull)
                .toList();
        String reasonCode = targets.isEmpty() ? ("active_copy_close".equals(source) ? "close_without_open_copy" : "resize_without_open_copy") : "active_copy_targets_found";
        String diagnostic = targets.isEmpty()
                ? CopyLogAdvice.fields(reasonCode, CopyLogAdvice.context(0, 0, 0, 0, null, null, activeCopyOperationCache.activeSize(), source))
                : "";
        log.info("event=copy_route.targets_resolved stage=target_selection source={} routeKind=active_copy originId={} wallet={} symbol={} activeTargets={} usersCached={} targetUsers={} targetStrategies={} reasonCode={} {}",
                source, safeLog(originId), safeLog(operation.getIdCuenta()), safeLog(operation.getParSymbol()), targets.size(), usersCached.size(), targets.size(), strategyCsv(targets), reasonCode, diagnostic);
        return new CandidateUsers(usersCached, targets, source);
    }

    private CandidateUsers activeCopyTargetsByWalletAndSymbol(OperacionDto operation, List<UserDetailDto> usersCached, Map<UUID, UserDetailDto> usersById, String source) {
        // FLIP necesita encontrar copias abiertas del mismo wallet + activo base aunque el originId cambie por el cambio de lado.
        List<CandidateCopyTarget> targets = activeCopyOperationCache.activeUserIdsByWalletAndBaseSymbol(operation.getIdCuenta(), operation.getParSymbol()).stream()
                .flatMap(userId -> activeCopyOperationCache.activeOperationsByUserAndWallet(userId, operation.getIdCuenta()).stream())
                .filter(copy -> CopyStrategyRuntimeRouter.normalizeWalletId(copy.getIdWalletOrigin()) != null)
                .filter(copy -> com.apunto.engine.shared.util.CopySymbolIdentity.sameBaseAsset(copy.getParsymbol(), operation.getParSymbol()))
                .map(copy -> targetFromCopy(copy, usersById, operation, source))
                .filter(Objects::nonNull)
                .toList();
        String reasonCode = targets.isEmpty() ? "flip_without_open_copy_same_base_asset" : "active_copy_targets_found_same_base_asset";
        log.info("event=copy_route.targets_resolved stage=target_selection source={} routeKind=active_copy_same_base wallet={} symbol={} targets={} targetStrategies={} reasonCode={}",
                source, safeLog(operation.getIdCuenta()), safeLog(operation.getParSymbol()), targets.size(), strategyCsv(targets), reasonCode);
        return new CandidateUsers(usersCached, targets, source);
    }

    private CandidateCopyTarget targetFromCopy(CopyOperationDto copy, Map<UUID, UserDetailDto> usersById, OperacionDto operation, String source) {
        if (copy == null || copy.getIdUser() == null) {
            return null;
        }
        UUID userId;
        try {
            userId = UUID.fromString(copy.getIdUser());
        } catch (IllegalArgumentException ex) {
            return null;
        }
        UserDetailDto user = usersById.get(userId);
        if (user == null) {
            return null;
        }
        UserCopyAllocationEntity allocation = null;
        if (copy.getCopyStrategyCode() != null && !copy.getCopyStrategyCode().isBlank()) {
            allocation = userCopyAllocationService.findOpenAllocation(userId, operation.getIdCuenta(), copy.getCopyStrategyCode()).orElse(null);
        }
        return new CandidateCopyTarget(user, allocation, copy.getCopyStrategyCode(), source);
    }

    private CandidateUsers flipTargets(OperacionDto operation, List<UserDetailDto> usersCached, Map<UUID, UserDetailDto> usersById, CopyJobAction action, HyperliquidDeltaType deltaType) {
        CandidateUsers activeTargets = activeCopyTargetsByWalletAndSymbol(operation, usersCached, usersById, "active_copy_flip");
        CandidateUsers allocationTargets = allocationTargets(operation, usersCached, usersById, action, deltaType);

        Map<String, CandidateCopyTarget> merged = new LinkedHashMap<>();
        for (CandidateCopyTarget target : activeTargets.copyTargets()) {
            merged.put(target.identityKey(), target);
        }
        for (CandidateCopyTarget target : allocationTargets.copyTargets()) {
            merged.put(target.identityKey(), target);
        }

        List<CandidateCopyTarget> targets = List.copyOf(merged.values());
        log.info("event=copy_route.targets_resolved stage=target_selection source=flip_merged routeKind=flip wallet={} symbol={} activeTargets={} allocationTargets={} mergedTargets={} targetStrategies={}",
                safeLog(operation == null ? null : operation.getIdCuenta()),
                safeLog(operation == null ? null : operation.getParSymbol()),
                activeTargets.copyTargets().size(),
                allocationTargets.copyTargets().size(),
                targets.size(),
                strategyCsv(targets));
        return new CandidateUsers(usersCached, targets, "flip_merged");
    }

    private CandidateUsers allocationTargets(OperacionDto operation, List<UserDetailDto> usersCached, Map<UUID, UserDetailDto> usersById, CopyJobAction action, HyperliquidDeltaType deltaType) {
        String walletId = operation.getIdCuenta();
        if (!filterByWalletAllocation) {
            List<CandidateCopyTarget> all = usersCached.stream()
                    .filter(Objects::nonNull)
                    .map(u -> new CandidateCopyTarget(u, null, CopyStrategyRuntimeRouter.DEFAULT_STRATEGY_CODE, "all_users_config"))
                    .toList();
            return new CandidateUsers(usersCached, all, "all_users_config");
        }
        if (walletId == null || walletId.isBlank()) {
            log.warn("event=copy_route.target_selection_skipped stage=target_selection reasonCode=wallet_missing usersCached={} {}", usersCached.size(),
                    CopyLogAdvice.fields("wallet_missing", CopyLogAdvice.context(null, 0, 0, 0, null, null, null, "allocation_wallet_missing")));
            List<CandidateCopyTarget> fallback = fallbackAllUsersOnEmptyAllocation
                    ? usersCached.stream().map(u -> new CandidateCopyTarget(u, null, CopyStrategyRuntimeRouter.DEFAULT_STRATEGY_CODE, "allocation_wallet_missing_fallback")).toList()
                    : List.of();
            return new CandidateUsers(usersCached, fallback, "allocation_wallet_missing");
        }

        String side = operation.getTipoOperacion() == null ? null : operation.getTipoOperacion().name();
        List<UserCopyAllocationEntity> activeAllocations = userCopyAllocationService.getActiveAllocationsByWallet(walletId);
        List<CandidateCopyTarget> targets = activeAllocations.stream()
                .filter(Objects::nonNull)
                .filter(a -> copyStrategyRuntimeRouter.allocationAppliesToEvent(a, action, deltaType, side))
                .filter(a -> guardAllowsNewEntry(a))
                .map(a -> targetFromAllocation(a, usersById))
                .filter(Objects::nonNull)
                .toList();
        if (targets.isEmpty()) {
            log.info("event=copy_route.targets_resolved stage=target_selection source=allocation routeKind=allocation wallet={} side={} deltaType={} action={} activeAllocations={} matchingTargets=0 usersCached={} fallbackAllUsers={} reasonCode=allocation_empty {}",
                    safeLog(walletId), safeLog(side), deltaType, action, activeAllocations.size(), usersCached.size(), fallbackAllUsersOnEmptyAllocation,
                    CopyLogAdvice.fields("allocation_empty", CopyLogAdvice.context(activeAllocations.size(), 0, 0, 0, null, null, 0, "allocation")));
            List<CandidateCopyTarget> fallback = fallbackAllUsersOnEmptyAllocation
                    ? usersCached.stream().map(u -> new CandidateCopyTarget(u, null, CopyStrategyRuntimeRouter.DEFAULT_STRATEGY_CODE, "allocation_empty_fallback")).toList()
                    : List.of();
            return new CandidateUsers(usersCached, fallback, "allocation_empty");
        }

        log.info("event=copy_route.targets_resolved stage=target_selection source=allocation routeKind=allocation wallet={} side={} deltaType={} action={} activeAllocations={} matchingTargets={} usersCached={} targetUsers={} targetStrategies={}",
                safeLog(walletId), safeLog(side), deltaType, action, activeAllocations.size(), targets.size(), usersCached.size(), userIdsCsv(targets), strategyCsv(targets));
        return new CandidateUsers(usersCached, targets, "allocation");
    }

    private CandidateCopyTarget targetFromAllocation(UserCopyAllocationEntity allocation, Map<UUID, UserDetailDto> usersById) {
        if (allocation == null || allocation.getIdUser() == null) {
            return null;
        }
        UserDetailDto user = usersById.get(allocation.getIdUser());
        if (user == null) {
            return null;
        }
        return new CandidateCopyTarget(user, allocation, allocation.getCopyStrategyCode(), "allocation");
    }

    private boolean guardAllowsNewEntry(UserCopyAllocationEntity allocation) {
        if (allocation == null) {
            return false;
        }
        CopyStrategyGuardDecision decision = metricWalletService.evaluateCopyStrategyForCopy(
                allocation.getWalletId(),
                allocation.getCopyStrategyCode()
        );
        if (decision.allowed()) {
            return true;
        }
        OffsetDateTime cooldownUntil = copyGuardCooldownHours <= 0
                ? null
                : OffsetDateTime.now().plusHours(copyGuardCooldownHours);
        userCopyAllocationService.markGuardBlocked(
                allocation.getIdUser(),
                allocation.getWalletId(),
                allocation.getCopyStrategyCode(),
                decision.statusWhenBlocked(),
                decision.reason() + ": " + decision.detail(),
                cooldownUntil
        );
        log.warn("event=copy_guard.blocked stage=target_selection allocationId={} userId={} wallet={} strategy={} status={} reason={} detail={} cooldownUntil={}",
                allocation.getId(), allocation.getIdUser(), safeLog(allocation.getWalletId()),
                safeLog(allocation.getCopyStrategyCode()), safeLog(decision.statusWhenBlocked()),
                safeLog(decision.reason()), safeLog(decision.detail()), cooldownUntil);
        return false;
    }

    private Map<UUID, UserDetailDto> usersById(List<UserDetailDto> usersCached) {
        Map<UUID, UserDetailDto> result = new LinkedHashMap<>();
        for (UserDetailDto user : safeUsers(usersCached)) {
            if (user == null || user.getUser() == null || user.getUser().getId() == null) {
                continue;
            }
            result.put(user.getUser().getId(), user);
        }
        return result;
    }

    private List<UserDetailDto> safeUsers(List<UserDetailDto> users) {
        return users == null ? Collections.emptyList() : users;
    }

    private String userIdsCsv(List<CandidateCopyTarget> targets) {
        if (targets == null || targets.isEmpty()) {
            return "";
        }
        return targets.stream()
                .map(CandidateCopyTarget::user)
                .filter(Objects::nonNull)
                .filter(u -> u.getUser() != null && u.getUser().getId() != null)
                .map(u -> u.getUser().getId().toString())
                .distinct()
                .sorted()
                .limit(20)
                .collect(Collectors.joining(","));
    }

    private String strategyCsv(List<CandidateCopyTarget> targets) {
        if (targets == null || targets.isEmpty()) {
            return "";
        }
        return targets.stream()
                .map(CandidateCopyTarget::strategyCode)
                .filter(v -> v != null && !v.isBlank())
                .distinct()
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

    public record CandidateUsers(
            List<UserDetailDto> usersCached,
            List<CandidateCopyTarget> copyTargets,
            String source
    ) {
        public List<UserDetailDto> eligibleUsers() {
            if (copyTargets == null || copyTargets.isEmpty()) {
                return List.of();
            }
            return copyTargets.stream().map(CandidateCopyTarget::user).filter(Objects::nonNull).distinct().toList();
        }
    }

    public record CandidateCopyTarget(
            UserDetailDto user,
            UserCopyAllocationEntity allocation,
            String strategyCode,
            String source
    ) {
        public String identityKey() {
            String userId = user == null || user.getUser() == null || user.getUser().getId() == null ? "unknown" : user.getUser().getId().toString();
            String strategy = strategyCode == null || strategyCode.isBlank() ? CopyStrategyRuntimeRouter.DEFAULT_STRATEGY_CODE : strategyCode.trim().replace('-', '_').toUpperCase(java.util.Locale.ROOT);
            return userId + '|' + strategy;
        }
    }
}
