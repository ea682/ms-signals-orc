package com.apunto.engine.service.impl;

import com.apunto.engine.dto.CopyOperationDto;
import com.apunto.engine.dto.OperacionDto;
import com.apunto.engine.dto.UserDetailDto;
import com.apunto.engine.events.OperacionEvent;
import com.apunto.engine.entity.UserCopyAllocationEntity;
import com.apunto.engine.hyperliquid.model.HyperliquidCopyLifecycleDecision;
import com.apunto.engine.hyperliquid.model.HyperliquidDeltaType;
import com.apunto.engine.hyperliquid.service.HyperliquidCopyLifecycleGuard;
import com.apunto.engine.jobs.model.CopyJobAction;
import com.apunto.engine.metric.TradingMetrics;
import com.apunto.engine.service.ActiveCopyOperationCache;
import com.apunto.engine.service.CopyExecutionJobService;
import com.apunto.engine.service.OperacionEventIngestService;
import com.apunto.engine.service.OperationMovementEventService;
import com.apunto.engine.service.MetricWalletService;
import com.apunto.engine.service.ShadowCopyTradingService;
import com.apunto.engine.service.UserCopyAllocationService;
import com.apunto.engine.service.UserDetailCachedService;
import com.apunto.engine.service.copy.CopyStrategyRuntimeRouter;
import com.apunto.engine.shared.exception.EngineException;
import com.apunto.engine.shared.exception.ValidationException;
import com.apunto.engine.shared.util.CopySymbolIdentity;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class OperacionEventIngestServiceImpl implements OperacionEventIngestService {

    private static final String ERR_EVENT_NULL = "El evento no puede ser null";
    private static final String ERR_TIPO_NULL = "El tipo del evento no puede ser null";
    private static final String ERR_OPERACION_NULL = "La operacion no puede ser null";
    private static final String ERR_ID_NULL = "El idOperacion no puede ser null";

    private static final String COPY_JOB_INGEST_SOURCE = "copy_job_ingest";
    private static final String LOG_ENQUEUED =
            "event=copy.execution.enqueued originId={} wallet={} symbol={} tipo={} action={} engineAction={} copyIntent={} deltaType={} source={} sourceCategory=DERIVED_COPY_TRADE metricEligible=false metricDecisionUse=audit_only_excluded_from_joyas usersCached={} eligibleUsers={} eligibleUserIds={} enqueued={} allocationFilter={}";

    private final UserDetailCachedService userDetailCachedService;
    private final UserCopyAllocationService userCopyAllocationService;
    private final CopyExecutionJobService copyExecutionJobService;
    private final ActiveCopyOperationCache activeCopyOperationCache;
    private final HyperliquidCopyLifecycleGuard lifecycleGuard;
    private final TradingMetrics tradingMetrics;
    private final OperationMovementEventService operationMovementEventService;
    private final CopyStrategyRuntimeRouter copyStrategyRuntimeRouter;
    private final MetricWalletService metricWalletService;
    private final ShadowCopyTradingService shadowCopyTradingService;

    @Value("${operation.job.ingest.filter-by-wallet-allocation:${copy.job.ingest.filter-by-wallet-allocation:true}}")
    private boolean filterByWalletAllocation;

    @Value("${operation.job.ingest.fallback-all-users-on-empty-allocation:${copy.job.ingest.fallback-all-users-on-empty-allocation:false}}")
    private boolean fallbackAllUsersOnEmptyAllocation;

    @Override
    public int ingest(OperacionEvent event) {
        long t0 = System.nanoTime();
        String tipo = "UNKNOWN";
        String result = "ok";

        try {
            requireNonNull(event, ERR_EVENT_NULL);
            requireNonNull(event.getTipo(), ERR_TIPO_NULL);
            requireNonNull(event.getOperacion(), ERR_OPERACION_NULL);
            requireNonNull(event.getOperacion().getIdOperacion(), ERR_ID_NULL);

            final OperacionDto operacion = event.getOperacion();
            tipo = event.getTipo().name();

            final String originId = operacion.getIdOperacion().toString();
            final String walletId = operacion.getIdCuenta();
            final CopyJobAction action = mapAction(event.getTipo());
            final HyperliquidDeltaType deltaType = HyperliquidDeltaType.from(event.getDeltaType());
            operationMovementEventService.recordAsync(
                    event,
                    COPY_JOB_INGEST_SOURCE,
                    activeCopyOperationCache.traceId(originId, "origin", walletId, operacion.getParSymbol()),
                    null
            );
            recordShadowSafely(event);

            final List<UserDetailDto> usersCached = safeUsers(userDetailCachedService.getUsers());
            final List<UserDetailDto> eligibleUsers = applyBusinessRules(event, action, resolveCandidateUsers(event, action, usersCached));
            final int enqueued = copyExecutionJobService.enqueueForUsers(event, eligibleUsers, action);

            tradingMetrics.jobsEnqueued(action.name(), eligibleUsers.size(), enqueued);

            log.info(LOG_ENQUEUED,
                    originId,
                    walletId,
                    operacion.getParSymbol(),
                    event.getTipo(),
                    displayAction(action, deltaType),
                    action,
                    copyIntent(action, deltaType),
                    deltaType,
                    COPY_JOB_INGEST_SOURCE,
                    usersCached.size(),
                    eligibleUsers.size(),
                    userIdsCsv(eligibleUsers),
                    enqueued,
                    filterByWalletAllocation);
            return enqueued;

        } catch (EngineException | DataAccessException | RestClientException | IllegalStateException | IllegalArgumentException ex) {
            result = "error";
            throw ex;
        } finally {
            tradingMetrics.ingestDuration(tipo, result, System.nanoTime() - t0);
        }
    }


    private List<UserDetailDto> resolveCandidateUsers(OperacionEvent event, CopyJobAction action, List<UserDetailDto> usersCached) {
        OperacionDto operation = event.getOperacion();
        HyperliquidDeltaType deltaType = HyperliquidDeltaType.from(event.getDeltaType());
        if (action == CopyJobAction.CLOSE) {
            return filterUsersById(usersCached, activeCopyOperationCache.activeUserIds(operation.getIdOperacion().toString()));
        }
        if (action == CopyJobAction.OPEN && deltaType.canAdjustExistingCopy()) {
            if (deltaType == HyperliquidDeltaType.FLIP) {
                return mergeUsers(
                        usersCached,
                        filterUsersById(usersCached, activeCopyOperationCache.activeUserIdsByWalletAndBaseSymbol(operation.getIdCuenta(), operation.getParSymbol())),
                        resolveEligibleUsers(operation, action, deltaType, usersCached)
                );
            }
            return filterUsersById(usersCached, activeCopyOperationCache.activeUserIds(operation.getIdOperacion().toString()));
        }
        return resolveEligibleUsers(operation, action, deltaType, usersCached);
    }

    private void recordShadowSafely(OperacionEvent event) {
        try {
            int recorded = shadowCopyTradingService.recordShadowEvent(event);
            if (recorded > 0) {
                OperacionDto op = event == null ? null : event.getOperacion();
                log.info("event=shadow_separate_ingest_ok originId={} walletId={} symbol={} recorded={} copyImpact=shadow_event_tracked",
                        op == null || op.getIdOperacion() == null ? null : op.getIdOperacion(),
                        op == null ? null : op.getIdCuenta(),
                        op == null ? null : op.getParSymbol(),
                        recorded);
            }
        } catch (RuntimeException ex) {
            OperacionDto op = event == null ? null : event.getOperacion();
            log.error("event=shadow_separate_ingest_failed originId={} walletId={} symbol={} reasonCode=shadow_ingest_failed copyImpact=live_not_blocked errClass={} errMsg=\"{}\"",
                    op == null || op.getIdOperacion() == null ? null : op.getIdOperacion(),
                    op == null ? null : op.getIdCuenta(),
                    op == null ? null : op.getParSymbol(),
                    ex.getClass().getSimpleName(), ex.getMessage());
        }
    }


    private List<UserDetailDto> mergeUsers(List<UserDetailDto> usersCached, List<UserDetailDto> first, List<UserDetailDto> second) {
        Set<UUID> ids = new java.util.LinkedHashSet<>();
        if (first != null) {
            first.stream()
                    .filter(Objects::nonNull)
                    .filter(u -> u.getUser() != null && u.getUser().getId() != null)
                    .map(u -> u.getUser().getId())
                    .forEach(ids::add);
        }
        if (second != null) {
            second.stream()
                    .filter(Objects::nonNull)
                    .filter(u -> u.getUser() != null && u.getUser().getId() != null)
                    .map(u -> u.getUser().getId())
                    .forEach(ids::add);
        }
        if (ids.isEmpty()) {
            return List.of();
        }
        return usersCached.stream()
                .filter(Objects::nonNull)
                .filter(u -> u.getUser() != null && u.getUser().getId() != null)
                .filter(u -> ids.contains(u.getUser().getId()))
                .toList();
    }

    private List<UserDetailDto> applyBusinessRules(OperacionEvent event, CopyJobAction action, List<UserDetailDto> users) {
        if (users == null || users.isEmpty()) {
            return List.of();
        }
        OperacionDto operation = event.getOperacion();
        String originId = operation.getIdOperacion().toString();
        String wallet = operation.getIdCuenta();
        String symbol = operation.getParSymbol();
        HyperliquidDeltaType deltaType = HyperliquidDeltaType.from(event.getDeltaType());
        String newSide = operation.getTipoOperacion() == null ? null : operation.getTipoOperacion().name();
        List<UserDetailDto> allowed = users.stream()
                .filter(u -> isBusinessAllowed(originId, wallet, symbol, newSide, action, deltaType, u))
                .toList();
        int skipped = users.size() - allowed.size();
        if (skipped > 0) {
            log.info("event=copy.execution.business_filter originId={} wallet={} symbol={} action={} engineAction={} deltaType={} eligibleBefore={} eligibleAfter={} skipped={} activeCacheSize={}",
                    originId, wallet, symbol, displayAction(action, deltaType), action, deltaType, users.size(), allowed.size(), skipped, activeCopyOperationCache.activeSize());
        }
        return allowed;
    }

    private boolean isBusinessAllowed(String originId, String wallet, String symbol, String newSide, CopyJobAction action, HyperliquidDeltaType deltaType, UserDetailDto user) {
        String uid = userId(user);
        if (uid == null) {
            log.info("event=copy.execution.business_skip originId={} userId=NA wallet={} symbol={} action={} engineAction={} deltaType={} reasonCode=user_missing cacheActive=false",
                    originId, wallet, symbol, displayAction(action, deltaType), action, deltaType);
            return false;
        }
        boolean active = deltaType == HyperliquidDeltaType.FLIP
                ? hasActiveCopyForFlip(uid, wallet, symbol, newSide)
                : activeCopyOperationCache.isActive(originId, uid);
        HyperliquidCopyLifecycleDecision decision = lifecycleGuard.decide(action, deltaType, active);
        if (decision.allowed()) {
            return true;
        }
        log.info("event=copy.execution.business_skip originId={} userId={} wallet={} symbol={} action={} engineAction={} deltaType={} reasonCode={} cacheActive={} activeCacheSize={}",
                originId, uid, wallet, symbol, displayAction(action, deltaType), action, deltaType, decision.reasonCode(), decision.cacheActive(), activeCopyOperationCache.activeSize());
        return false;
    }


    private boolean hasActiveCopyForFlip(String userId, String wallet, String symbol, String newSide) {
        if (userId == null || userId.isBlank() || wallet == null || wallet.isBlank()
                || symbol == null || symbol.isBlank() || newSide == null || newSide.isBlank()) {
            return false;
        }
        return activeCopyOperationCache.activeOperationsByUserAndWallet(userId, wallet).stream()
                .filter(Objects::nonNull)
                .filter(CopyOperationDto::isActive)
                .filter(copy -> CopySymbolIdentity.sameBaseAsset(copy.getParsymbol(), symbol))
                .anyMatch(copy -> copy.getTypeOperation() != null && !newSide.equalsIgnoreCase(copy.getTypeOperation()));
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

    private String userId(UserDetailDto user) {
        if (user == null || user.getUser() == null || user.getUser().getId() == null) {
            return null;
        }
        return user.getUser().getId().toString();
    }

    private void requireNonNull(Object value, String message) {
        if (value == null) {
            throw new ValidationException(message, java.util.Map.of("reason", "required_value_missing"));
        }
    }

    private List<UserDetailDto> resolveEligibleUsers(OperacionDto operation, CopyJobAction action, HyperliquidDeltaType deltaType, List<UserDetailDto> usersCached) {
        String walletId = operation == null ? null : operation.getIdCuenta();
        String side = operation == null || operation.getTipoOperacion() == null ? null : operation.getTipoOperacion().name();
        if (!filterByWalletAllocation) {
            return usersCached;
        }
        if (walletId == null || walletId.isBlank()) {
            log.warn("event=copy.execution.user_filter_skipped reason=wallet_missing usersCached={}", usersCached.size());
            return fallbackAllUsersOnEmptyAllocation ? usersCached : List.of();
        }

        final List<UserCopyAllocationEntity> activeAllocations = userCopyAllocationService.getActiveAllocationsByWallet(walletId);
        final Set<UUID> activeUserIds = activeAllocations.stream()
                .filter(Objects::nonNull)
                .filter(a -> copyStrategyRuntimeRouter.allocationAppliesToEvent(a, action, deltaType, side))
                .filter(a -> !requiresCopyHealthGuard(action, deltaType)
                        || metricWalletService.isCopyStrategyHealthyForCopy(a.getWalletId(), a.getCopyStrategyCode()))
                .map(UserCopyAllocationEntity::getIdUser)
                .filter(Objects::nonNull)
                .collect(Collectors.toUnmodifiableSet());
        if (activeUserIds.isEmpty()) {
            log.info("event=copy.execution.user_filter wallet={} activeAllocationUsers=0 usersCached={} eligibleUsers=0 eligibleUserIds=\"\" fallbackAllUsers={}",
                    walletId, usersCached.size(), fallbackAllUsersOnEmptyAllocation);
            return fallbackAllUsersOnEmptyAllocation ? usersCached : List.of();
        }

        final List<UserDetailDto> eligible = usersCached.stream()
                .filter(Objects::nonNull)
                .filter(u -> u.getUser() != null && u.getUser().getId() != null)
                .filter(u -> activeUserIds.contains(u.getUser().getId()))
                .toList();

        log.info("event=copy.execution.user_filter wallet={} side={} deltaType={} action={} activeAllocations={} matchingHealthyAllocationUsers={} usersCached={} eligibleUsers={} activeUserIds={} eligibleUserIds={}",
                walletId,
                side,
                deltaType,
                action,
                activeAllocations.size(),
                activeUserIds.size(),
                usersCached.size(),
                eligible.size(),
                uuidSetCsv(activeUserIds),
                userIdsCsv(eligible));
        return eligible;
    }

    private boolean requiresCopyHealthGuard(CopyJobAction action, HyperliquidDeltaType deltaType) {
        if (action != CopyJobAction.OPEN) {
            return false;
        }
        HyperliquidDeltaType effectiveDelta = deltaType == null ? HyperliquidDeltaType.UNKNOWN : deltaType;
        return effectiveDelta == HyperliquidDeltaType.OPEN
                || effectiveDelta == HyperliquidDeltaType.FLIP
                || effectiveDelta == HyperliquidDeltaType.UNKNOWN;
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

    private String uuidSetCsv(Set<UUID> ids) {
        if (ids == null || ids.isEmpty()) {
            return "";
        }
        return ids.stream()
                .filter(Objects::nonNull)
                .map(UUID::toString)
                .sorted()
                .limit(20)
                .collect(Collectors.joining(","));
    }

    private String displayAction(CopyJobAction action, HyperliquidDeltaType deltaType) {
        String intent = copyIntent(action, deltaType);
        if ("ADJUST".equals(intent) || "FLIP".equals(intent)) {
            return intent;
        }
        return action == null ? "UNKNOWN" : action.name();
    }

    private String copyIntent(CopyJobAction action, HyperliquidDeltaType deltaType) {
        HyperliquidDeltaType effectiveDelta = deltaType == null ? HyperliquidDeltaType.UNKNOWN : deltaType;
        if (action == CopyJobAction.CLOSE) {
            return "CLOSE";
        }
        if (effectiveDelta == HyperliquidDeltaType.FLIP) {
            return "FLIP";
        }
        if (action == CopyJobAction.OPEN && effectiveDelta.canAdjustExistingCopy()) {
            return "ADJUST";
        }
        if (action == CopyJobAction.OPEN) {
            return "OPEN";
        }
        return "UNKNOWN";
    }

    private CopyJobAction mapAction(OperacionEvent.Tipo tipo) {
        return switch (tipo) {
            case ABIERTA -> CopyJobAction.OPEN;
            case CERRADA -> CopyJobAction.CLOSE;
        };
    }

    private List<UserDetailDto> safeUsers(List<UserDetailDto> users) {
        return users == null ? Collections.emptyList() : users;
    }
}
