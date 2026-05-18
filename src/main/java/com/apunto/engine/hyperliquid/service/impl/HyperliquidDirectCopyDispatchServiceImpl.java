package com.apunto.engine.hyperliquid.service.impl;

import com.apunto.engine.dto.OperacionDto;
import com.apunto.engine.dto.UserDetailDto;
import com.apunto.engine.events.OperacionEvent;
import com.apunto.engine.hyperliquid.dto.HyperliquidDirectCopyDispatchResult;
import com.apunto.engine.hyperliquid.dto.HyperliquidMappedDelta;
import com.apunto.engine.hyperliquid.model.HyperliquidCopyLifecycleDecision;
import com.apunto.engine.hyperliquid.model.HyperliquidDeltaType;
import com.apunto.engine.hyperliquid.service.HyperliquidCopyLifecycleGuard;
import com.apunto.engine.hyperliquid.service.HyperliquidDirectCopyDispatchService;
import com.apunto.engine.jobs.model.CopyJobAction;
import com.apunto.engine.service.ActiveCopyOperationCache;
import com.apunto.engine.service.BinanceCopyExecutionService;
import com.apunto.engine.service.binance.BinanceFuturesSymbolCatalog;
import com.apunto.engine.service.OperacionEventIngestService;
import com.apunto.engine.shared.exception.CopyPersistenceConflictException;
import com.apunto.engine.shared.exception.EngineException;
import com.apunto.engine.shared.exception.SkipExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Slf4j
@Service
public class HyperliquidDirectCopyDispatchServiceImpl implements HyperliquidDirectCopyDispatchService {

    private final BinanceCopyExecutionService binanceCopyExecutionService;
    private final ActiveCopyOperationCache activeCopyOperationCache;
    private final HyperliquidCopyLifecycleGuard lifecycleGuard;
    private final HyperliquidCopyCandidateResolver candidateResolver;
    private final OperacionEventIngestService fallbackIngestService;
    private final BinanceFuturesSymbolCatalog symbolCatalog;
    private final ThreadPoolTaskExecutor copyJobExecutor;

    @Value("${hyperliquid.direct-ingest.fallback-db-on-direct-failure:true}")
    private boolean fallbackDbOnDirectFailure;

    public HyperliquidDirectCopyDispatchServiceImpl(
            BinanceCopyExecutionService binanceCopyExecutionService,
            ActiveCopyOperationCache activeCopyOperationCache,
            HyperliquidCopyLifecycleGuard lifecycleGuard,
            HyperliquidCopyCandidateResolver candidateResolver,
            OperacionEventIngestService fallbackIngestService,
            BinanceFuturesSymbolCatalog symbolCatalog,
            @Qualifier("copyJobExecutor") ThreadPoolTaskExecutor copyJobExecutor
    ) {
        this.binanceCopyExecutionService = binanceCopyExecutionService;
        this.activeCopyOperationCache = activeCopyOperationCache;
        this.lifecycleGuard = lifecycleGuard;
        this.candidateResolver = candidateResolver;
        this.fallbackIngestService = fallbackIngestService;
        this.symbolCatalog = symbolCatalog;
        this.copyJobExecutor = copyJobExecutor;
    }

    @Override
    public HyperliquidDirectCopyDispatchResult dispatch(HyperliquidMappedDelta mappedDelta) {
        long startedNs = System.nanoTime();
        requireMappedDelta(mappedDelta);

        OperacionEvent event = mappedDelta.event();
        OperacionDto operacion = event.getOperacion();
        String originId = operacion.getIdOperacion().toString();
        String wallet = operacion.getIdCuenta();
        String symbol = operacion.getParSymbol();
        HyperliquidDeltaType deltaType = HyperliquidDeltaType.from(firstNonBlank(mappedDelta.deltaType(), event.getDeltaType()));
        CopyJobAction action = mapAction(event.getTipo());
        String actionLabel = displayAction(action, deltaType);

        if (symbolCatalog.resolve(symbol).isEmpty()) {
            long elapsedMs = Duration.ofNanos(System.nanoTime() - startedNs).toMillis();
            String traceId = activeCopyOperationCache.traceId(originId, "NA", wallet, symbol);
            log.info("event=hyperliquid.direct_copy.business_skip category=copy reasonAlias=binance_symbol_unsupported friendlyReason=simbolo_no_existe_en_binance explanation=no_se_copia_porque_binance_no_soporta_el_simbolo copyImpact=no_copy_order traceId={} originId={} userId=NA wallet={} symbol={} action={} engineAction={} copyIntent={} deltaType={} reasonCode=binance_symbol_unsupported cacheActive=false activeCacheSize={} source=binance_symbol_catalog",
                    traceId, originId, safeLog(wallet), safeLog(symbol), actionLabel, action, copyIntent(action, deltaType), deltaType, activeCopyOperationCache.activeSize());
            log.info("event=hyperliquid.direct_copy.dispatched traceId={} originId={} wallet={} symbol={} action={} engineAction={} copyIntent={} deltaType={} usersCached=0 eligibleUsers=0 eligibleUserIds= submitted=0 businessSkipped=1 fallbackJobs=0 fallbackUsed=false source=binance_symbol_catalog elapsedMs={}",
                    traceId, originId, safeLog(wallet), safeLog(symbol), actionLabel, action, copyIntent(action, deltaType), deltaType, elapsedMs);
            return new HyperliquidDirectCopyDispatchResult(0, 0, 1, 0, false);
        }

        HyperliquidCopyCandidateResolver.CandidateUsers candidates = candidateResolver.resolve(mappedDelta, action);
        List<UserDetailDto> eligibleUsers = candidates.eligibleUsers();
        AtomicBoolean fallbackSubmitted = new AtomicBoolean(false);
        AtomicInteger submitted = new AtomicInteger(0);
        AtomicInteger fallbackJobs = new AtomicInteger(0);
        AtomicInteger businessSkipped = new AtomicInteger(0);

        if (eligibleUsers.isEmpty() && action == CopyJobAction.OPEN && deltaType.canAdjustExistingCopy()) {
            businessSkipped.incrementAndGet();
            String traceId = activeCopyOperationCache.traceId(originId, "NA", wallet, symbol);
            log.info("event=hyperliquid.direct_copy.business_skip category=copy reasonAlias=adjustment_without_active_copy friendlyReason=ajuste_sin_copia_activa explanation=ajuste_no_copiado_porque_no_existe_copia_abierta copyImpact=no_copy_order traceId={} originId={} userId=NA wallet={} symbol={} action={} engineAction={} copyIntent={} deltaType={} reasonCode={} cacheActive=false activeCacheSize={} source={}",
                    traceId, originId, safeLog(wallet), safeLog(symbol), actionLabel, action, copyIntent(action, deltaType), deltaType, adjustmentReason(deltaType), activeCopyOperationCache.activeSize(), candidates.source());
        }

        if (eligibleUsers.isEmpty() && action == CopyJobAction.CLOSE) {
            businessSkipped.incrementAndGet();
            String traceId = activeCopyOperationCache.traceId(originId, "NA", wallet, symbol);
            log.info("event=hyperliquid.direct_copy.business_skip category=copy reasonAlias=close_without_active_copy friendlyReason=cierre_sin_copia_activa explanation=cierre_no_copiado_porque_no_existe_copia_abierta copyImpact=no_copy_order traceId={} originId={} userId=NA wallet={} symbol={} action={} engineAction={} copyIntent={} deltaType={} reasonCode=close_without_open_copy cacheActive=false activeCacheSize={} source={}",
                    traceId, originId, safeLog(wallet), safeLog(symbol), actionLabel, action, copyIntent(action, deltaType), deltaType, activeCopyOperationCache.activeSize(), candidates.source());
        }

        for (UserDetailDto user : eligibleUsers) {
            String userTraceId = activeCopyOperationCache.traceId(originId, userId(user), wallet, symbol);
            HyperliquidCopyLifecycleDecision decision = businessDecision(originId, action, deltaType, user);
            if (!decision.allowed()) {
                businessSkipped.incrementAndGet();
                log.info("event=hyperliquid.direct_copy.business_skip category=copy reasonAlias=blocked_by_lifecycle_guard friendlyReason=guard_bloqueo_la_copia explanation=no_se_envio_orden_porque_la_regla_de_lifecycle_no_lo_permitio copyImpact=no_copy_order traceId={} originId={} userId={} wallet={} symbol={} action={} engineAction={} copyIntent={} deltaType={} reasonCode={} cacheActive={} activeCacheSize={} source={}",
                        userTraceId, originId, userId(user), safeLog(wallet), safeLog(symbol), actionLabel, action, copyIntent(action, deltaType), deltaType, decision.reasonCode(), decision.cacheActive(), activeCopyOperationCache.activeSize(), candidates.source());
                continue;
            }
            try (MDC.MDCCloseable ignored = MDC.putCloseable("traceId", userTraceId)) {
                copyJobExecutor.execute(() -> executeCopy(event, user, action, fallbackSubmitted, fallbackJobs));
                submitted.incrementAndGet();
            } catch (RejectedExecutionException rejected) {
                log.warn("event=hyperliquid.direct_copy.rejected traceId={} originId={} wallet={} symbol={} action={} engineAction={} deltaType={} eligibleUsers={} submitted={} errClass={} errMsg=\"{}\"",
                        userTraceId, originId, safeLog(wallet), safeLog(symbol), actionLabel, action, deltaType, eligibleUsers.size(), submitted.get(),
                        rejected.getClass().getSimpleName(), safeLog(rejected.getMessage()));
                fallbackJobs.addAndGet(submitFallbackOnce(event, fallbackSubmitted, "executor_rejected"));
                break;
            }
        }

        long elapsedMs = Duration.ofNanos(System.nanoTime() - startedNs).toMillis();
        log.info("event=hyperliquid.direct_copy.dispatched traceId={} originId={} wallet={} symbol={} action={} engineAction={} copyIntent={} deltaType={} usersCached={} eligibleUsers={} eligibleUserIds={} submitted={} businessSkipped={} fallbackJobs={} fallbackUsed={} source={} elapsedMs={}",
                activeCopyOperationCache.traceId(originId, "summary", wallet, symbol),
                originId,
                safeLog(wallet),
                safeLog(symbol),
                actionLabel,
                action,
                copyIntent(action, deltaType),
                deltaType,
                candidates.usersCached().size(),
                eligibleUsers.size(),
                userIdsCsv(eligibleUsers),
                submitted.get(),
                businessSkipped.get(),
                fallbackJobs.get(),
                fallbackSubmitted.get(),
                candidates.source(),
                elapsedMs);

        return new HyperliquidDirectCopyDispatchResult(
                eligibleUsers.size(),
                submitted.get(),
                businessSkipped.get(),
                fallbackJobs.get(),
                fallbackSubmitted.get()
        );
    }

    private HyperliquidCopyLifecycleDecision businessDecision(String originId, CopyJobAction action, HyperliquidDeltaType deltaType, UserDetailDto user) {
        String uid = userId(user);
        if (uid == null || uid.isBlank() || "unknown".equals(uid)) {
            return HyperliquidCopyLifecycleDecision.skip("user_missing", false);
        }
        boolean active = activeCopyOperationCache.isActive(originId, uid);
        return lifecycleGuard.decide(action, deltaType, active);
    }

    private void executeCopy(
            OperacionEvent event,
            UserDetailDto user,
            CopyJobAction action,
            AtomicBoolean fallbackSubmitted,
            AtomicInteger fallbackJobs
    ) {
        long startedNs = System.nanoTime();
        OperacionDto operacion = event.getOperacion();
        String originId = operacion.getIdOperacion().toString();
        String wallet = operacion.getIdCuenta();
        String symbol = operacion.getParSymbol();
        String userId = userId(user);
        String traceId = activeCopyOperationCache.traceId(originId, userId, wallet, symbol);
        HyperliquidDeltaType eventDeltaType = HyperliquidDeltaType.from(event.getDeltaType());
        String actionLabel = displayAction(action, eventDeltaType);
        try {
            if (action == CopyJobAction.OPEN) {
                binanceCopyExecutionService.executeOpenForUser(event, user);
            } else {
                binanceCopyExecutionService.executeCloseForUser(event, user);
            }
            log.info("event=hyperliquid.direct_copy.completed traceId={} originId={} userId={} wallet={} symbol={} action={} engineAction={} copyIntent={} deltaType={} elapsedMs={}",
                    traceId, originId, userId, safeLog(wallet), safeLog(symbol), actionLabel, action, copyIntent(action, eventDeltaType), safeLog(event.getDeltaType()), elapsedMs(startedNs));
        } catch (SkipExecutionException skip) {
            log.info("event=hyperliquid.direct_copy.skipped traceId={} originId={} userId={} wallet={} symbol={} action={} engineAction={} copyIntent={} deltaType={} reasonCode={} reason=\"{}\" details=\"{}\" elapsedMs={}",
                    traceId,
                    originId,
                    userId,
                    safeLog(wallet),
                    safeLog(symbol),
                    actionLabel,
                    action,
                    copyIntent(action, eventDeltaType),
                    safeLog(event.getDeltaType()),
                    safeLog(skip.getReasonCode()),
                    safeLog(skip.getReason()),
                    safeLog(skip.getDetails()),
                    elapsedMs(startedNs));
        } catch (EngineException | DataAccessException | RestClientException | IllegalStateException | IllegalArgumentException | ArithmeticException ex) {
            int fallback = ex instanceof CopyPersistenceConflictException
                    ? 0
                    : submitFallbackOnce(event, fallbackSubmitted, "direct_execution_failed");
            fallbackJobs.addAndGet(fallback);
            if (shouldLogStacktrace(ex)) {
                log.error("event=hyperliquid.direct_copy.failed traceId={} originId={} userId={} wallet={} symbol={} action={} engineAction={} deltaType={} fallbackJobs={} errClass={} errMsg=\"{}\" elapsedMs={}",
                        traceId, originId, userId, safeLog(wallet), safeLog(symbol), actionLabel, action, safeLog(event.getDeltaType()), fallback,
                        ex.getClass().getSimpleName(), safeLog(ex.getMessage()), elapsedMs(startedNs), ex);
            } else {
                log.error("event=hyperliquid.direct_copy.failed traceId={} originId={} userId={} wallet={} symbol={} action={} engineAction={} deltaType={} fallbackJobs={} errClass={} errMsg=\"{}\" elapsedMs={}",
                        traceId, originId, userId, safeLog(wallet), safeLog(symbol), actionLabel, action, safeLog(event.getDeltaType()), fallback,
                        ex.getClass().getSimpleName(), safeLog(ex.getMessage()), elapsedMs(startedNs));
            }
        }
    }

    private int submitFallbackOnce(OperacionEvent event, AtomicBoolean fallbackSubmitted, String reason) {
        if (!fallbackDbOnDirectFailure || !fallbackSubmitted.compareAndSet(false, true)) {
            return 0;
        }
        try {
            int jobs = fallbackIngestService.ingest(event);
            OperacionDto operacion = event.getOperacion();
            log.warn("event=hyperliquid.direct_copy.fallback_db_enqueued traceId={} reason={} originId={} wallet={} symbol={} jobs={}",
                    activeCopyOperationCache.traceId(String.valueOf(operacion.getIdOperacion()), "fallback", operacion.getIdCuenta(), operacion.getParSymbol()),
                    reason,
                    operacion.getIdOperacion(),
                    safeLog(operacion.getIdCuenta()),
                    safeLog(operacion.getParSymbol()),
                    jobs);
            return jobs;
        } catch (EngineException | DataAccessException | RestClientException | IllegalStateException | IllegalArgumentException fallbackEx) {
            OperacionDto operacion = event.getOperacion();
            log.error("event=hyperliquid.direct_copy.fallback_db_failed traceId={} reason={} originId={} wallet={} symbol={} errClass={} errMsg=\"{}\"",
                    activeCopyOperationCache.traceId(String.valueOf(operacion.getIdOperacion()), "fallback", operacion.getIdCuenta(), operacion.getParSymbol()),
                    reason,
                    operacion.getIdOperacion(),
                    safeLog(operacion.getIdCuenta()),
                    safeLog(operacion.getParSymbol()),
                    fallbackEx.getClass().getSimpleName(),
                    safeLog(fallbackEx.getMessage()));
            return 0;
        }
    }

    private boolean shouldLogStacktrace(Throwable ex) {
        return !(ex instanceof EngineException || ex instanceof RestClientException || ex instanceof IllegalArgumentException);
    }

    private void requireMappedDelta(HyperliquidMappedDelta mappedDelta) {
        if (mappedDelta == null) {
            throw new IllegalArgumentException("HyperliquidMappedDelta is required");
        }
        requireEvent(mappedDelta.event());
    }

    private void requireEvent(OperacionEvent event) {
        if (event == null) {
            throw new IllegalArgumentException("OperacionEvent is required");
        }
        if (event.getTipo() == null) {
            throw new IllegalArgumentException("OperacionEvent.tipo is required");
        }
        if (event.getOperacion() == null) {
            throw new IllegalArgumentException("OperacionEvent.operacion is required");
        }
        if (event.getOperacion().getIdOperacion() == null) {
            throw new IllegalArgumentException("OperacionEvent.operacion.idOperacion is required");
        }
    }

    private CopyJobAction mapAction(OperacionEvent.Tipo tipo) {
        return switch (tipo) {
            case ABIERTA -> CopyJobAction.OPEN;
            case CERRADA -> CopyJobAction.CLOSE;
        };
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
        if (effectiveDelta.canAdjustExistingCopy()) {
            return "ADJUST";
        }
        if (effectiveDelta.isOpen()) {
            return "OPEN";
        }
        return "SKIP";
    }

    private String adjustmentReason(HyperliquidDeltaType deltaType) {
        return switch (deltaType) {
            case FLIP -> "flip_without_open_copy";
            case UPDATE -> "update_without_open_copy";
            case RESIZE -> "resize_without_open_copy";
            default -> "adjustment_without_open_copy";
        };
    }

    private String userId(UserDetailDto user) {
        if (user == null || user.getUser() == null || user.getUser().getId() == null) {
            return "unknown";
        }
        return user.getUser().getId().toString();
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

    private long elapsedMs(long startedNs) {
        return Duration.ofNanos(System.nanoTime() - startedNs).toMillis();
    }

    private String firstNonBlank(String first, String second) {
        if (first != null && !first.isBlank()) {
            return first.trim();
        }
        if (second != null && !second.isBlank()) {
            return second.trim();
        }
        return null;
    }

    private String safeLog(String value) {
        if (value == null || value.isBlank()) {
            return "NA";
        }
        String clean = value.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ').replace('"', '\'');
        return clean.length() > 1000 ? clean.substring(0, 1000) : clean;
    }
}
