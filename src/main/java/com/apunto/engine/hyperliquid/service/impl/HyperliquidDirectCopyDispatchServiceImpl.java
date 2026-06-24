package com.apunto.engine.hyperliquid.service.impl;

import com.apunto.engine.dto.CopyOperationDto;
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
import com.apunto.engine.metric.TradingMetrics;
import com.apunto.engine.service.ActiveCopyOperationCache;
import com.apunto.engine.service.BinanceCopyExecutionService;
import com.apunto.engine.service.OperacionEventIngestService;
import com.apunto.engine.shared.exception.CopyPersistenceConflictException;
import com.apunto.engine.shared.exception.EngineException;
import com.apunto.engine.shared.exception.SkipExecutionException;
import com.apunto.engine.shared.util.CopyLogAdvice;
import com.apunto.engine.shared.util.CopySymbolIdentity;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@Slf4j
@Service
public class HyperliquidDirectCopyDispatchServiceImpl implements HyperliquidDirectCopyDispatchService {

    private final BinanceCopyExecutionService binanceCopyExecutionService;
    private final ActiveCopyOperationCache activeCopyOperationCache;
    private final HyperliquidCopyLifecycleGuard lifecycleGuard;
    private final HyperliquidCopyCandidateResolver candidateResolver;
    private final OperacionEventIngestService fallbackIngestService;
    private final ThreadPoolTaskExecutor copyJobExecutor;
    private final TradingMetrics tradingMetrics;

    @Value("${hyperliquid.direct-ingest.fallback-db-on-direct-failure:true}")
    private boolean fallbackDbOnDirectFailure;

    public HyperliquidDirectCopyDispatchServiceImpl(
            BinanceCopyExecutionService binanceCopyExecutionService,
            ActiveCopyOperationCache activeCopyOperationCache,
            HyperliquidCopyLifecycleGuard lifecycleGuard,
            HyperliquidCopyCandidateResolver candidateResolver,
            OperacionEventIngestService fallbackIngestService,
            @Qualifier("copyJobExecutor") ThreadPoolTaskExecutor copyJobExecutor,
            TradingMetrics tradingMetrics
    ) {
        this.binanceCopyExecutionService = binanceCopyExecutionService;
        this.activeCopyOperationCache = activeCopyOperationCache;
        this.lifecycleGuard = lifecycleGuard;
        this.candidateResolver = candidateResolver;
        this.fallbackIngestService = fallbackIngestService;
        this.copyJobExecutor = copyJobExecutor;
        this.tradingMetrics = tradingMetrics;
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


        HyperliquidCopyCandidateResolver.CandidateUsers candidates = candidateResolver.resolve(mappedDelta, action);
        List<UserDetailDto> eligibleUsers = candidates.eligibleUsers();
        AtomicBoolean fallbackSubmitted = new AtomicBoolean(false);
        AtomicInteger submitted = new AtomicInteger(0);
        AtomicInteger fallbackJobs = new AtomicInteger(0);
        AtomicInteger businessSkipped = new AtomicInteger(0);
        AtomicReference<String> firstReasonCode = new AtomicReference<>();

        if (eligibleUsers.isEmpty() && candidates.reasonCode() != null && !candidates.reasonCode().isBlank()
                && !(action == CopyJobAction.OPEN && deltaType.canAdjustExistingCopy())
                && action != CopyJobAction.CLOSE) {
            businessSkipped.incrementAndGet();
            firstReasonCode.compareAndSet(null, candidates.reasonCode());
            String traceId = originTraceId(originId, wallet, symbol);
            log.info("event=hyperliquid.direct_copy.business_skip category=copy reasonAlias=allocation_empty friendlyReason=sin_asignacion_live explanation=no_se_envio_orden_porque_no_hay_usuarios_live_asignados_a_este_wallet copyImpact=no_copy_order traceId={} originId={} userId=NA wallet={} symbol={} action={} engineAction={} copyIntent={} deltaType={} reasonCode={} cacheActive=false activeCacheSize={} source={} {}",
                    traceId, originId, safeLog(wallet), safeLog(symbol), actionLabel, action, copyIntent(action, deltaType), deltaType, candidates.reasonCode(), activeCopyOperationCache.activeSize(), candidates.source(),
                    CopyLogAdvice.fields(candidates.reasonCode(), CopyLogAdvice.context(eligibleUsers.size(), eligibleUsers.size(), submitted.get(), businessSkipped.get(), null, false, activeCopyOperationCache.activeSize(), candidates.source())));
        }

        if (eligibleUsers.isEmpty() && action == CopyJobAction.OPEN && deltaType.canAdjustExistingCopy()) {
            businessSkipped.incrementAndGet();
            firstReasonCode.compareAndSet(null, adjustmentReason(deltaType));
            String traceId = originTraceId(originId, wallet, symbol);
            String reasonCode = adjustmentReason(deltaType);
            log.info("event=hyperliquid.direct_copy.business_skip category=copy reasonAlias=adjustment_without_active_copy friendlyReason=ajuste_sin_copia_activa explanation=ajuste_no_copiado_porque_no_existe_copia_abierta copyImpact=no_copy_order traceId={} originId={} userId=NA wallet={} symbol={} action={} engineAction={} copyIntent={} deltaType={} reasonCode={} cacheActive=false activeCacheSize={} source={} {}",
                    traceId, originId, safeLog(wallet), safeLog(symbol), actionLabel, action, copyIntent(action, deltaType), deltaType, reasonCode, activeCopyOperationCache.activeSize(), candidates.source(),
                    CopyLogAdvice.fields(reasonCode, CopyLogAdvice.context(eligibleUsers.size(), eligibleUsers.size(), submitted.get(), businessSkipped.get(), null, false, activeCopyOperationCache.activeSize(), candidates.source())));
        }

        if (eligibleUsers.isEmpty() && action == CopyJobAction.CLOSE) {
            businessSkipped.incrementAndGet();
            firstReasonCode.compareAndSet(null, "close_without_open_copy");
            String traceId = originTraceId(originId, wallet, symbol);
            log.info("event=hyperliquid.direct_copy.business_skip category=copy reasonAlias=close_without_active_copy friendlyReason=cierre_sin_copia_activa explanation=cierre_no_copiado_porque_no_existe_copia_abierta copyImpact=no_copy_order traceId={} originId={} userId=NA wallet={} symbol={} action={} engineAction={} copyIntent={} deltaType={} reasonCode=close_without_open_copy cacheActive=false activeCacheSize={} source={} {}",
                    traceId, originId, safeLog(wallet), safeLog(symbol), actionLabel, action, copyIntent(action, deltaType), deltaType, activeCopyOperationCache.activeSize(), candidates.source(),
                    CopyLogAdvice.fields("close_without_open_copy", CopyLogAdvice.context(eligibleUsers.size(), eligibleUsers.size(), submitted.get(), businessSkipped.get(), null, false, activeCopyOperationCache.activeSize(), candidates.source())));
        }

        for (UserDetailDto user : eligibleUsers) {
            String userTraceId = activeCopyOperationCache.traceId(originId, userId(user), wallet, symbol);
            HyperliquidCopyLifecycleDecision decision = businessDecision(
                    originId,
                    wallet,
                    symbol,
                    operacion.getTipoOperacion() == null ? null : operacion.getTipoOperacion().name(),
                    action,
                    deltaType,
                    user
            );
            if (!decision.allowed()) {
                businessSkipped.incrementAndGet();
                firstReasonCode.compareAndSet(null, decision.reasonCode());
                log.info("event=hyperliquid.direct_copy.business_skip category=copy reasonAlias=blocked_by_lifecycle_guard friendlyReason=guard_bloqueo_la_copia explanation=no_se_envio_orden_porque_la_regla_de_lifecycle_no_lo_permitio copyImpact=no_copy_order traceId={} originId={} userId={} wallet={} symbol={} action={} engineAction={} copyIntent={} deltaType={} reasonCode={} cacheActive={} activeCacheSize={} source={} {}",
                        userTraceId, originId, userId(user), safeLog(wallet), safeLog(symbol), actionLabel, action, copyIntent(action, deltaType), deltaType, decision.reasonCode(), decision.cacheActive(), activeCopyOperationCache.activeSize(), candidates.source(),
                        CopyLogAdvice.fields(decision.reasonCode(), CopyLogAdvice.context(eligibleUsers.size(), eligibleUsers.size(), submitted.get(), businessSkipped.get(), null, decision.cacheActive(), activeCopyOperationCache.activeSize(), candidates.source())));
                continue;
            }
            try (MDC.MDCCloseable ignored = MDC.putCloseable("traceId", userTraceId)) {
                copyJobExecutor.execute(() -> executeCopy(event, user, action, fallbackSubmitted, fallbackJobs));
                submitted.incrementAndGet();
            } catch (RejectedExecutionException rejected) {
                tradingMetrics.directCopyRejected(copyIntent(action, deltaType), "executor_rejected");
                log.warn("event=hyperliquid.direct_copy.rejected traceId={} originId={} wallet={} symbol={} action={} engineAction={} deltaType={} eligibleUsers={} submitted={} reasonCode=executor_rejected errClass={} errMsg=\"{}\" humanMessage=no_hay_espacio_en_la_cola_para_enviar_esta_copia_ahora {}",
                        userTraceId, originId, safeLog(wallet), safeLog(symbol), actionLabel, action, deltaType, eligibleUsers.size(), submitted.get(),
                        rejected.getClass().getSimpleName(), safeLog(rejected.getMessage()),
                        CopyLogAdvice.fields("executor_rejected", CopyLogAdvice.context(eligibleUsers.size(), eligibleUsers.size(), submitted.get(), businessSkipped.get(), null, null, activeCopyOperationCache.activeSize(), candidates.source())));
                fallbackJobs.addAndGet(submitFallbackOnce(event, fallbackSubmitted, "executor_rejected"));
                break;
            }
        }

        long elapsedMs = Duration.ofNanos(System.nanoTime() - startedNs).toMillis();
        String dispatchReasonCode = firstReasonCode.get();
        String dispatchDiagnostic = dispatchReasonCode == null ? "" : CopyLogAdvice.fields(
                dispatchReasonCode,
                CopyLogAdvice.context(eligibleUsers.size(), eligibleUsers.size(), submitted.get(), businessSkipped.get(), null, null, activeCopyOperationCache.activeSize(), candidates.source())
        );
        log.info("event=hyperliquid.direct_copy.dispatched traceId={} originId={} wallet={} symbol={} action={} engineAction={} copyIntent={} deltaType={} usersCached={} eligibleUsers={} eligibleUserIds={} submitted={} businessSkipped={} fallbackJobs={} fallbackUsed={} source={} copySkipReasonCode={} elapsedMs={} humanMessage=termine_de_decidir_a_quienes_se_debe_copiar_esta_operacion {}",
                originTraceId(originId, wallet, symbol),
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
                safeLog(dispatchReasonCode),
                elapsedMs,
                dispatchDiagnostic);
        tradingMetrics.directCopyDispatch(copyIntent(action, deltaType), eligibleUsers.size(), submitted.get(), businessSkipped.get(), fallbackJobs.get(), fallbackSubmitted.get(), dispatchReasonCode, elapsedMs);

        return HyperliquidDirectCopyDispatchResult.ok(
                eligibleUsers.size(),
                submitted.get(),
                businessSkipped.get(),
                fallbackJobs.get(),
                fallbackSubmitted.get(),
                firstReasonCode.get()
        );
    }

    private String originTraceId(String originId, String wallet, String symbol) {
        return activeCopyOperationCache.traceId(originId, "origin", wallet, symbol);
    }

    private HyperliquidCopyLifecycleDecision businessDecision(
            String originId,
            String wallet,
            String symbol,
            String newSide,
            CopyJobAction action,
            HyperliquidDeltaType deltaType,
            UserDetailDto user
    ) {
        String uid = userId(user);
        if (uid == null || uid.isBlank() || "unknown".equals(uid)) {
            return HyperliquidCopyLifecycleDecision.skip("user_missing", false);
        }
        boolean active = deltaType == HyperliquidDeltaType.FLIP
                ? hasActiveCopyForFlip(uid, wallet, symbol, newSide)
                : activeCopyOperationCache.isActive(originId, uid);
        return lifecycleGuard.decide(action, deltaType, active);
    }

    private boolean hasActiveCopyForFlip(String userId, String wallet, String symbol, String newSide) {
        if (userId == null || userId.isBlank() || wallet == null || wallet.isBlank()
                || symbol == null || symbol.isBlank() || newSide == null || newSide.isBlank()) {
            return false;
        }
        // FLIP llega con originId nuevo porque cambia el lado (LONG<->SHORT).
        // Para permitirlo debe existir una copia activa del mismo wallet + activo base, pero del lado anterior.
        // Ejemplo valido: evento BTCUSD y copia BTCUSDT/BTCUSDC.
        return activeCopyOperationCache.activeOperationsByUserAndWallet(userId, wallet).stream()
                .filter(Objects::nonNull)
                .filter(CopyOperationDto::isActive)
                .filter(copy -> CopySymbolIdentity.sameBaseAsset(copy.getParsymbol(), symbol))
                .anyMatch(copy -> copy.getTypeOperation() != null && !newSide.equalsIgnoreCase(copy.getTypeOperation()));
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
        try (MDC.MDCCloseable ignored = MDC.putCloseable("traceId", traceId)) {
            try {
                if (action == CopyJobAction.OPEN) {
                    binanceCopyExecutionService.executeOpenForUser(event, user);
                } else {
                    binanceCopyExecutionService.executeCloseForUser(event, user);
                }
                long elapsedMs = elapsedMs(startedNs);
                tradingMetrics.directCopyExecution(copyIntent(action, eventDeltaType), "completed", "none", elapsedMs);
                log.info("event=hyperliquid.direct_copy.completed traceId={} originId={} userId={} wallet={} symbol={} action={} engineAction={} copyIntent={} deltaType={} elapsedMs={} humanMessage=la_copia_se_envio_y_termino_bien",
                        traceId, originId, userId, safeLog(wallet), safeLog(symbol), actionLabel, action, copyIntent(action, eventDeltaType), safeLog(event.getDeltaType()), elapsedMs);
            } catch (SkipExecutionException skip) {
                tradingMetrics.directCopyExecution(copyIntent(action, eventDeltaType), "skipped", skip.getReasonCode(), elapsedMs(startedNs));
                log.info("event=hyperliquid.direct_copy.skipped traceId={} originId={} userId={} wallet={} symbol={} action={} engineAction={} copyIntent={} deltaType={} reasonCode={} reason=\"{}\" details=\"{}\" elapsedMs={} humanMessage=esta_copia_se_salto_por_una_regla_del_negocio {}",
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
                        elapsedMs(startedNs),
                        CopyLogAdvice.fields(skip.getReasonCode(), CopyLogAdvice.context(null, null, null, 1, null, null, activeCopyOperationCache.activeSize(), "direct_execution")));
            } catch (EngineException | DataAccessException | RestClientException | IllegalStateException | IllegalArgumentException | ArithmeticException ex) {
                int fallback = ex instanceof CopyPersistenceConflictException
                        ? 0
                        : submitFallbackOnce(event, fallbackSubmitted, "direct_execution_failed");
                fallbackJobs.addAndGet(fallback);
                tradingMetrics.directCopyExecution(copyIntent(action, eventDeltaType), "failed", "direct_execution_failed", elapsedMs(startedNs));
                if (shouldLogStacktrace(ex)) {
                    log.error("event=hyperliquid.direct_copy.failed traceId={} originId={} userId={} wallet={} symbol={} action={} engineAction={} deltaType={} reasonCode=direct_execution_failed fallbackJobs={} errClass={} errMsg=\"{}\" elapsedMs={} humanMessage=intente_copiar_pero_algo_fallo_y_revise_si_pude_dejarlo_en_cola_de_respaldo {}",
                            traceId, originId, userId, safeLog(wallet), safeLog(symbol), actionLabel, action, safeLog(event.getDeltaType()), fallback,
                            ex.getClass().getSimpleName(), safeLog(ex.getMessage()), elapsedMs(startedNs),
                            CopyLogAdvice.fields("direct_execution_failed", CopyLogAdvice.context(null, null, null, null, null, null, activeCopyOperationCache.activeSize(), "direct_execution")), ex);
                } else {
                    log.error("event=hyperliquid.direct_copy.failed traceId={} originId={} userId={} wallet={} symbol={} action={} engineAction={} deltaType={} reasonCode=direct_execution_failed fallbackJobs={} errClass={} errMsg=\"{}\" elapsedMs={} humanMessage=intente_copiar_pero_algo_fallo_y_revise_si_pude_dejarlo_en_cola_de_respaldo {}",
                            traceId, originId, userId, safeLog(wallet), safeLog(symbol), actionLabel, action, safeLog(event.getDeltaType()), fallback,
                            ex.getClass().getSimpleName(), safeLog(ex.getMessage()), elapsedMs(startedNs),
                            CopyLogAdvice.fields("direct_execution_failed", CopyLogAdvice.context(null, null, null, null, null, null, activeCopyOperationCache.activeSize(), "direct_execution")));
                }
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
            tradingMetrics.directCopyFallback("enqueued", reason);
            log.warn("event=hyperliquid.direct_copy.fallback_db_enqueued traceId={} reason={} originId={} wallet={} symbol={} jobs={} humanMessage=la_copia_directa_fallo_pero_deje_un_trabajo_de_respaldo_en_la_base",
                    activeCopyOperationCache.traceId(String.valueOf(operacion.getIdOperacion()), "fallback", operacion.getIdCuenta(), operacion.getParSymbol()),
                    reason,
                    operacion.getIdOperacion(),
                    safeLog(operacion.getIdCuenta()),
                    safeLog(operacion.getParSymbol()),
                    jobs);
            return jobs;
        } catch (EngineException | DataAccessException | RestClientException | IllegalStateException | IllegalArgumentException fallbackEx) {
            OperacionDto operacion = event.getOperacion();
            tradingMetrics.directCopyFallback("failed", reason);
            log.error("event=hyperliquid.direct_copy.fallback_db_failed traceId={} reason={} originId={} wallet={} symbol={} errClass={} errMsg=\"{}\" humanMessage=fallo_tambien_el_respaldo_y_esta_copia_necesita_revision",
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
