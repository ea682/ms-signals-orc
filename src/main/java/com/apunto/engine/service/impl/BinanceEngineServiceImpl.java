package com.apunto.engine.service.impl;

import com.apunto.engine.dto.CopyOperationDto;
import com.apunto.engine.dto.CopyOperationEventRecordCommand;
import com.apunto.engine.dto.FuturesPositionDto;
import com.apunto.engine.dto.OriginBasketPositionDto;
import com.apunto.engine.dto.OperacionDto;
import com.apunto.engine.dto.OperationDto;
import com.apunto.engine.dto.UserDetailDto;
import com.apunto.engine.dto.client.BinanceFuturesOrderClientResponse;
import com.apunto.engine.dto.client.BinanceFuturesSymbolFilterDto;
import com.apunto.engine.dto.client.BinanceFuturesSymbolInfoClientDto;
import com.apunto.engine.dto.client.MetricaWalletDto;
import com.apunto.engine.entity.UserCopyAllocationEntity;
import com.apunto.engine.events.OperacionEvent;
import com.apunto.engine.hyperliquid.model.HyperliquidDeltaType;
import com.apunto.engine.jobs.model.CopyJobAction;
import com.apunto.engine.mapper.CopyTradingMapper;
import com.apunto.engine.service.ActiveCopyOperationCache;
import com.apunto.engine.service.BinanceCopyExecutionService;
import com.apunto.engine.service.BinanceEngineService;
import com.apunto.engine.service.CopyOperationService;
import com.apunto.engine.service.CopyOperationEventService;
import com.apunto.engine.service.DistributedLockService;
import com.apunto.engine.service.FuturesPositionService;
import com.apunto.engine.service.MetricWalletService;
import com.apunto.engine.service.UserCopyAllocationService;
import com.apunto.engine.service.ProcesBinanceService;
import com.apunto.engine.service.copy.CopyMinNotionalPolicy;
import com.apunto.engine.service.copy.CopyMinNotionalPolicyResolver;
import com.apunto.engine.service.copy.CopyStrategyRuntimeRouter;
import com.apunto.engine.service.copy.ProportionalCopySizingCalculator;
import com.apunto.engine.shared.enums.OrderType;
import com.apunto.engine.shared.enums.PositionSide;
import com.apunto.engine.shared.enums.PositionStatus;
import com.apunto.engine.shared.enums.FuturesCapitalAsset;
import com.apunto.engine.shared.enums.Side;
import com.apunto.engine.shared.exception.CopyBinanceClientException;
import com.apunto.engine.shared.exception.EngineException;
import com.apunto.engine.shared.exception.ErrorCode;
import com.apunto.engine.shared.exception.CopySymbolMetadataException;
import com.apunto.engine.shared.exception.SkipExecutionException;
import com.apunto.engine.shared.util.LogFmt;
import com.apunto.engine.shared.util.IdempotencyKeyUtil;
import com.apunto.engine.shared.util.CopyLogAdvice;
import com.apunto.engine.shared.util.CopySymbolIdentity;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.math.BigDecimal.ONE;

@Service
@Slf4j
@RequiredArgsConstructor
public class BinanceEngineServiceImpl implements BinanceEngineService, BinanceCopyExecutionService {

    private static final double DEFAULT_BASE_CAPITAL = 1_000.0;
    private static final BigDecimal MIN_NOTIONAL_FALLBACK = new BigDecimal("5");
    private static final BigDecimal ZERO = BigDecimal.ZERO;
    private static final BigDecimal REBALANCE_TOLERANCE_PCT = new BigDecimal("0.02");

    private static final int DEFAULT_CALC_SCALE = 18;
    private static final long TTL_MS = 60_000L;

    private static final String FILTER_TYPE_LOT_SIZE = "LOT_SIZE";
    private static final String FILTER_TYPE_MARKET_LOT_SIZE = "MARKET_LOT_SIZE";
    private static final String FILTER_TYPE_MIN_NOTIONAL = "MIN_NOTIONAL";
    private static final String FILTER_TYPE_NOTIONAL = "NOTIONAL";

    private static final String ERR_EVENT_NULL = "El evento de operación no puede ser null";
    private static final String ERR_USERS_NULL = "La lista de usuarios no puede ser null";
    private static final String ERR_USER_NULL = "El detalle del usuario no puede ser null";

    private static final String LOG_OPEN_DUPLICATE_INMEM = "event=operation.open.duplicate_inmemory originId={} action=ignore";
    private static final String LOG_OPEN_SKIP_ALREADY = "event=operation.open.skip_already_processed originId={} userId={}";
    private static final String LOG_OPEN_SCHEDULED = "event=operation.open.scheduled originId={} users={} scheduled={} uniqueUsers={} delayBetweenMs={}";

    private static final String LOG_CLOSE_NO_COPIES = "event=operation.close.no_copies originId={} users={}";
    private static final String LOG_CLOSE_COPY_MISSING = "event=operation.close.copy_missing originId={} userId={}";
    private static final String LOG_CLOSE_SCHEDULED = "event=operation.close.scheduled originId={} users={} scheduled={} delayBetweenMs={}";

    private static final String LOG_OPEN_SKIP_BUDGET = "event=operation.open.skip_budget originId={} userId={} wallet={} symbol={} marginRequired={} marginBudget={} usedMargin={}";


    private static final String LOG_PREP_INVALID_METRIC = "event=operation.open.invalid_metric originId={} userId={} wallet={} reason=invalid_capitalShare";
    private static final String LOG_PREP_INVALID_BUDGET = "event=operation.open.invalid_budget originId={} userId={} wallet={} reason=walletBudget<=0";
    private static final String LOG_PREP_INVALID_CAPITAL_REFERENCE = "event=operation.open.invalid_capital_reference originId={} userId={} wallet={} reference={}";
    private static final String LOG_PREP_INVALID_SIZE = "event=operation.open.invalid_size originId={} userId={} wallet={} reason=size<=0";
    private static final String LOG_PREP_INVALID_SOURCE_MARGIN = "event=operation.open.invalid_source_margin originId={} userId={} wallet={} sourceMargin={}";
    private static final String LOG_PREP_INVALID_FRACTION = "event=operation.open.invalid_fraction originId={} userId={} wallet={} fraction={}";
    private static final String LOG_PREP_BUDGET_CLAMPED = "event=operation.open.budget_clamped originId={} userId={} wallet={} symbol={} requestedMargin={} allowedMargin={} usedMargin={} walletBudget={} reserve={} hardCap={}";
    private static final String LOG_PREP_SKIP_NO_ROOM = "event=operation.open.skip_no_room originId={} userId={} wallet={} symbol={} walletBudget={} usedMargin={} reserve={} hardCap={}";
    private static final String LOG_PREP_INVALID_ENTRY = "event=operation.open.invalid_entry_price originId={} userId={} wallet={} reason=entryPrice<=0";
    private static final String LOG_PREP_INVALID_SYMBOL = "event=operation.open.invalid_symbol originId={} userId={} wallet={} reason=symbol_blank";
    private static final String LOG_PREP_SKIP_TOO_SMALL = "event=operation.open.skip_too_small originId={} userId={} wallet={} symbol={} notional={}";
    private static final String LOG_PREP_SKIP_SYMBOL_RULES = "event=operation.open.skip_symbol_rules originId={} userId={} wallet={} symbol={} notionalMax={}";
    private static final String LOG_PREP_SKIP_NOTIONAL_ZERO = "event=operation.open.skip_notional_zero originId={} userId={} wallet={} symbol={}";
    private static final String LOG_PREP_PREPARED = "event=operation.open.prepared originId={} userId={} wallet={} symbol={} sourceMargin={} capitalReference={} fraction={} walletBudget={} usedMargin={} availableBufferedMargin={} marginThisTrade={} leverage={} notionalFinal={} marginRequired={} copyByMinNotional={}";
    private static final String LOG_PREP_COPY_BY_MIN_NOTIONAL = "event=operation.open.copy_by_min_notional originId={} userId={} wallet={} symbol={} candidateNotional={} forcedNotional={} minNotional={} policyMode={} allocationId={} score={} minScore={} historyDays={} minHistoryDays={} operationsCount={} minOperations={} maxForcedNotional={} notionalBudgetCeiling={}";
    private static final BigDecimal MAX_MARGIN_SAFETY_PCT = new BigDecimal("0.30");

    @Value("${binance.symbols.api-key:}")
    private String symbolsApiKey;

    private static final String LOG_QTY_ADJUSTED = "event=binance.qty_adjusted symbol={} qtyOriginal={} qtyFinal={} notionalFinal={} notionalMax={}";
    private static final String LOG_SYMBOLS_CACHE_REFRESH = "event=binance.symbols_cache.refresh size={} ttlMs={}";

    private static final String LOG_COPY_CREATE_INVALID = "event=copyop.create.invalid_order idUser={} idWallet={} idOperation={}";
    private static final String LOG_CLOSE_INVALID_RESPONSE = "event=binance.close.invalid_response originId={} userId={} symbol={}";
    private static final String LOG_CLOSE_OK = "event=binance.close.ok originId={} userId={} wallet={} symbol={} qty={} orderId={}";
    private static final String LOG_CLOSE_SKIPPED_ORIGIN_STILL_VALIDATION_ACTIVE = "event=copy_close_skipped reason=origin_still_active originId={} userId={} originActive={}";
    private static final String LOG_OPEN_START = "event=copy_open_start originId={} userId={}";
    private static final String LOG_OPEN_VALIDATION_OK = "event=copy_open_ok originId={} userId={}";
    private static final String LOG_RECONCILE_START = "event=copy_reconcile_start triggerOriginId={} userId={} wallet={} triggerType={}";
    private static final String LOG_RECONCILE_DONE = "event=copy_reconcile_done triggerOriginId={} userId={} wallet={} sourcePositions={} targets={} activeCopies={}";

    private static final int USER_LEVERAGE_MIN = 1;
    private static final int USER_LEVERAGE_MAX = 20;

    private static final List<String> KNOWN_QUOTES = List.of(
            "USDT", "USDC", "FDUSD", "BUSD", "BTC", "ETH"
    );

    private static final Pattern LEADING_MULTIPLIER_PATTERN =
            Pattern.compile("^(\\d+)([A-Z0-9]+)$");
    private static final List<String> CONTRACT_MULTIPLIERS = List.of("1000000000", "1000000", "1000");

    private final ProcesBinanceService procesBinanceService;
    private final ThreadPoolTaskScheduler binanceTaskScheduler;
    private final MetricWalletService metricWalletService;
    private final UserCopyAllocationService userCopyAllocationService;
    private final CopyOperationService copyOperationService;
    private final CopyOperationEventService copyOperationEventService;
    private final ActiveCopyOperationCache activeCopyOperationCache;
    private final DistributedLockService distributedLockService;
    private final ProportionalCopySizingCalculator copySizingCalculator;
    private final CopyMinNotionalPolicyResolver copyMinNotionalPolicyResolver;
    private final CopyStrategyRuntimeRouter copyStrategyRuntimeRouter;

    @Value("${binance.dispatch.delay-ms:0}")
    private long delayBetweenMs;

    @Value("${engine.copy.margin-safety-buffer-pct:0.05}")
    private BigDecimal marginSafetyBufferPct;

    @Value("${binance.symbols.cache-ttl-ms:60000}")
    private long symbolsCacheTtlMs;

    @Value("${engine.copy.wallet-hardcap-over-pct:0.03}")
    private BigDecimal walletHardcapOverPct;

    @Value("${engine.copy.wallet-reserve-pct:0.01}")
    private BigDecimal walletReservePct;

    @Value("${engine.copy.sizing.base-capital-cap:10000}")
    private double sizingBaseCapitalCap;

    @Value("${engine.copy.sizing.max-fraction-per-trade:0.1}")
    private BigDecimal maxFractionPerTrade;

    @Value("${engine.copy.sizing.notional-buffer-pct:0.03}")
    private BigDecimal notionalBufferPct;

    @Value("${engine.copy.reconcile.on-open:true}")
    private boolean reconcileOnOpen;

    @Value("${engine.copy.reconcile.on-close:true}")
    private boolean reconcileOnClose;

    @Value("${engine.copy.reconcile.trigger-scoped:true}")
    private boolean triggerScopedReconcile;

    @Value("${engine.copy.reconcile.max-missing-open-age:PT60S}")
    private Duration maxMissingOpenAge;

    @Value("${engine.copy.reconcile.max-missing-open-price-drift-pct:0.003}")
    private BigDecimal maxMissingOpenPriceDriftPct;

    @Value("${engine.copy.reconcile.skip-stale-increase:true}")
    private boolean skipStaleIncrease;

    @Value("${engine.copy.reconcile.max-increase-source-age:PT60S}")
    private Duration maxIncreaseSourceAge;

    @Value("${engine.copy.flip.close-previous-when-new-open-blocked:true}")
    private boolean closePreviousSideWhenFlipOpenBlocked;

    @Value("${engine.copy.rebalance.skip-small-increase:true}")
    private boolean skipSmallRebalanceIncrease;

    @Value("${copy.rules.min-notional-floor-usdt:0}")
    private BigDecimal configuredMinNotionalFloor;

    @Value("${copy.rules.min-notional-safety-pct:0.05}")
    private BigDecimal minNotionalSafetyPct;

    private final CopyTradingMapper copyTradingMapper;
    private final FuturesPositionService futuresPositionService;

    private final ConcurrentMap<String, Long> processedOperations = new ConcurrentHashMap<>();

    private final Object symbolsCacheLock = new Object();
    private final AtomicBoolean symbolsRefreshInFlight = new AtomicBoolean(false);
    private volatile SymbolsCache symbolsCache = new SymbolsCache(
            0L,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptySet()
    );

    @Override
    public void openOperation(OperacionEvent event, List<UserDetailDto> usersDetail) {
        Objects.requireNonNull(event, ERR_EVENT_NULL);
        Objects.requireNonNull(usersDetail, ERR_USERS_NULL);

        final OperacionDto originOperation = requireOperacion(event);
        final String originId = originOperation.getIdOperacion().toString();
        final long now = System.currentTimeMillis();

        boolean[] isNew = {false};
        processedOperations.compute(originId, (key, previousTs) -> {
            if (previousTs == null || now - previousTs > TTL_MS) {
                isNew[0] = true;
                return now;
            }
            return previousTs;
        });

        if (!isNew[0] && !reconcileOnOpen) {
            log.warn(LOG_OPEN_DUPLICATE_INMEM, originId);
            return;
        }
        if (!isNew[0]) {
            log.info("event=operation.open.duplicate_inmemory action=reconcile_allowed originId={}", originId);
        }

        processedOperations.entrySet().removeIf(e -> now - e.getValue() > TTL_MS * 5);

        final Set<String> alreadyCopiedUsers = copyOperationService.findOperationsByOrigin(originId)
                .stream()
                .map(CopyOperationDto::getIdUser)
                .collect(Collectors.toSet());

        final long baseTime = System.currentTimeMillis();
        int scheduled = 0;

        for (UserDetailDto userDetail : usersDetail) {
            final String userId = userDetail.getUser().getId().toString();
            if (alreadyCopiedUsers.contains(userId) && !reconcileOnOpen) {
                log.info(LOG_OPEN_SKIP_ALREADY, originId, userId);
                continue;
            }
            if (alreadyCopiedUsers.contains(userId)) {
                log.info("event=operation.open.already_processed action=reconcile_allowed originId={} userId={}", originId, userId);
            }

            final long delay = (long) scheduled * delayBetweenMs;
            final Instant executionTime = Instant.ofEpochMilli(baseTime + delay);
            final String traceId = activeCopyOperationCache.traceId(originId, userId, originOperation.getIdCuenta(), originOperation.getParSymbol());
            binanceTaskScheduler.schedule(() -> {
                try (MDC.MDCCloseable ignored = MDC.putCloseable("traceId", traceId)) {
                    executeOpenForUser(event, userDetail);
                }
            }, executionTime);
            scheduled++;
        }

        log.info(LOG_OPEN_SCHEDULED, originId, usersDetail.size(), scheduled, usersDetail.size(), delayBetweenMs);
    }

    @Override
    public void closeOperation(OperacionEvent event, List<UserDetailDto> usersDetail) {
        Objects.requireNonNull(event, ERR_EVENT_NULL);
        Objects.requireNonNull(usersDetail, ERR_USERS_NULL);

        final long baseTime = System.currentTimeMillis();
        final OperacionDto originOperation = requireOperacion(event);
        final String originId = originOperation.getIdOperacion().toString();

        final List<CopyOperationDto> copiesByOrigin = copyOperationService.findOperationsByOrigin(originId)
                .stream()
                .filter(Objects::nonNull)
                .filter(CopyOperationDto::isActive)
                .toList();
        final Map<String, UserDetailDto> usersById = usersDetail.stream()
                .filter(Objects::nonNull)
                .filter(u -> u.getUser() != null && u.getUser().getId() != null)
                .collect(Collectors.toMap(u -> u.getUser().getId().toString(), java.util.function.Function.identity(), (a, b) -> a));

        if (!copiesByOrigin.isEmpty()) {
            int scheduled = 0;
            for (CopyOperationDto copy : copiesByOrigin) {
                UserDetailDto userDetail = usersById.get(copy.getIdUser());
                if (userDetail == null) {
                    log.warn("event=operation.close.copy_user_missing category=legacy_close reasonCode=user_not_cached originId={} userId={} strategy={} copyId={}",
                            originId, copy.getIdUser(), normalizeStrategy(copy.getCopyStrategyCode()), copy.getIdOperation());
                    continue;
                }
                final long delay = (long) scheduled * delayBetweenMs;
                final Instant executionTime = Instant.ofEpochMilli(baseTime + delay);
                final String traceId = activeCopyOperationCache.traceId(originId, copy.getIdUser(), originOperation.getIdCuenta(), originOperation.getParSymbol(), copy.getCopyStrategyCode());
                final UserCopyAllocationEntity allocation = allocationMetadataFromCopy(copy);
                binanceTaskScheduler.schedule(() -> {
                    try (MDC.MDCCloseable ignored = MDC.putCloseable("traceId", traceId)) {
                        executeCloseForUser(event, userDetail, allocation, copy.getCopyStrategyCode());
                    }
                }, executionTime);
                scheduled++;
            }
            log.info("event=operation.close.scheduled_strategy_aware category=legacy_close originId={} activeCopies={} scheduled={} delayBetweenMs={} strategies={}",
                    originId, copiesByOrigin.size(), scheduled, delayBetweenMs,
                    copiesByOrigin.stream().map(CopyOperationDto::getCopyStrategyCode).filter(Objects::nonNull).distinct().sorted().collect(Collectors.joining(",")));
            return;
        }

        if (!reconcileOnClose) {
            log.warn(LOG_CLOSE_NO_COPIES, originId, usersDetail.size());
            return;
        }
        log.info("event=operation.close.no_direct_copies action=reconcile_allowed originId={} users={}", originId, usersDetail.size());

        int scheduled = 0;
        for (UserDetailDto userDetail : usersDetail) {
            final String userId = userDetail.getUser().getId().toString();
            log.info("event=operation.close.copy_missing action=reconcile_allowed originId={} userId={}", originId, userId);
            final long delay = (long) scheduled * delayBetweenMs;
            final Instant executionTime = Instant.ofEpochMilli(baseTime + delay);
            final String traceId = activeCopyOperationCache.traceId(originId, userId, originOperation.getIdCuenta(), originOperation.getParSymbol());
            binanceTaskScheduler.schedule(() -> {
                try (MDC.MDCCloseable ignored = MDC.putCloseable("traceId", traceId)) {
                    executeCloseForUser(event, userDetail);
                }
            }, executionTime);
            scheduled++;
        }

        log.info(LOG_CLOSE_SCHEDULED, originId, usersDetail.size(), scheduled, delayBetweenMs);
    }

    @Override
    public void executeOpenForUser(OperacionEvent event, UserDetailDto userDetail) {
        executeOpenForUser(event, userDetail, null);
    }

    @Override
    public void executeOpenForUser(OperacionEvent event, UserDetailDto userDetail, UserCopyAllocationEntity allocationOverride) {
        Objects.requireNonNull(event, ERR_EVENT_NULL);
        Objects.requireNonNull(userDetail, ERR_USER_NULL);

        final OperacionDto originOperation = requireOperacion(event);
        final String originId = originOperation.getIdOperacion().toString();
        final UUID userId = userDetail.getUser().getId();
        final HyperliquidDeltaType deltaType = HyperliquidDeltaType.from(event.getDeltaType());
        final Optional<FuturesPositionDto> positionOriginDto = futuresPositionService.getIdFuturesPosition(originId);

        log.debug(LOG_OPEN_START, originId, userId);

        if (!originOperation.isOperacionActiva()) {
            log.info("event=copy_open_skipped reasonCode=origin_not_active reason=origin_not_active originId={} userId={} wallet={} symbol={}",
                    originId, userId, safeLog(originOperation.getIdCuenta()), safeLog(originOperation.getParSymbol()));
            return;
        }

        final boolean staleOpen = positionOriginDto
                .map(dto -> !PositionStatus.OPEN.equals(dto.getIsActive()))
                .orElse(false);

        if (staleOpen) {
            log.info("event=copy_open_skipped reasonCode=stale_open_event reason=stale_open_event originId={} userId={} wallet={} symbol={} originStatus={}",
                    originId, userId, safeLog(originOperation.getIdCuenta()), safeLog(originOperation.getParSymbol()),
                    positionOriginDto.map(FuturesPositionDto::getIsActive).orElse(null));
            return;
        }

        if (deltaType.canAdjustExistingCopy()) {
            log.info("event=copy.adjust.reconcile_required category=copy reasonCode=adjustment_reconcile_required reasonAlias=adjustment_uses_basket friendlyReason=ajuste_por_reconciliacion explanation=los_RESIZE_UPDATE_FLIP_se_resuelven_con_reconciliacion_para_evitar_usar_estado_viejo_o_abrir_duplicados copyImpact=copy_safe_path originId={} userId={} wallet={} symbol={} deltaType={} triggerScopedReconcile={} {}",
                    originId,
                    userId,
                    safeLog(originOperation.getIdCuenta()),
                    safeLog(originOperation.getParSymbol()),
                    deltaType,
                    triggerScopedReconcile,
                    CopyLogAdvice.fields("adjustment_reconcile_required", CopyLogAdvice.context(null, null, null, null, null, null, activeCopyOperationCache.activeSize(), "binance_engine")));
            reconcileWalletBasketForUser(event, userDetail, allocationOverride);
            return;
        }

        if (reconcileOnOpen && !triggerScopedReconcile) {
            reconcileWalletBasketForUser(event, userDetail, allocationOverride);
        } else if (reconcileOnOpen) {
            final long existingNs = System.nanoTime();
            final String strategyOverride = allocationOverride == null ? null : allocationOverride.getCopyStrategyCode();
            final CopyOperationDto existingCopy = strategyOverride == null || strategyOverride.isBlank()
                    ? copyOperationService.findOperationForUser(originId, userId.toString())
                    : copyOperationService.findOperationForUserAndStrategy(originId, userId.toString(), strategyOverride);
            log.info("event=copy.job.phase action=OPEN phase=load_existing_copy originId={} userId={} elapsedMs={}",
                    originId, userId, elapsedMsSince(existingNs));
            if (existingCopy != null && existingCopy.isActive()) {
                log.info("event=copy.open.trigger_scoped action=reconcile_existing_copy originId={} userId={} symbol={} copyId={}",
                        originId, userId, originOperation.getParSymbol(), existingCopy.getIdOperation());
                reconcileWalletBasketForUser(event, userDetail, allocationOverride);
            } else {
                executeDirectOpenForUser(event, userDetail, allocationOverride);
            }
        } else {
            executeDirectOpenForUser(event, userDetail, allocationOverride);
        }

        log.debug(LOG_OPEN_VALIDATION_OK, originId, userId);
    }

    @Override
    public void executeCloseForUser(OperacionEvent event, UserDetailDto userDetail) {
        executeCloseForUser(event, userDetail, null, null);
    }

    @Override
    public void executeCloseForUser(OperacionEvent event, UserDetailDto userDetail, UserCopyAllocationEntity allocationOverride, String strategyCodeOverride) {
        Objects.requireNonNull(event, ERR_EVENT_NULL);
        Objects.requireNonNull(userDetail, ERR_USER_NULL);

        final long startedNs = System.nanoTime();
        final OperacionDto originOperation = requireOperacion(event);
        final String originId = originOperation.getIdOperacion().toString();
        final UUID userId = userDetail.getUser().getId();
        final String walletId = originOperation.getIdCuenta();
        final String symbol = safeLog(originOperation.getParSymbol());

        log.info("event=copy.close.fast_path.start originId={} userId={} wallet={} symbol={}", originId, userId, safeLog(walletId), symbol);

        if (originOperation.isOperacionActiva()) {
            log.warn("event=copy.close.fast_path.skip reasonCode=origin_still_active reason=origin_still_active originId={} userId={} wallet={} symbol={} elapsedMs={}",
                    originId, userId, safeLog(walletId), symbol, elapsedMsSince(startedNs));
            return;
        }

        executeDirectCloseForUser(event, userDetail, startedNs, firstNonBlank(strategyCodeOverride, allocationOverride == null ? null : allocationOverride.getCopyStrategyCode()));
    }

    private void executeDirectOpenForUser(OperacionEvent event, UserDetailDto userDetail, UserCopyAllocationEntity allocationOverride) {
        final OperacionDto originOperation = requireOperacion(event);
        final String originId = originOperation.getIdOperacion().toString();
        final String userId = userDetail.getUser().getId().toString();
        final String walletId = originOperation.getIdCuenta();
        final String symbol = safeLog(originOperation.getParSymbol());
        final String strategyOverride = allocationOverride == null ? null : allocationOverride.getCopyStrategyCode();
        final String traceId = activeCopyOperationCache.traceId(originId, userId, walletId, symbol, strategyOverride);

        if (walletId == null || walletId.isBlank()) {
            throw new SkipExecutionException(
                    "wallet_missing",
                    "La operación origen no trae idCuenta/wallet",
                    LogFmt.kv("originId", originId, "userId", userId, "symbol", symbol)
            );
        }

        final String lockKey = distLockKey(userId, walletId);

        log.info(
                "event=copy_open_lock_wait traceId={} originId={} userId={} wallet={} symbol={} lockKey=\"{}\"",
                traceId,
                originId,
                userId,
                walletId,
                symbol,
                lockKey
        );

        distributedLockService.withLock(lockKey, Duration.ofSeconds(120), () -> {
            log.info(
                    "event=copy_open_lock_acquired traceId={} originId={} userId={} wallet={} symbol={} lockKey=\"{}\"",
                    traceId,
                    originId,
                    userId,
                    walletId,
                    symbol,
                    lockKey
            );

            if (activeCopyOperationCache.isKnown(originId, userId, strategyOverride)) {
                CopyOperationDto runtimeCopy = activeCopyOperationCache.activeOperation(originId, userId, strategyOverride);
                log.info(
                        "event=copy_open_skipped category=runtime_state reasonCode=copy_known_in_ram reasonAlias=copy_known_in_ram friendlyReason=copia_ya_conocida_en_ram explanation=no_se_consulta_bd_ni_se_abre_otra_orden_porque_la_ruta_caliente_ya_tiene_estado_de_esta_copia copyImpact=no_copy_order traceId={} originId={} userId={} wallet={} symbol={} active={} copyId={} copyOrderId={}",
                        traceId,
                        originId,
                        userId,
                        walletId,
                        symbol,
                        runtimeCopy != null && runtimeCopy.isActive(),
                        runtimeCopy == null ? null : runtimeCopy.getIdOperation(),
                        runtimeCopy == null ? null : runtimeCopy.getIdOrden()
                );
                return null;
            }

            final CopyOperationDto existingCopy = strategyOverride == null || strategyOverride.isBlank()
                    ? copyOperationService.findOperationForUser(originId, userId)
                    : copyOperationService.findOperationForUserAndStrategy(originId, userId, strategyOverride);
            if (existingCopy != null) {
                final String reason = existingCopy.isActive() ? "copy_already_active" : "copy_already_recorded";
                if (existingCopy.isActive()) {
                    activeCopyOperationCache.markOpen(existingCopy);
                }
                log.info(
                        "event=copy_open_skipped reasonCode={} reason={} traceId={} originId={} userId={} wallet={} symbol={} active={} copyId={} copyOrderId={}",
                        reason,
                        reason,
                        traceId,
                        originId,
                        userId,
                        walletId,
                        symbol,
                        existingCopy.isActive(),
                        existingCopy.getIdOperation(),
                        existingCopy.getIdOrden()
                );
                return null;
            }

            final long metricNs = System.nanoTime();
            final MetricaWalletDto walletMetric = resolveWalletMetric(event, originId, walletId, userDetail, allocationOverride);
            log.info("event=copy.job.phase action=OPEN phase=load_metric originId={} userId={} wallet={} symbol={} elapsedMs={}",
                    originId, userId, walletId, symbol, elapsedMsSince(metricNs));
            log.info("event=copy.job.phase action=OPEN phase=load_allocation originId={} userId={} wallet={} symbol={} elapsedMs={}",
                    originId, userId, walletId, symbol, elapsedMsSince(metricNs));
            if (walletMetric == null) {
                log.warn(
                        "event=copy_open_skipped reasonCode=metric_missing reason=metric_missing originId={} userId={} wallet={} symbol={} note=wallet_not_present_in_candidates",
                        originId,
                        userId,
                        walletId,
                        symbol
                );
                return null;
            }

            final UserCopyAllocationEntity allocation = resolveAllocationForMetric(userDetail, walletId, walletMetric, allocationOverride);
            if (allocation == null || !allocation.allowsNewEntries(OffsetDateTime.now())) {
                log.info("event=copy_open_skipped reasonCode=allocation_not_openable originId={} userId={} wallet={} symbol={} strategy={} allocationId={} status={} executionMode={}",
                        originId, userId, walletId, symbol, copyStrategyRuntimeRouter.strategyCodeOf(walletMetric),
                        allocation == null ? null : allocation.getId(), allocation == null ? null : allocation.getStatus(),
                        allocation == null ? null : allocation.getExecutionMode());
                return null;
            }

            final boolean hasScoring = walletMetric.getScoring() != null;
            final Integer decisionMetricConservative = hasScoring
                    ? walletMetric.getScoring().getDecisionMetricConservative()
                    : null;
            final Boolean passesFilter = hasScoring
                    ? walletMetric.getScoring().getPassesFilter()
                    : null;
            final Boolean preCopiable = hasScoring
                    ? walletMetric.getScoring().getPreCopiable()
                    : null;

            if (!passesWalletFilters(walletMetric)) {
                log.info(
                        "event=copy_open_skipped reasonCode=wallet_filtered reason=wallet_filtered originId={} userId={} wallet={} symbol={} hasScoring={} decisionMetricConservative={} passesFilter={} preCopiable={} capitalShare={}",
                        originId,
                        userId,
                        walletId,
                        symbol,
                        hasScoring,
                        decisionMetricConservative,
                        passesFilter,
                        preCopiable,
                        formatCapitalShare(walletMetric)
                );
                return null;
            }

            final long calcNs = System.nanoTime();
            final PreparedOpen prepared = prepareOpenOperation(event, userDetail, walletMetric, allocation);
            log.info("event=copy.job.phase action=OPEN phase=calculate_target originId={} userId={} wallet={} symbol={} elapsedMs={}",
                    originId, userId, walletId, prepared.symbol, elapsedMsSince(calcNs));
            BinanceFuturesOrderClientResponse order = null;
            try {
                final long sendNs = System.nanoTime();
                activeCopyOperationCache.markPendingOpen(originId, userId, walletId, prepared.symbol,
                        prepared.dto.getPositionSide() == null ? null : prepared.dto.getPositionSide().name(),
                        allocation == null ? strategyOverride : allocation.getCopyStrategyCode(), traceId);
                log.info("event=copy.open.order.send category=copy reasonAlias=open_order_send friendlyReason=envio_de_apertura explanation=se_envia_orden_market_para_abrir_copia copyImpact=copy_order_sent traceId={} originId={} userId={} wallet={} symbol={} strategy={} allocationId={} qty={} positionSide={} reduceOnly={} clientOrderId={}",
                        traceId,
                        originId,
                        userId,
                        walletId,
                        prepared.symbol,
                        normalizeStrategy(allocation == null ? strategyOverride : allocation.getCopyStrategyCode()),
                        allocation == null ? null : allocation.getId(),
                        prepared.dto.getQuantity(),
                        prepared.dto.getPositionSide(),
                        prepared.dto.isReduceOnly(),
                        prepared.dto.getClientOrderId());

                order = executeOrShadow(prepared.dto, prepared.entryPrice, allocation, traceId);
                if (!isValidOrderResponse(order)) {
                    final Map<String, Object> invalidOrderDetails = orderResponseDetails(
                            originId,
                            userId,
                            walletId,
                            prepared.symbol,
                            order
                    );
                    log.warn("event=copy.open.order.invalid_response originId={} userId={} wallet={} symbol={} orderId={} avgPrice={} executedQty={} cumQty={} origQty={} updateTime={}",
                            originId,
                            userId,
                            walletId,
                            prepared.symbol,
                            order == null ? null : order.getOrderId(),
                            order == null ? null : order.getAvgPrice(),
                            order == null ? null : order.getExecutedQty(),
                            order == null ? null : order.getCumQty(),
                            order == null ? null : order.getOrigQty(),
                            order == null ? null : order.getUpdateTime());
                    throw new CopyBinanceClientException(
                            "Respuesta inválida de ms-binance: orderId/campos de ejecución null",
                            invalidOrderDetails
                    );
                }

                log.info("event=copy.open.order.ok category=copy reasonAlias=open_order_filled friendlyReason=apertura_ejecutada explanation=binance_o_shadow_confirmo_la_apertura copyImpact=copy_opened traceId={} originId={} userId={} wallet={} symbol={} strategy={} allocationId={} orderId={} elapsedMs={}",
                        traceId, originId, userId, walletId, prepared.symbol,
                        normalizeStrategy(allocation == null ? strategyOverride : allocation.getCopyStrategyCode()),
                        allocation == null ? null : allocation.getId(), order.getOrderId(), elapsedMsSince(sendNs));
                recordCopyOperationEvent(
                        "OPEN",
                        "OPEN",
                        originId,
                        userId,
                        walletId,
                        prepared.symbol,
                        prepared.dto.getPositionSide(),
                        prepared.dto.getSide(),
                        prepared.dto.getClientOrderId(),
                        order,
                        parseQuantity(prepared.dto.getQuantity()),
                        ZERO,
                        safeQty(copyTradingMapper.resolveFilledQty(order)),
                        null,
                        traceId,
                        "direct_open",
                        null,
                        allocation
                );
                final long saveNs = System.nanoTime();
                createNewOperation(order, walletId, originOperation.getIdOperacion(), userId, prepared.leverage, allocation);
                log.info("event=copy.job.phase action=OPEN phase=save_copy_operation traceId={} originId={} userId={} wallet={} symbol={} elapsedMs={}",
                        traceId, originId, userId, walletId, prepared.symbol, elapsedMsSince(saveNs));
                log.info("event=copy_open_completed traceId={} originId={} userId={} wallet={} symbol={} orderId={} qty={} notional={}",
                        traceId,
                        originId,
                        userId,
                        walletId,
                        prepared.symbol,
                        order.getOrderId(),
                        safeQty(copyTradingMapper.resolveFilledQty(order)).toPlainString(),
                        prepared.notional.toPlainString());
                return null;
            } catch (EngineException | DataAccessException | IllegalStateException | IllegalArgumentException | ArithmeticException ex) {
                if (shouldLogStacktrace(ex)) {
                    log.error(
                            "event=copy_open_failed traceId={} originId={} userId={} wallet={} symbol={} errClass={} err=\"{}\"",
                            traceId,
                            originId,
                            userId,
                            walletId,
                            prepared.symbol,
                            ex.getClass().getSimpleName(),
                            safeLog(ex.getMessage()),
                            ex
                    );
                } else {
                    log.error(
                            "event=copy_open_failed traceId={} originId={} userId={} wallet={} symbol={} errClass={} err=\"{}\"",
                            traceId,
                            originId,
                            userId,
                            walletId,
                            prepared.symbol,
                            ex.getClass().getSimpleName(),
                            safeLog(ex.getMessage())
                    );
                }

                if (order != null) {
                    if (isValidOrderResponse(order)) {
                        CopyOperationDto uncertain = copyTradingMapper.buildCopyNewOperationDto(
                                order,
                                walletId,
                                originOperation.getIdOperacion(),
                                userId,
                                prepared.leverage
                        );
                        activeCopyOperationCache.markUncertain(uncertain, traceId, "persist_failed_after_order");
                    }
                    try {
                        tryPanicCloseAfterPersistFailure(originId, userId, walletId, prepared, order, userDetail, allocation);
                    } catch (EngineException | DataAccessException | IllegalStateException | IllegalArgumentException | ArithmeticException panicEx) {
                        log.error("event=copy_open_panic_close_failed traceId={} originId={} userId={} wallet={} symbol={} panicErrClass={} panicErr=\"{}\"",
                                traceId,
                                originId,
                                userId,
                                walletId,
                                prepared.symbol,
                                panicEx.getClass().getSimpleName(),
                                safeLog(panicEx.getMessage()),
                                panicEx);
                    }
                } else {
                    activeCopyOperationCache.forgetPending(originId, userId, allocation == null ? strategyOverride : allocation.getCopyStrategyCode(), traceId, "order_not_sent_or_failed_before_response");
                }
                throw ex;
            }
        });
    }

    private void executeDirectCloseForUser(OperacionEvent event, UserDetailDto userDetail, long startedNs, String strategyCodeOverride) {
        final OperacionDto originOperation = requireOperacion(event);
        final String originId = originOperation.getIdOperacion().toString();
        final String userId = userDetail.getUser().getId().toString();
        final String walletId = originOperation.getIdCuenta();
        final String traceId = activeCopyOperationCache.traceId(originId, userId, walletId, safeLog(originOperation.getParSymbol()), strategyCodeOverride);

        if (walletId == null || walletId.isBlank()) {
            throw new SkipExecutionException(
                    "wallet_missing",
                    "La operación origen no trae idCuenta/wallet",
                    LogFmt.kv("originId", originId, "userId", userId, "symbol", safeLog(originOperation.getParSymbol()))
            );
        }

        distributedLockService.withLock(distLockKey(userId, walletId), Duration.ofSeconds(120), () -> {
            final long loadCopyNs = System.nanoTime();
            final CopyOperationDto currentCopy = strategyCodeOverride == null || strategyCodeOverride.isBlank()
                    ? activeCopyOperationCache.activeOperation(originId, userId)
                    : activeCopyOperationCache.activeOperation(originId, userId, strategyCodeOverride);
            final String symbol = safeLog(originOperation.getParSymbol());
            log.info("event=copy.job.phase action=CLOSE phase=load_copy_from_ram traceId={} originId={} userId={} wallet={} symbol={} elapsedMs={}",
                    traceId, originId, userId, walletId, symbol, elapsedMsSince(loadCopyNs));
            if (currentCopy == null) {
                log.info("event=copy_close_skipped reasonCode=copy_missing reason=copy_missing traceId={} originId={} userId={} wallet={} symbol={}",
                        traceId,
                        originId,
                        userId,
                        walletId,
                        symbol);
                return null;
            }

            if (!currentCopy.isActive()) {
                log.info("event=copy_close_skipped reasonCode=copy_already_inactive reason=copy_already_inactive traceId={} originId={} userId={} wallet={} symbol={}",
                        traceId,
                        originId,
                        userId,
                        walletId,
                        symbol);
                return null;
            }

            executeCloseOperation(currentCopy, userDetail, "CLOSE", "direct_close", null, originClosePriceRef(originOperation));
            log.info("event=copy.close.fast_path.done traceId={} originId={} userId={} wallet={} symbol={} qty={} elapsedMs={}",
                    traceId,
                    originId,
                    userId,
                    walletId,
                    currentCopy.getParsymbol(),
                    safeQty(currentCopy.getSizePar()).toPlainString(),
                    elapsedMsSince(startedNs));
            return null;
        });
    }

    private OperacionDto requireOperacion(OperacionEvent event) {
        Objects.requireNonNull(event, ERR_EVENT_NULL);

        final OperacionDto operacion = event.getOperacion();
        if (operacion == null) {
            throw new SkipExecutionException(
                    "operation_event_without_operacion",
                    "El evento no trae operación origen",
                    null
            );
        }
        return operacion;
    }

    private void reconcileWalletBasketForUser(OperacionEvent event, UserDetailDto userDetail) {
        reconcileWalletBasketForUser(event, userDetail, null);
    }

    private void reconcileWalletBasketForUser(OperacionEvent event, UserDetailDto userDetail, UserCopyAllocationEntity allocationOverride) {
        final OperacionDto originOperation = requireOperacion(event);
        final String triggerOriginId = originOperation.getIdOperacion().toString();
        final String walletId = originOperation.getIdCuenta();
        final String userId = userDetail.getUser().getId().toString();
        final HyperliquidDeltaType triggerDeltaType = HyperliquidDeltaType.from(event.getDeltaType());
        final boolean flipTrigger = triggerDeltaType == HyperliquidDeltaType.FLIP;

        if (walletId == null || walletId.isBlank()) {
            throw new SkipExecutionException(
                    "wallet_missing",
                    "La operación origen no trae idCuenta/wallet",
                    LogFmt.kv("originId", triggerOriginId, "userId", userId, "symbol", safeLog(originOperation.getParSymbol()))
            );
        }

        distributedLockService.withLock(distLockKey(userId, walletId), Duration.ofSeconds(120), () -> {
            log.info(LOG_RECONCILE_START, triggerOriginId, userId, walletId, originOperation.isOperacionActiva() ? "OPEN" : "CLOSE");

            final MetricaWalletDto walletMetric = resolveWalletMetric(event, triggerOriginId, walletId, userDetail, allocationOverride);
            final boolean canOpenOrResize = passesWalletFilters(walletMetric);
            if (!canOpenOrResize) {
                log.info("event=copy_reconcile_metric_not_eligible triggerOriginId={} userId={} wallet={} metricPresent={} note=will_only_close_stale_copies",
                        triggerOriginId,
                        userId,
                        walletId,
                        walletMetric != null);
            }
            final BigDecimal walletBudget = canOpenOrResize ? resolveWalletBudget(userDetail, walletMetric) : ZERO;
            final UserCopyAllocationEntity walletAllocation = resolveAllocationForMetric(userDetail, walletId, walletMetric, allocationOverride);
            final int leverage = resolveUserLeverage(userDetail, walletAllocation);

            final List<OriginBasketPositionDto> sourceBasket = patchSourceBasket(
                    futuresPositionService.getOpenBasketByWallet(walletId),
                    originOperation,
                    triggerDeltaType
            );
            final Set<String> sourceOriginIds = sourceBasket.stream()
                    .map(OriginBasketPositionDto::getOriginId)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toSet());

            final Map<String, TargetLeg> targets = buildTargetBasket(userDetail, sourceBasket, walletBudget, walletMetric, leverage, triggerDeltaType, triggerOriginId);
            final String activeStrategyCode = walletAllocation == null ? null : walletAllocation.getCopyStrategyCode();
            final List<CopyOperationDto> runtimeActiveCopies = activeCopyOperationCache.activeOperationsByUserAndWallet(userId, walletId);
            final Map<String, CopyOperationDto> current = runtimeActiveCopies
                    .stream()
                    .filter(Objects::nonNull)
                    .filter(copy -> activeStrategyCode == null || sameStrategy(copy.getCopyStrategyCode(), activeStrategyCode))
                    .collect(Collectors.toMap(CopyOperationDto::getIdOrderOrigin, java.util.function.Function.identity(), (a, b) -> a, HashMap::new));
            final BigDecimal hardCap = walletBudget.multiply(ONE.add(walletHardcapOverPct == null ? ZERO : walletHardcapOverPct));
            final BigDecimal reserve = walletBudget.multiply(walletReservePct == null ? ZERO : walletReservePct);
            final TargetLeg flipTarget = flipTrigger ? targets.get(triggerOriginId) : null;
            final boolean flipOpenAllowed = !flipTrigger || isFlipOpenPreflightAllowed(
                    flipTarget, triggerOriginId, userId, walletId, canOpenOrResize, walletBudget, hardCap, reserve);
            if (flipTrigger && !flipOpenAllowed && !closePreviousSideWhenFlipOpenBlocked) {
                log.warn("event=rebalance.copy.flip_blocked category=rebalance reasonCode=flip_preflight_blocked reasonAlias=flip_preflight_blocked friendlyReason=flip_no_ejecutado_por_preflight explanation=no_se_cierra_el_lado_anterior_ni_se_abre_el_nuevo_porque_la_validacion_previa_fallo copyImpact=no_copy_order traceId={} triggerOriginId={} userId={} wallet={} symbol={} {}",
                        activeCopyOperationCache.traceId(triggerOriginId, userId, walletId, originOperation.getParSymbol()),
                        triggerOriginId, userId, walletId, safeLog(originOperation.getParSymbol()),
                        CopyLogAdvice.fields("flip_preflight_blocked", CopyLogAdvice.context(null, null, null, null, null, true, activeCopyOperationCache.activeSize(), "rebalance")));
                return null;
            }
            log.info("event=copy_runtime_state.hot_path_loaded category=runtime_state reasonAlias=ram_state_used friendlyReason=estado_en_ram_usado explanation=la_reconciliacion_usa_ram_para_evitar_consultar_bd_en_la_ruta_caliente copyImpact=fast_path traceId={} triggerOriginId={} userId={} wallet={} activeCopies={} flipTrigger={} flipOpenAllowed={}",
                    activeCopyOperationCache.traceId(triggerOriginId, userId, walletId, originOperation.getParSymbol()), triggerOriginId, userId, walletId, current.size(), flipTrigger, flipOpenAllowed);

            for (CopyOperationDto copy : new ArrayList<>(current.values())) {
                if (copy == null) continue;
                final String copyOriginId = copy.getIdOrderOrigin();
                if (copyOriginId == null) continue;

                if (!sourceOriginIds.contains(copyOriginId)) {
                    boolean closeFromFlip = flipTrigger && sameWalletSymbolDifferentSide(copy, originOperation);
                    executeFullClose(
                            copy,
                            userDetail,
                            closeFromFlip ? "FLIP" : "CLOSE",
                            closeFromFlip ? "flip_close_previous_side" : "rebalance_full_close",
                            closeFromFlip ? "flip_close_previous_side" : null
                    );
                    current.remove(copyOriginId);
                    continue;
                }

                final TargetLeg target = targets.get(copyOriginId);
                if (target == null) {
                    if (canOpenOrResize) {
                        log.info("event=rebalance.copy.close_no_target originId={} userId={} wallet={} symbol={} reason=target_not_copiable",
                                copyOriginId, userId, walletId, copy.getParsymbol());
                        executeFullClose(copy, userDetail);
                        current.remove(copyOriginId);
                    }
                    continue;
                }

                final BigDecimal currentQty = safeQty(copy.getSizePar());
                final BigDecimal targetQty = safeQty(target.targetQty());
                if (shouldReduce(currentQty, targetQty)) {
                    if (targetQty.compareTo(ZERO) <= 0) {
                        executeFullClose(copy, userDetail);
                        current.remove(copyOriginId);
                    } else {
                        CopyOperationDto updated = executePartialReduce(copy, target, triggerOriginId, userDetail, walletAllocation);
                        if (updated == null || !updated.isActive()) {
                            current.remove(copyOriginId);
                        } else {
                            current.put(copyOriginId, updated);
                        }
                    }
                }
            }

            BigDecimal usedMargin = sumBufferedMargin(current.values());

            for (TargetLeg target : targets.values()) {
                if (target == null || safeQty(target.targetQty()).compareTo(ZERO) <= 0) {
                    continue;
                }

                CopyOperationDto currentCopy = current.get(target.originId());
                if (currentCopy == null) {
                    currentCopy = findKnownCopyForTarget(target, userId, activeStrategyCode);
                    if (currentCopy != null && currentCopy.isActive()) {
                        current.put(target.originId(), currentCopy);
                        usedMargin = sumBufferedMargin(current.values());
                        log.info("event=rebalance.copy.existing_copy_recovered category=rebalance reasonAlias=active_copy_not_in_snapshot friendlyReason=copia_activa_recuperada explanation=la_copia_ya_existia_en_ram_y_se_uso_para_reconciliar_en_vez_de_abrir_otra copyImpact=uses_existing_copy traceId={} originId={} triggerOriginId={} userId={} wallet={} symbol={} copyId={} orderId={}",
                                activeCopyOperationCache.traceId(target.originId(), userId, walletId, target.symbol()),
                                target.originId(), triggerOriginId, userId, walletId, target.symbol(), currentCopy.getIdOperation(), currentCopy.getIdOrden());
                    } else if (activeCopyOperationCache.isKnown(target.originId(), userId, activeStrategyCode)) {
                        final boolean retryFlipOpen = flipTrigger && Objects.equals(target.originId(), triggerOriginId);
                        if (!retryFlipOpen) {
                            log.info("event=rebalance.copy.open_skip category=rebalance reasonCode=copy_state_pending_or_uncertain reasonAlias=copy_state_pending_or_uncertain friendlyReason=copia_pendiente_o_incierta explanation=no_se_abre_otra_orden_porque_la_ruta_caliente_ya_tiene_un_estado_pendiente_o_incierto copyImpact=no_copy_order traceId={} originId={} triggerOriginId={} userId={} wallet={} symbol={}",
                                    activeCopyOperationCache.traceId(target.originId(), userId, walletId, target.symbol()),
                                    target.originId(), triggerOriginId, userId, walletId, target.symbol());
                            continue;
                        }
                        log.warn("event=rebalance.copy.flip_open_retry_allowed category=rebalance reasonCode=flip_open_retry_allowed reasonAlias=flip_open_retry_allowed friendlyReason=reintento_de_apertura_flip explanation=se_permite_reintentar_la_apertura_del_FLIP_con_el_mismo_clientOrderId_para_recuperar_un_estado_pendiente_o_incierto copyImpact=copy_recovery traceId={} originId={} triggerOriginId={} userId={} wallet={} symbol={} {}",
                                activeCopyOperationCache.traceId(target.originId(), userId, walletId, target.symbol()),
                                target.originId(), triggerOriginId, userId, walletId, target.symbol(),
                                CopyLogAdvice.fields("flip_open_retry_allowed", CopyLogAdvice.context(null, null, null, null, null, true, activeCopyOperationCache.activeSize(), "rebalance")));
                    }
                }

                if (currentCopy == null || !currentCopy.isActive()) {
                    final boolean targetIsFlipOpen = flipTrigger && Objects.equals(target.originId(), triggerOriginId);
                    if (targetIsFlipOpen && !flipOpenAllowed) {
                        log.warn("event=rebalance.copy.open_skip category=rebalance reasonCode=flip_open_blocked_after_close reasonAlias=flip_open_blocked_after_close friendlyReason=apertura_flip_bloqueada explanation=el_lado_anterior_pudo_cerrarse_pero_la_apertura_nueva_no_paso_preflight copyImpact=copy_flat_or_desynced traceId={} originId={} triggerOriginId={} userId={} wallet={} symbol={} {}",
                                activeCopyOperationCache.traceId(target.originId(), userId, walletId, target.symbol()),
                                target.originId(), triggerOriginId, userId, walletId, target.symbol(),
                                CopyLogAdvice.fields("flip_open_blocked_after_close", CopyLogAdvice.context(null, null, null, null, null, true, activeCopyOperationCache.activeSize(), "rebalance")));
                        continue;
                    }
                    if (shouldSkipMissingReconcileOpen(target, triggerOriginId, userId, walletId, originOperation.isOperacionActiva(), flipTrigger)) {
                        continue;
                    }
                    final BigDecimal required = computeBufferedMargin(target.targetQty(), target.priceRef(), target.leverage());
                    if (walletBudget.compareTo(ZERO) <= 0 || usedMargin.add(required).add(reserve).compareTo(hardCap) > 0) {
                        log.warn(LOG_OPEN_SKIP_BUDGET, target.originId(), userId, walletId, target.symbol(), required.toPlainString(), walletBudget.toPlainString(), usedMargin.toPlainString());
                        continue;
                    }
                    CopyOperationDto created = currentCopy == null
                            ? executeOpenTarget(
                            target,
                            triggerOriginId,
                            userDetail,
                            walletAllocation,
                            flipTrigger && Objects.equals(target.originId(), triggerOriginId) ? "FLIP" : "OPEN",
                            flipTrigger && Objects.equals(target.originId(), triggerOriginId) ? "flip_open_new_side" : "rebalance_open",
                            flipTrigger && Objects.equals(target.originId(), triggerOriginId) ? "flip_open_new_side" : null
                    )
                            : executeReopenExistingTarget(currentCopy, target, triggerOriginId, userDetail, walletAllocation);
                    if (created == null) {
                        continue;
                    }
                    current.put(target.originId(), created);
                    usedMargin = usedMargin.add(computeCopyBufferedMargin(created));
                    continue;
                }

                final BigDecimal currentQty = safeQty(currentCopy.getSizePar());
                final BigDecimal targetQty = safeQty(target.targetQty());
                if (shouldIncrease(currentQty, targetQty)) {
                    if (shouldSkipStaleIncrease(target, triggerOriginId, userId, walletId, originOperation.isOperacionActiva())) {
                        continue;
                    }
                    final BigDecimal deltaQty = targetQty.subtract(currentQty);
                    final BigDecimal required = computeBufferedMargin(deltaQty, target.priceRef(), target.leverage());
                    if (walletBudget.compareTo(ZERO) <= 0 || usedMargin.add(required).add(reserve).compareTo(hardCap) > 0) {
                        log.warn(LOG_OPEN_SKIP_BUDGET, target.originId(), userId, walletId, target.symbol(), required.toPlainString(), walletBudget.toPlainString(), usedMargin.toPlainString());
                        continue;
                    }
                    CopyOperationDto updated = executeIncrease(currentCopy, target, triggerOriginId, userDetail, walletAllocation);
                    current.put(target.originId(), updated);
                    usedMargin = sumBufferedMargin(current.values());
                }
            }

            log.info(LOG_RECONCILE_DONE,
                    triggerOriginId,
                    userId,
                    walletId,
                    sourceBasket.size(),
                    targets.size(),
                    current.size());
            return null;
        });
    }

    private boolean isFlipOpenPreflightAllowed(TargetLeg flipTarget,
                                                String triggerOriginId,
                                                String userId,
                                                String walletId,
                                                boolean canOpenOrResize,
                                                BigDecimal walletBudget,
                                                BigDecimal hardCap,
                                                BigDecimal reserve) {
        if (flipTarget == null || safeQty(flipTarget.targetQty()).compareTo(ZERO) <= 0) {
            log.error("event=rebalance.copy.flip_preflight_failed category=rebalance reasonCode=flip_preflight_no_new_target reasonAlias=flip_preflight_no_new_target friendlyReason=flip_sin_objetivo_nuevo explanation=se_detecto_FLIP_pero_no_existe_un_objetivo_valido_para_el_lado_nuevo copyImpact=copy_desync_risk traceId={} triggerOriginId={} userId={} wallet={} {}",
                    activeCopyOperationCache.traceId(triggerOriginId, userId, walletId, null),
                    triggerOriginId, userId, walletId,
                    CopyLogAdvice.fields("flip_preflight_no_new_target", CopyLogAdvice.context(null, null, null, null, null, true, activeCopyOperationCache.activeSize(), "rebalance")));
            return false;
        }
        if (!canOpenOrResize || walletBudget == null || walletBudget.compareTo(ZERO) <= 0) {
            log.error("event=rebalance.copy.flip_preflight_failed category=rebalance reasonCode=flip_preflight_metric_blocked reasonAlias=flip_preflight_metric_blocked friendlyReason=flip_bloqueado_por_metricas explanation=no_hay_presupuesto_o_metricas_validas_para_abrir_el_lado_nuevo copyImpact=copy_flat_or_desynced traceId={} originId={} userId={} wallet={} symbol={} targetQty={} walletBudget={} {}",
                    activeCopyOperationCache.traceId(flipTarget.originId(), userId, walletId, flipTarget.symbol()),
                    flipTarget.originId(), userId, walletId, flipTarget.symbol(), safeQty(flipTarget.targetQty()).toPlainString(), walletBudget,
                    CopyLogAdvice.fields("flip_preflight_metric_blocked", CopyLogAdvice.context(null, null, null, null, null, true, activeCopyOperationCache.activeSize(), "rebalance")));
            return false;
        }
        BigDecimal required = computeBufferedMargin(flipTarget.targetQty(), flipTarget.priceRef(), flipTarget.leverage());
        if (required.compareTo(ZERO) <= 0 || required.add(reserve == null ? ZERO : reserve).compareTo(hardCap == null ? ZERO : hardCap) > 0) {
            log.error("event=rebalance.copy.flip_preflight_failed category=rebalance reasonCode=flip_preflight_budget_blocked reasonAlias=flip_preflight_budget_blocked friendlyReason=flip_bloqueado_por_presupuesto explanation=el_lado_nuevo_del_FLIP_supera_el_presupuesto_disponible_o_no_tiene_margen_calculable copyImpact=copy_flat_or_desynced traceId={} originId={} userId={} wallet={} symbol={} requiredMargin={} hardCap={} reserve={} {}",
                    activeCopyOperationCache.traceId(flipTarget.originId(), userId, walletId, flipTarget.symbol()),
                    flipTarget.originId(), userId, walletId, flipTarget.symbol(), required.toPlainString(), hardCap, reserve,
                    CopyLogAdvice.fields("flip_preflight_budget_blocked", CopyLogAdvice.context(null, null, null, null, null, true, activeCopyOperationCache.activeSize(), "rebalance")));
            return false;
        }
        return true;
    }

    private MetricaWalletDto resolveWalletMetric(String walletId, UserDetailDto userDetail) {
        return resolveWalletMetric(null, null, walletId, userDetail);
    }

    private MetricaWalletDto resolveWalletMetric(String originId, String walletId, UserDetailDto userDetail) {
        return resolveWalletMetric(null, originId, walletId, userDetail);
    }

    private MetricaWalletDto resolveWalletMetric(OperacionEvent event, String originId, String walletId, UserDetailDto userDetail) {
        return resolveWalletMetric(event, originId, walletId, userDetail, null);
    }

    private MetricaWalletDto resolveWalletMetric(OperacionEvent event, String originId, String walletId, UserDetailDto userDetail, UserCopyAllocationEntity allocationOverride) {
        final UUID userId = userDetail.getUser().getId();
        final List<MetricaWalletDto> metrics = normalizeCapitalShares(metricWalletService.getCandidatesUser(userId));
        final String walletKey = normalizeSymbolKey(walletId);

        final long candidates = metrics.size();
        final long candidatesWithWallet = metrics.stream()
                .filter(Objects::nonNull)
                .filter(m -> m.getWallet() != null && m.getWallet().getIdWallet() != null)
                .count();
        final long candidatesWithScoring = metrics.stream()
                .filter(Objects::nonNull)
                .filter(m -> m.getScoring() != null)
                .count();

        final CopyJobAction action = event == null || event.getTipo() == null
                ? CopyJobAction.OPEN
                : (event.getTipo() == OperacionEvent.Tipo.CERRADA ? CopyJobAction.CLOSE : CopyJobAction.OPEN);
        final HyperliquidDeltaType deltaType = event == null ? HyperliquidDeltaType.UNKNOWN : HyperliquidDeltaType.from(event.getDeltaType());
        final String side = event == null || event.getOperacion() == null || event.getOperacion().getTipoOperacion() == null
                ? null
                : event.getOperacion().getTipoOperacion().name();

        final List<MetricaWalletDto> matches = metrics.stream()
                .filter(Objects::nonNull)
                .filter(m -> m.getWallet() != null && m.getWallet().getIdWallet() != null)
                .filter(m -> Objects.equals(normalizeSymbolKey(m.getWallet().getIdWallet()), walletKey))
                .sorted(copyStrategyRuntimeRouter.metricPreferenceComparator(action, deltaType, side))
                .toList();

        final long matchedWithScoring = matches.stream()
                .filter(m -> m.getScoring() != null)
                .count();
        final long matchedWithoutScoring = matches.size() - matchedWithScoring;

        log.info(
                "event=copy_open_metric_lookup originId={} userId={} wallet={} walletKey={} candidates={} candidatesWithWallet={} candidatesWithScoring={} matched={} matchedWithScoring={} matchedWithoutScoring={} sampleWallets=\"{}\"",
                originId,
                userId,
                walletId,
                walletKey,
                candidates,
                candidatesWithWallet,
                candidatesWithScoring,
                matches.size(),
                matchedWithScoring,
                matchedWithoutScoring,
                sampleWalletIds(metrics, 8)
        );

        if (matches.isEmpty()) {
            log.warn(
                    "event=copy_open_metric_lookup_miss originId={} userId={} wallet={} walletKey={} candidates={} candidatesWithScoring={} sampleWallets=\"{}\"",
                    originId,
                    userId,
                    walletId,
                    walletKey,
                    candidates,
                    candidatesWithScoring,
                    sampleWalletIds(metrics, 8)
            );
            return null;
        }

        if (matches.size() > 1) {
            log.warn(
                    "event=copy_open_metric_lookup_duplicate originId={} userId={} wallet={} walletKey={} matched={} matchedWallets=\"{}\"",
                    originId,
                    userId,
                    walletId,
                    walletKey,
                    matches.size(),
                    sampleWalletIds(matches, 8)
            );
        }

        final List<MetricaWalletDto> strategyOverrideMatches = allocationOverride == null || allocationOverride.getCopyStrategyCode() == null
                ? List.of()
                : matches.stream()
                .filter(m -> Objects.equals(
                        copyStrategyRuntimeRouter.strategyCodeOf(m),
                        copyStrategyRuntimeRouter.strategyCodeOf(allocationOverride)))
                .toList();
        final List<MetricaWalletDto> allocationBackedMatches = !strategyOverrideMatches.isEmpty()
                ? strategyOverrideMatches
                : matches.stream()
                .filter(m -> hasOpenAllocationForMetric(userId, walletId, m))
                .toList();
        final List<MetricaWalletDto> selectableMatches = allocationBackedMatches.isEmpty()
                ? matches
                : allocationBackedMatches;
        final MetricaWalletDto selected = selectableMatches.get(0);
        final boolean hasScoring = selected.getScoring() != null;

        log.info(
                "event=copy_open_metric_lookup_hit originId={} userId={} wallet={} selectedWallet={} selectedStrategy={} capitalShare={} hasScoring={} decisionMetricConservative={} passesFilter={} preCopiable={} historyDays={} countOperation={} matchedWithOpenAllocation={}",
                originId,
                userId,
                walletId,
                selected.getWallet() != null ? selected.getWallet().getIdWallet() : null,
                copyStrategyRuntimeRouter.strategySummary(selected),
                formatCapitalShare(selected),
                hasScoring,
                hasScoring ? selected.getScoring().getDecisionMetricConservative() : null,
                hasScoring ? selected.getScoring().getPassesFilter() : null,
                hasScoring ? selected.getScoring().getPreCopiable() : null,
                selected.getWallet() != null ? selected.getWallet().getHistoryDays() : null,
                selected.getWallet() != null ? selected.getWallet().getCountOperation() : null,
                allocationBackedMatches.size()
        );

        return selected;
    }

    private boolean hasOpenAllocationForMetric(UUID userId, String walletId, MetricaWalletDto metric) {
        if (userId == null || walletId == null || metric == null || metric.getStrategy() == null) {
            return false;
        }
        final String strategyCode = metric.getStrategy().getStrategyCode();
        if (strategyCode == null || strategyCode.isBlank()) {
            return false;
        }
        return userCopyAllocationService.findOpenAllocation(userId, walletId, strategyCode).isPresent();
    }

    private String sampleWalletIds(List<MetricaWalletDto> metrics, int limit) {
        if (metrics == null || metrics.isEmpty()) {
            return "";
        }

        return metrics.stream()
                .filter(Objects::nonNull)
                .map(MetricaWalletDto::getWallet)
                .filter(Objects::nonNull)
                .map(MetricaWalletDto.WalletDto::getIdWallet)
                .filter(Objects::nonNull)
                .map(this::normalizeSymbolKey)
                .distinct()
                .limit(limit)
                .collect(Collectors.joining(","));
    }

    private BigDecimal resolveWalletBudget(UserDetailDto userDetail, MetricaWalletDto walletMetric) {
        if (userDetail == null || userDetail.getDetail() == null || walletMetric == null) {
            return ZERO;
        }
        final Double share = walletMetric.getCapitalShare();
        if (share == null || !Double.isFinite(share) || share <= 0) {
            return ZERO;
        }
        return BigDecimal.valueOf(userDetail.getDetail().getCapital())
                .multiply(BigDecimal.valueOf(Math.max(0.0, Math.min(1.0, share))));
    }

    private int resolveUserLeverage(UserDetailDto userDetail) {
        return resolveUserLeverage(userDetail, null);
    }

    private int resolveUserLeverage(UserDetailDto userDetail, UserCopyAllocationEntity allocation) {
        if (allocation != null && allocation.getLeverageOverride() != null
                && allocation.getLeverageOverride().compareTo(ZERO) > 0) {
            return Math.min(USER_LEVERAGE_MAX, Math.max(USER_LEVERAGE_MIN, allocation.getLeverageOverride().setScale(0, RoundingMode.HALF_UP).intValue()));
        }
        return Math.min(USER_LEVERAGE_MAX, Math.max(USER_LEVERAGE_MIN, userDetail.getDetail().getLeverage()));
    }

    private UserCopyAllocationEntity resolveAllocationForMetric(UserDetailDto userDetail, String walletId, MetricaWalletDto metric) {
        return resolveAllocationForMetric(userDetail, walletId, metric, null);
    }

    private UserCopyAllocationEntity resolveAllocationForMetric(UserDetailDto userDetail, String walletId, MetricaWalletDto metric, UserCopyAllocationEntity allocationOverride) {
        if (allocationOverride != null) {
            return allocationOverride;
        }
        if (userDetail == null || userDetail.getUser() == null || userDetail.getUser().getId() == null || walletId == null || metric == null) {
            return null;
        }
        String strategyCode = copyStrategyRuntimeRouter.strategyCodeOf(metric);
        return userCopyAllocationService.findOpenAllocation(userDetail.getUser().getId(), walletId, strategyCode).orElse(null);
    }

    private String formatCapitalShare(MetricaWalletDto walletMetric) {
        if (walletMetric == null) {
            return "null";
        }
        final Double capitalShare = walletMetric.getCapitalShare();
        if (capitalShare == null || !Double.isFinite(capitalShare)) {
            return "null";
        }
        return BigDecimal.valueOf(capitalShare)
                .setScale(6, RoundingMode.HALF_UP)
                .toPlainString();
    }

    private List<OriginBasketPositionDto> patchSourceBasket(List<OriginBasketPositionDto> currentBasket,
                                                              OperacionDto originOperation,
                                                              HyperliquidDeltaType triggerDeltaType) {
        final Map<String, OriginBasketPositionDto> byOriginId = new HashMap<>();
        if (currentBasket != null) {
            for (OriginBasketPositionDto p : currentBasket) {
                if (p == null || p.getOriginId() == null) continue;
                byOriginId.put(p.getOriginId(), p);
            }
        }

        final String originId = originOperation.getIdOperacion().toString();
        if (originOperation.isOperacionActiva()) {
            if (triggerDeltaType == HyperliquidDeltaType.FLIP) {
                List<String> oppositeOriginIds = byOriginId.values().stream()
                        .filter(p -> sameWalletSymbolDifferentSide(p, originOperation))
                        .map(OriginBasketPositionDto::getOriginId)
                        .filter(Objects::nonNull)
                        .toList();
                for (String oppositeOriginId : oppositeOriginIds) {
                    byOriginId.remove(oppositeOriginId);
                    log.info("event=rebalance.source.flip_opposite_removed category=rebalance reasonCode=flip_replaces_previous_side reasonAlias=flip_replaces_previous_side friendlyReason=flip_cierra_lado_anterior explanation=el_FLIP_remueve_la_pierna_opuesta_del_basket_para_forzar_cierre_y_apertura_contraria copyImpact=copy_safe_path oldOriginId={} newOriginId={} wallet={} symbol={} newSide={} {}",
                            oppositeOriginId,
                            originId,
                            safeLog(originOperation.getIdCuenta()),
                            safeLog(originOperation.getParSymbol()),
                            originOperation.getTipoOperacion(),
                            CopyLogAdvice.fields("flip_replaces_previous_side", CopyLogAdvice.context(null, null, null, null, null, true, activeCopyOperationCache.activeSize(), "rebalance")));
                }
            }

            final BigDecimal inferredNotional = inferEconomicNotionalFromEvent(originOperation);
            byOriginId.put(originId, OriginBasketPositionDto.builder()
                    .originId(originId)
                    .walletId(originOperation.getIdCuenta())
                    .symbol(originOperation.getParSymbol())
                    .side(originOperation.getTipoOperacion())
                    .entryPrice(originOperation.getPrecioEntrada())
                    .markPrice(originOperation.getPrecioMercado())
                    .marginUsedUsd(originOperation.getMarginUsedUsd())
                    .notionalUsd(inferredNotional)
                    .sizeQty(originOperation.getSizeQty())
                    .sizeLegacy(originOperation.getSize())
                    .createdAt(toUtcOffsetDateTime(originOperation.getFechaCreacion()))
                    .updatedAt(toUtcOffsetDateTime(originOperation.getFechaCreacion()))
                    .sourceTs(toUtcOffsetDateTime(originOperation.getFechaCreacion()))
                    .build());
            log.debug("event=rebalance.source.patch_applied originId={} deltaType={} sizeQty={} notionalUsd={}",
                    originId, triggerDeltaType, originOperation.getSizeQty(), inferredNotional);
        } else {
            byOriginId.remove(originId);
        }
        return new ArrayList<>(byOriginId.values());
    }

    private boolean sameWalletSymbolDifferentSide(OriginBasketPositionDto p, OperacionDto operation) {
        if (p == null || operation == null) {
            return false;
        }
        return CopySymbolIdentity.sameWalletAndBaseAsset(p.getWalletId(), operation.getIdCuenta(), p.getSymbol(), operation.getParSymbol())
                && p.getSide() != null
                && operation.getTipoOperacion() != null
                && p.getSide() != operation.getTipoOperacion();
    }

    private boolean sameWalletSymbolDifferentSide(CopyOperationDto copy, OperacionDto operation) {
        if (copy == null || operation == null) {
            return false;
        }
        PositionSide copySide = parsePositionSide(copy.getTypeOperation());
        return CopySymbolIdentity.sameWalletAndBaseAsset(copy.getIdWalletOrigin(), operation.getIdCuenta(), copy.getParsymbol(), operation.getParSymbol())
                && copySide != null
                && operation.getTipoOperacion() != null
                && copySide != operation.getTipoOperacion();
    }

    private Map<String, TargetLeg> buildTargetBasket(UserDetailDto userDetail,
                                                     List<OriginBasketPositionDto> sourceBasket,
                                                     BigDecimal walletBudget,
                                                     MetricaWalletDto walletMetric,
                                                     int leverage,
                                                     HyperliquidDeltaType triggerDeltaType,
                                                     String triggerOriginId) {
        if (sourceBasket == null || sourceBasket.isEmpty()) {
            return Map.of();
        }
        if (walletBudget == null || walletBudget.compareTo(ZERO) <= 0) {
            return Map.of();
        }

        final BigDecimal capitalReference = resolveCapitalReference(walletMetric);
        if (capitalReference.compareTo(ZERO) <= 0) {
            return Map.of();
        }

        final Map<String, BinanceFuturesSymbolInfoClientDto> symbolsBySymbol = getSymbolsBySymbol();
        final Map<String, TargetLeg> result = new HashMap<>();
        for (OriginBasketPositionDto leg : sourceBasket) {
            if (leg == null || leg.getOriginId() == null) {
                continue;
            }
            try {
                if (!copyStrategyRuntimeRouter.metricAllowsTargetLeg(walletMetric, leg.getSide(), triggerDeltaType, leg.getOriginId(), triggerOriginId)) {
                    log.info("event=rebalance.target.skip originLegId={} wallet={} symbol={} side={} reason=strategy_filter strategy={} triggerDeltaType={} triggerOriginId={}",
                            leg.getOriginId(), leg.getWalletId(), safeLog(leg.getSymbol()), leg.getSide(), copyStrategyRuntimeRouter.strategySummary(walletMetric), triggerDeltaType, triggerOriginId);
                    continue;
                }
                final SymbolContractResolution symbolContract = resolveSymbolContract(leg.getSymbol(), resolveCapitalAsset(userDetail));
                final String symbol = symbolContract.canonicalSymbol();
                final BigDecimal originPriceRef = resolvePriceRef(leg.getMarkPrice(), leg.getEntryPrice());
                final BigDecimal priceRef = symbolContract.executionPrice(originPriceRef);
                final BigDecimal entryPriceRef = symbolContract.executionPrice(leg.getEntryPrice());
                if (priceRef.compareTo(ZERO) <= 0 || leg.getSide() == null) {
                    continue;
                }
                if (symbolContract.usesDifferentContractUnit()) {
                    log.info("event=rebalance.symbol_contract.sizing originLegId={} wallet={} rawSymbol={} canonicalSymbol={} originPriceRef={} contractPriceRef={} contractMultiplier={} humanMessage=rebalance_usara_el_precio_del_contrato_binance_para_calcular_cantidad",
                            leg.getOriginId(),
                            leg.getWalletId(),
                            safeLog(leg.getSymbol()),
                            safeLog(symbol),
                            originPriceRef.toPlainString(),
                            priceRef.toPlainString(),
                            symbolContract.priceMultiplier().toPlainString());
                }

                final BigDecimal sourceMargin = resolveSourceMarginForSizing(leg, walletMetric);
                final BigDecimal fraction = copySizingCalculator.computeFraction(
                        sourceMargin,
                        capitalReference,
                        maxFractionPerTrade
                );
                final BigDecimal targetMargin = copySizingCalculator.computeTargetMargin(
                        walletBudget,
                        sourceMargin,
                        capitalReference,
                        maxFractionPerTrade
                );

                if (fraction.compareTo(ZERO) <= 0 || targetMargin.compareTo(ZERO) <= 0) {
                    log.debug("event=rebalance.target.skip originLegId={} wallet={} symbol={} reason=target_margin_zero sourceMargin={} capitalReference={} fraction={} walletBudget={}",
                            leg.getOriginId(),
                            leg.getWalletId(),
                            leg.getSymbol(),
                            sourceMargin.toPlainString(),
                            capitalReference.toPlainString(),
                            fraction.toPlainString(),
                            walletBudget.toPlainString());
                    continue;
                }

                final BinanceFuturesSymbolInfoClientDto symbolInfo = symbolsBySymbol.get(symbol);
                if (symbolInfo == null) {
                    throw new SkipExecutionException(
                            "symbol_rules_missing",
                            "No existen reglas de Binance para el símbolo (cache/endpoint sin data)",
                            LogFmt.kv(
                                    "rawSymbol", leg.getSymbol(),
                                    "symbol", symbol,
                                    "sourceMargin", sourceMargin,
                                    "capitalReference", capitalReference,
                                    "fraction", fraction,
                                    "targetMargin", targetMargin
                            )
                    );
                }

                final SymbolRules rules = extractRules(symbolInfo);
                if (rules == null || rules.effectiveMinNotional == null) {
                    throw new SkipExecutionException(
                            "symbol_rules_invalid",
                            "Reglas de Binance inválidas/incompletas (effectiveMinNotional null)",
                            LogFmt.kv(
                                    "rawSymbol", leg.getSymbol(),
                                    "symbol", symbol,
                                    "sourceMargin", sourceMargin,
                                    "capitalReference", capitalReference,
                                    "fraction", fraction,
                                    "targetMargin", targetMargin
                            )
                    );
                }

                final BigDecimal notionalMax = targetMargin.multiply(BigDecimal.valueOf(leverage));
                final BigDecimal buf = safePct(notionalBufferPct, new BigDecimal("0.30"));
                BigDecimal targetNotional = notionalMax.multiply(ONE.subtract(buf));
                BigDecimal notionalBudgetCeiling = walletBudget.multiply(BigDecimal.valueOf(leverage));
                boolean copyByMinNotional = false;

                if (targetNotional.compareTo(rules.effectiveMinNotional) < 0) {
                    final BigDecimal originalTargetNotional = targetNotional;
                    final CopyMinNotionalPolicy minNotionalPolicy =
                            copyMinNotionalPolicyResolver.resolve(userDetail, leg.getWalletId(), walletMetric);

                    validateCopyByMinNotionalPolicy(
                            minNotionalPolicy,
                            rules.effectiveMinNotional,
                            LogFmt.kv(
                                    "rawSymbol", leg.getSymbol(),
                                    "symbol", symbol,
                                    "candidateNotional", targetNotional,
                                    "minNotional", rules.effectiveMinNotional,
                                    "exchangeMinNotional", rules.exchangeMinNotional,
                                    "effectiveMinNotional", rules.effectiveMinNotional,
                                    "minNotionalFloor", configuredMinNotionalFloor == null ? ZERO : configuredMinNotionalFloor,
                                    "sourceMargin", sourceMargin,
                                    "capitalReference", capitalReference,
                                    "fraction", fraction,
                                    "targetMargin", targetMargin,
                                    "walletBudget", walletBudget
                            )
                    );

                    targetNotional = rules.effectiveMinNotional;
                    copyByMinNotional = true;

                    log.info("event=rebalance.target.copy_by_min_notional originLegId={} wallet={} symbol={} candidateNotional={} forcedNotional={} minNotional={} policyMode={} allocationId={} score={} minScore={} historyDays={} minHistoryDays={} operationsCount={} minOperations={} maxForcedNotional={} notionalBudgetCeiling={}",
                            leg.getOriginId(),
                            leg.getWalletId(),
                            symbol,
                            originalTargetNotional.toPlainString(),
                            targetNotional.toPlainString(),
                            rules.effectiveMinNotional.toPlainString(),
                            minNotionalPolicy.mode(),
                            minNotionalPolicy.allocationId(),
                            minNotionalPolicy.score(),
                            minNotionalPolicy.minScore(),
                            minNotionalPolicy.historyDays(),
                            minNotionalPolicy.minHistoryDays(),
                            minNotionalPolicy.operationsCount(),
                            minNotionalPolicy.minOperations(),
                            minNotionalPolicy.maxNotionalUsdt(),
                            notionalBudgetCeiling.toPlainString());
                }

                final String legPositionSide = leg.getSide() == null ? "NA" : leg.getSide().name();
                final String legOrderSide = switch (legPositionSide) {
                    case "LONG" -> Side.BUY.name();
                    case "SHORT" -> Side.SELL.name();
                    default -> "NA";
                };

                BigDecimal rawQty = targetNotional.divide(priceRef, DEFAULT_CALC_SCALE, RoundingMode.DOWN);

                BigDecimal targetQty = adjustQuantityToBinanceRules(symbol, rawQty, priceRef, rules, notionalBudgetCeiling);
                if (targetQty == null) {
                    targetQty = ZERO;
                }
                if (copyByMinNotional || targetQty.multiply(priceRef).compareTo(rules.effectiveMinNotional) < 0) {
                    targetQty = adjustQuantityUpToMinNotional(
                            symbol,
                            targetQty,
                            priceRef,
                            rules,
                            rules.effectiveMinNotional,
                            notionalBudgetCeiling
                    );
                }
                if (targetQty == null) {
                    targetQty = ZERO;
                }

                if (targetQty.compareTo(ZERO) <= 0) {
                    throw new SkipExecutionException(
                            "qty_adjusted_zero",
                            "Target descartado porque la cantidad quedó en 0 tras aplicar reglas Binance",
                            LogFmt.kv(
                                    "wallet", leg.getWalletId(),
                                    "rawSymbol", leg.getSymbol(),
                                    "symbol", symbol,
                                    "side", legOrderSide,
                                    "positionSide", legPositionSide,
                                    "qty", targetQty,
                                    "qtyRaw", rawQty,
                                    "qtyFinal", targetQty,
                                    "candidateNotional", targetNotional,
                                    "priceRef", priceRef,
                                    "notionalFinal", targetQty.multiply(priceRef),
                                    "notionalBudgetCeiling", notionalBudgetCeiling,
                                    "effectiveMinNotional", rules.effectiveMinNotional,
                                    "exchangeMinNotional", rules.exchangeMinNotional,
                                    "stepSize", rules.stepSize,
                                    "minQty", rules.minQty,
                                    "qtyScale", rules.qtyScale,
                                    "copyByMinNotional", copyByMinNotional
                            )
                    );
                }

                BigDecimal targetNotionalFinal = targetQty.multiply(priceRef);
                if (targetQty.compareTo(ZERO) > 0
                        && targetNotionalFinal.compareTo(rules.effectiveMinNotional) < 0) {
                    throw new SkipExecutionException(
                            "notional_too_small",
                            "Target descartado porque el notional final quedó bajo el mínimo de Binance",
                            LogFmt.kv(
                                    "rawSymbol", leg.getSymbol(),
                                    "symbol", symbol,
                                    "targetQty", targetQty,
                                    "priceRef", priceRef,
                                    "targetNotional", targetNotionalFinal,
                                    "minNotional", rules.effectiveMinNotional,
                                    "exchangeMinNotional", rules.exchangeMinNotional,
                                    "effectiveMinNotional", rules.effectiveMinNotional,
                                    "minNotionalFloor", configuredMinNotionalFloor == null ? ZERO : configuredMinNotionalFloor,
                                    "sourceMargin", sourceMargin,
                                    "capitalReference", capitalReference,
                                    "fraction", fraction,
                                    "targetMargin", targetMargin,
                                    "walletBudget", walletBudget
                            )
                    );
                }

                log.info("event=rebalance.target.prepared originLegId={} wallet={} symbol={} sourceMargin={} capitalReference={} fraction={} walletBudget={} targetMargin={} leverage={} notionalMax={} targetNotional={} qty={} finalNotional={}",
                        leg.getOriginId(),
                        leg.getWalletId(),
                        symbol,
                        sourceMargin.toPlainString(),
                        capitalReference.toPlainString(),
                        fraction.toPlainString(),
                        walletBudget.toPlainString(),
                        targetMargin.toPlainString(),
                        leverage,
                        notionalMax.toPlainString(),
                        targetNotional.toPlainString(),
                        targetQty.toPlainString(),
                        targetNotionalFinal.toPlainString());

                result.put(leg.getOriginId(), new TargetLeg(
                        leg.getOriginId(),
                        leg.getWalletId(),
                        symbol,
                        leg.getSide(),
                        leverage,
                        priceRef,
                        targetQty,
                        targetNotionalFinal,
                        entryPriceRef,
                        leg.getCreatedAt(),
                        leg.getUpdatedAt(),
                        leg.getSourceTs()
                ));
            } catch (SkipExecutionException ex) {
                log.warn("event=rebalance.target.skip originLegId={} wallet={} rawSymbol={} symbol={} side={} priceRef={} sizeQty={} notionalUsd={} reasonCode={} reason=\"{}\" details=\"{}\"",
                        leg.getOriginId(),
                        leg.getWalletId(),
                        leg.getSymbol(),
                        leg.getSymbol(),
                        leg.getSide(),
                        resolvePriceRef(leg.getMarkPrice(), leg.getEntryPrice()),
                        leg.getSizeQty(),
                        leg.getNotionalUsd(),
                        ex.getReasonCode(),
                        safeLog(ex.getReason()),
                        safeLog(ex.getDetails()));
            } catch (EngineException | IllegalStateException | IllegalArgumentException | ArithmeticException ex) {
                log.error("event=rebalance.target.error originLegId={} wallet={} rawSymbol={} side={} errClass={} err=\"{}\"",
                        leg.getOriginId(),
                        leg.getWalletId(),
                        leg.getSymbol(),
                        leg.getSide(),
                        ex.getClass().getSimpleName(),
                        safeLog(ex.getMessage()),
                        ex);
            }
        }

        return result;
    }

    private boolean shouldSkipMissingReconcileOpen(TargetLeg target,
                                                   String triggerOriginId,
                                                   String userId,
                                                   String walletId,
                                                   boolean triggerIsOpen,
                                                   boolean flipTrigger) {
        if (target == null) {
            return true;
        }

        if (flipTrigger && Objects.equals(target.originId(), triggerOriginId)) {
            log.info("event=rebalance.copy.open_preflight_ok category=rebalance reasonCode=flip_open_new_side reasonAlias=flip_open_new_side friendlyReason=apertura_flip_validada explanation=la_apertura_del_lado_nuevo_del_FLIP_no_usa_reglas_de_apertura_tardia copyImpact=copy_safe_path originId={} triggerOriginId={} userId={} wallet={} symbol={} {}",
                    target.originId(), triggerOriginId, userId, walletId, target.symbol(),
                    CopyLogAdvice.fields("flip_open_new_side", CopyLogAdvice.context(null, null, null, null, null, true, activeCopyOperationCache.activeSize(), "rebalance")));
            return false;
        }

        if (!triggerIsOpen) {
            log.info(
                    "event=rebalance.copy.open_skip category=rebalance reasonAlias=trigger_is_close friendlyReason=rebalance_por_cierre_no_abre_posiciones explanation=un_cierre_disparo_rebalance_y_no_se_abren_posiciones_faltantes reason=trigger_is_close copyImpact=no_copy_order originId={} triggerOriginId={} userId={} wallet={} symbol={}",
                    target.originId(),
                    triggerOriginId,
                    userId,
                    walletId,
                    target.symbol()
            );
            return true;
        }

        if (!Objects.equals(target.originId(), triggerOriginId)) {
            log.info(
                    "event=rebalance.copy.open_skip category=rebalance reasonAlias=non_trigger_missing_position friendlyReason=posicion_original_sin_copia_no_abierta explanation=rebalance_detecto_posicion_original_abierta_sin_copia_pero_no_la_abrio_porque_no_fue_el_trigger reason=non_trigger_missing_position copyImpact=no_copy_order originId={} triggerOriginId={} userId={} wallet={} symbol={}",
                    target.originId(),
                    triggerOriginId,
                    userId,
                    walletId,
                    target.symbol()
            );
            return true;
        }

        final OffsetDateTime sourceCreatedAt = target.sourceCreatedAt();
        if (sourceCreatedAt == null) {
            log.warn(
                    "event=rebalance.copy.open_skip category=rebalance reasonAlias=source_created_at_missing friendlyReason=posicion_original_sin_fecha_de_apertura explanation=no_se_abre_posicion_faltante_porque_no_tiene_fecha_de_apertura reason=source_created_at_missing copyImpact=no_copy_order originId={} triggerOriginId={} userId={} wallet={} symbol={}",
                    target.originId(),
                    triggerOriginId,
                    userId,
                    walletId,
                    target.symbol()
            );
            return true;
        }

        final Duration maxAge = positiveDurationOrNull(maxMissingOpenAge);
        if (maxAge != null) {
            final Duration age = Duration.between(sourceCreatedAt.toInstant(), Instant.now());
            if (age.compareTo(maxAge) > 0) {
                log.warn(
                        "event=rebalance.copy.open_skip category=rebalance reasonAlias=stale_source_position friendlyReason=posicion_original_demasiado_antigua explanation=no_se_abre_posicion_faltante_porque_la_apertura_original_es_muy_antigua reason=stale_source_position copyImpact=no_copy_order originId={} triggerOriginId={} userId={} wallet={} symbol={} sourceCreatedAt={} ageMs={} maxAgeMs={}",
                        target.originId(),
                        triggerOriginId,
                        userId,
                        walletId,
                        target.symbol(),
                        sourceCreatedAt,
                        age.toMillis(),
                        maxAge.toMillis()
                );
                return true;
            }
        }

        final BigDecimal driftLimit = safePct(maxMissingOpenPriceDriftPct, ONE);
        final BigDecimal drift = computeEntryPriceDriftPct(target);
        if (driftLimit.compareTo(ZERO) > 0 && drift.compareTo(driftLimit) > 0) {
            log.warn(
                    "event=rebalance.copy.open_skip category=rebalance reasonAlias=entry_price_drift friendlyReason=precio_ya_esta_muy_lejos explanation=no_se_abre_o_reabre_porque_el_precio_actual_esta_muy_lejos_del_precio_de_entrada reason=entry_price_drift copyImpact=no_copy_order originId={} triggerOriginId={} userId={} wallet={} symbol={} entryPrice={} priceRef={} driftPct={} maxDriftPct={}",
                    target.originId(),
                    triggerOriginId,
                    userId,
                    walletId,
                    target.symbol(),
                    toPlain(target.entryPrice()),
                    toPlain(target.priceRef()),
                    drift.toPlainString(),
                    driftLimit.toPlainString()
            );
            return true;
        }

        return false;
    }

    private boolean shouldSkipStaleIncrease(TargetLeg target,
                                            String triggerOriginId,
                                            String userId,
                                            String walletId,
                                            boolean triggerIsOpen) {
        if (target == null) {
            return true;
        }

        if (!triggerIsOpen) {
            log.info(
                    "event=rebalance.copy.increase_skip category=rebalance reasonCode=trigger_is_close reasonAlias=trigger_is_close friendlyReason=aumento_ignorado_por_cierre explanation=un_cierre_disparo_rebalance_y_no_corresponde_aumentar_posiciones copyImpact=no_copy_order originId={} triggerOriginId={} userId={} wallet={} symbol={} {}",
                    target.originId(),
                    triggerOriginId,
                    userId,
                    walletId,
                    target.symbol(),
                    CopyLogAdvice.fields("trigger_is_close", CopyLogAdvice.context(null, null, null, null, null, true, activeCopyOperationCache.activeSize(), "rebalance"))
            );
            return true;
        }

        if (!Objects.equals(target.originId(), triggerOriginId)) {
            log.info(
                    "event=rebalance.copy.increase_skip category=rebalance reasonCode=non_trigger_position reasonAlias=non_trigger_position friendlyReason=aumento_no_es_del_trigger explanation=solo_se_aumenta_la_posicion_que_disparo_el_evento_para_no_copiar_historico_fuera_de_orden copyImpact=no_copy_order originId={} triggerOriginId={} userId={} wallet={} symbol={} {}",
                    target.originId(),
                    triggerOriginId,
                    userId,
                    walletId,
                    target.symbol(),
                    CopyLogAdvice.fields("non_trigger_position", CopyLogAdvice.context(null, null, null, null, null, true, activeCopyOperationCache.activeSize(), "rebalance"))
            );
            return true;
        }

        if (!skipStaleIncrease) {
            return false;
        }

        final Duration maxAge = positiveDurationOrNull(maxIncreaseSourceAge);
        if (maxAge == null) {
            return false;
        }

        final OffsetDateTime sourceUpdatedAt = firstNonNull(target.sourceUpdatedAt(), target.sourceTs(), target.sourceCreatedAt());
        if (sourceUpdatedAt == null) {
            log.warn(
                    "event=rebalance.copy.increase_skip category=rebalance reasonCode=source_update_time_missing reasonAlias=source_update_time_missing friendlyReason=aumento_sin_fecha_fuente explanation=no_se_aumenta_porque_no_hay_fecha_confiable_para_validar_que_el_evento_es_reciente copyImpact=no_copy_order originId={} triggerOriginId={} userId={} wallet={} symbol={} {}",
                    target.originId(),
                    triggerOriginId,
                    userId,
                    walletId,
                    target.symbol(),
                    CopyLogAdvice.fields("source_update_time_missing", CopyLogAdvice.context(null, null, null, null, null, true, activeCopyOperationCache.activeSize(), "rebalance"))
            );
            return true;
        }

        final Duration age = Duration.between(sourceUpdatedAt.toInstant(), Instant.now());
        if (age.compareTo(maxAge) <= 0) {
            return false;
        }

        log.warn(
                "event=rebalance.copy.increase_skip category=rebalance reasonCode=stale_source_update reasonAlias=stale_source_update friendlyReason=aumento_fuera_de_tiempo explanation=no_se_aumenta_porque_el_estado_fuente_es_antiguo_y_podria_descuadrar_la_copia copyImpact=no_copy_order originId={} triggerOriginId={} userId={} wallet={} symbol={} sourceUpdatedAt={} ageMs={} maxAgeMs={} "+ CopyLogAdvice.fields("stale_source_update", CopyLogAdvice.context(null, null, null, null, null, true, activeCopyOperationCache.activeSize(), "rebalance")),
                target.originId(),
                triggerOriginId,
                userId,
                walletId,
                target.symbol(),
                sourceUpdatedAt,
                age.toMillis(),
                maxAge.toMillis()
        );
        return true;
    }

    private Duration positiveDurationOrNull(Duration value) {
        if (value == null || value.isZero() || value.isNegative()) {
            return null;
        }
        return value;
    }

    private OffsetDateTime firstNonNull(OffsetDateTime first, OffsetDateTime second, OffsetDateTime third) {
        if (first != null) return first;
        if (second != null) return second;
        return third;
    }

    private BigDecimal computeEntryPriceDriftPct(TargetLeg target) {
        if (target == null) {
            return ZERO;
        }
        final BigDecimal entry = target.entryPrice();
        final BigDecimal price = target.priceRef();
        if (entry == null || price == null || entry.compareTo(ZERO) <= 0 || price.compareTo(ZERO) <= 0) {
            return ZERO;
        }
        return price.subtract(entry).abs().divide(entry, DEFAULT_CALC_SCALE, RoundingMode.HALF_UP);
    }

    private OffsetDateTime toUtcOffsetDateTime(Instant instant) {
        return instant == null ? null : OffsetDateTime.ofInstant(instant, java.time.ZoneOffset.UTC);
    }

    private String toPlain(BigDecimal value) {
        return value == null ? "null" : value.toPlainString();
    }

    private BigDecimal originClosePriceRef(OperacionDto originOperation) {
        if (originOperation == null) {
            return ZERO;
        }
        return resolvePriceRef(
                originOperation.getPrecioCierre(),
                resolvePriceRef(originOperation.getPrecioMercado(), originOperation.getPrecioEntrada())
        );
    }

    private BigDecimal inferEconomicNotionalFromEvent(OperacionDto originOperation) {
        if (originOperation == null) return ZERO;

        BigDecimal notional = abs(originOperation.getNotionalUsd());
        if (notional.compareTo(ZERO) > 0) {
            return notional;
        }

        final BigDecimal price = resolvePriceRef(originOperation.getPrecioMercado(), originOperation.getPrecioEntrada());
        notional = safeQty(originOperation.getSizeQty()).multiply(price);
        if (notional.compareTo(ZERO) > 0) {
            return notional;
        }

        return ZERO;
    }

    private BigDecimal resolvePriceRef(BigDecimal markPrice, BigDecimal entryPrice) {
        if (markPrice != null && markPrice.compareTo(ZERO) > 0) return markPrice;
        if (entryPrice != null && entryPrice.compareTo(ZERO) > 0) return entryPrice;
        return ZERO;
    }

    private boolean shouldReduce(BigDecimal currentQty, BigDecimal targetQty) {
        return currentQty.compareTo(targetQty) > 0 && !isWithinTolerance(currentQty, targetQty);
    }

    private boolean shouldIncrease(BigDecimal currentQty, BigDecimal targetQty) {
        return targetQty.compareTo(currentQty) > 0 && !isWithinTolerance(currentQty, targetQty);
    }

    private boolean isWithinTolerance(BigDecimal currentQty, BigDecimal targetQty) {
        final BigDecimal max = currentQty.max(targetQty);
        if (max.compareTo(ZERO) <= 0) return true;
        final BigDecimal diff = currentQty.subtract(targetQty).abs();
        return diff.compareTo(max.multiply(REBALANCE_TOLERANCE_PCT)) <= 0;
    }

    private boolean sameStrategy(String a, String b) {
        if (a == null || a.isBlank()) {
            a = CopyStrategyRuntimeRouter.DEFAULT_STRATEGY_CODE;
        }
        if (b == null || b.isBlank()) {
            b = CopyStrategyRuntimeRouter.DEFAULT_STRATEGY_CODE;
        }
        return a.trim().replace('-', '_').equalsIgnoreCase(b.trim().replace('-', '_'));
    }

    private CopyOperationDto findKnownCopyForTarget(TargetLeg target, String userId, String strategyCode) {
        if (target == null || userId == null || userId.isBlank()) {
            return null;
        }
        return activeCopyOperationCache.activeOperation(target.originId(), userId, strategyCode);
    }

    private CopyOperationDto executeOpenTarget(TargetLeg target, String triggerOriginId, UserDetailDto userDetail, UserCopyAllocationEntity allocation, String copyIntent, String source, String reasonCode) {
        final String userId = userDetail.getUser().getId().toString();
        final String strategyCode = strategyOf(allocation);
        final String traceId = activeCopyOperationCache.traceId(target.originId(), userId, target.walletId(), target.symbol(), strategyCode);
        final String effectiveReasonCode = reasonCode == null ? "rebalance_open" : reasonCode;
        final boolean flipOpen = "FLIP".equalsIgnoreCase(copyIntent);
        if (!allocationAllowsNewExposure(allocation, traceId, target.originId(), userId, target.walletId(), target.symbol(), flipOpen ? "flip_open_allocation_not_openable" : "rebalance_open_allocation_not_openable")) {
            return null;
        }
        final OperationDto dto = buildOpenOrIncreaseOrder(target, target.targetQty(), userDetail,
                IdempotencyKeyUtil.openClientOrderId(target.originId(), userId, target.walletId(), allocation == null ? null : allocation.getCopyStrategyCode()), true);

        activeCopyOperationCache.markPendingOpen(target.originId(), userId, target.walletId(), target.symbol(),
                target.side() == null ? null : target.side().name(), strategyCode, traceId);
        log.info("event=rebalance.copy.open_send category=rebalance reasonCode={} reasonAlias={} friendlyReason={} explanation={} copyImpact=copy_order_sent traceId={} originId={} triggerOriginId={} userId={} wallet={} symbol={} qty={} positionSide={} clientOrderId={} {}",
                effectiveReasonCode,
                flipOpen ? "flip_open_new_side" : "missing_copy_open",
                flipOpen ? "flip_abre_nuevo_lado" : "apertura_de_copia_faltante",
                flipOpen ? "se_envia_la_apertura_del_lado_contrario_despues_de_cerrar_la_copia_anterior" : "se_envia_orden_para_crear_una_copia_que_no_existia_en_ram",
                traceId,
                target.originId(),
                triggerOriginId,
                userId,
                target.walletId(),
                target.symbol(),
                dto.getQuantity(),
                dto.getPositionSide(),
                dto.getClientOrderId(),
                CopyLogAdvice.fields(effectiveReasonCode, CopyLogAdvice.context(null, null, null, null, null, null, activeCopyOperationCache.activeSize(), source == null ? "rebalance" : source)));

        final BinanceFuturesOrderClientResponse response = executeOrShadow(dto, target.priceRef(), allocation, traceId);
        if (!isValidOrderResponse(response)) {
            throw new CopyBinanceClientException(
                    "Respuesta inválida de ms-binance al abrir copia faltante de rebalance",
                    orderResponseDetails(target.originId(), userId, target.walletId(), target.symbol(), response)
            );
        }
        final CopyOperationDto created = copyTradingMapper.buildCopyNewOperationDto(
                response,
                target.walletId(),
                UUID.fromString(target.originId()),
                userId,
                target.leverage()
        );
        applyCopyMetadata(created, allocation);
        recordCopyOperationEvent(
                "OPEN",
                copyIntent == null ? "OPEN" : copyIntent,
                target.originId(),
                userId,
                target.walletId(),
                target.symbol(),
                target.side(),
                dto.getSide(),
                dto.getClientOrderId(),
                response,
                parseQuantity(dto.getQuantity()),
                ZERO,
                safeQty(copyTradingMapper.resolveFilledQty(response)),
                null,
                traceId,
                source == null ? "rebalance_open" : source,
                reasonCode,
                allocation
        );
        persistActiveAfterOrder(created, activeCopyOperationCache.traceId(target.originId(), userId, target.walletId(), target.symbol(), strategyCode), "rebalance_open_persist_failed");
        log.info("event=rebalance.copy.open_ok category=rebalance reasonCode={} reasonAlias={} friendlyReason={} explanation={} copyImpact=copy_tracked traceId={} originId={} triggerOriginId={} userId={} wallet={} symbol={} orderId={} qty={} {}",
                effectiveReasonCode,
                flipOpen ? "flip_open_new_side" : "missing_copy_opened",
                flipOpen ? "flip_nuevo_lado_abierto" : "copia_faltante_abierta",
                flipOpen ? "la_nueva_direccion_del_FLIP_quedo_abierta_y_registrada" : "la_copia_faltante_quedo_abierta_y_registrada",
                traceId,
                target.originId(),
                triggerOriginId,
                userId,
                target.walletId(),
                target.symbol(),
                response.getOrderId(),
                safeQty(copyTradingMapper.resolveFilledQty(response)).toPlainString(),
                CopyLogAdvice.fields(effectiveReasonCode, CopyLogAdvice.context(null, null, 1, null, null, true, activeCopyOperationCache.activeSize(), source == null ? "rebalance" : source)));
        return created;
    }

    private CopyOperationDto executeReopenExistingTarget(CopyOperationDto existingCopy, TargetLeg target, String triggerOriginId, UserDetailDto userDetail, UserCopyAllocationEntity fallbackAllocation) {
        final String userId = userDetail.getUser().getId().toString();
        final UserCopyAllocationEntity allocation = fallbackAllocation;
        final String strategyCode = strategyOf(allocation);
        final String traceId = activeCopyOperationCache.traceId(target.originId(), userId, target.walletId(), target.symbol(), strategyCode);
        if (!allocationAllowsNewExposure(allocation, traceId, target.originId(), userId, target.walletId(), target.symbol(), "rebalance_reopen_allocation_not_openable")) {
            return null;
        }
        final OperationDto dto = buildOpenOrIncreaseOrder(target, target.targetQty(), userDetail,
                IdempotencyKeyUtil.rebalanceReopenClientOrderId(triggerOriginId, target.originId(), userId, target.walletId(), target.targetQty().stripTrailingZeros().toPlainString(), allocation == null ? null : allocation.getCopyStrategyCode()), true);

        activeCopyOperationCache.markPendingOpen(target.originId(), userId, target.walletId(), target.symbol(),
                target.side() == null ? null : target.side().name(), strategyCode, traceId);
        log.info("event=rebalance.copy.reopen_send category=rebalance reasonAlias=inactive_copy_reopen friendlyReason=copia_inactiva_reabierta explanation=ya_existia_una_copia_inactiva_y_se_reabre_actualizando_la_misma_fila copyImpact=copy_order_sent traceId={} originId={} triggerOriginId={} userId={} wallet={} symbol={} copyId={} previousOrderId={} qty={} positionSide={} clientOrderId={}",
                traceId, target.originId(), triggerOriginId, userId, target.walletId(), target.symbol(), existingCopy.getIdOperation(), existingCopy.getIdOrden(), dto.getQuantity(), dto.getPositionSide(), dto.getClientOrderId());

        final BinanceFuturesOrderClientResponse response = executeOrShadow(dto, target.priceRef(), allocation, traceId);
        if (!isValidOrderResponse(response)) {
            throw new CopyBinanceClientException(
                    "Respuesta inválida de ms-binance al reabrir copia existente",
                    orderResponseDetails(target.originId(), userId, target.walletId(), target.symbol(), response)
            );
        }

        final CopyOperationDto reopened = copyTradingMapper.buildCopyNewOperationDto(
                response,
                target.walletId(),
                UUID.fromString(target.originId()),
                userId,
                target.leverage()
        );
        reopened.setIdOperation(existingCopy.getIdOperation());
        applyCopyMetadata(reopened, allocation);
        recordCopyOperationEvent(
                "REOPEN",
                "OPEN",
                target.originId(),
                userId,
                target.walletId(),
                target.symbol(),
                target.side(),
                dto.getSide(),
                dto.getClientOrderId(),
                response,
                parseQuantity(dto.getQuantity()),
                safeQty(existingCopy.getSizePar()),
                safeQty(copyTradingMapper.resolveFilledQty(response)),
                existingCopy.getPriceEntry(),
                traceId,
                "rebalance_reopen",
                null,
                allocation
        );
        persistActiveAfterOrder(reopened, activeCopyOperationCache.traceId(target.originId(), userId, target.walletId(), target.symbol(), strategyCode), "rebalance_reopen_persist_failed");
        log.info("event=rebalance.copy.reopen_ok category=rebalance reasonAlias=inactive_copy_reopened friendlyReason=copia_existente_reabierta explanation=se_actualizo_la_copia_existente_en_vez_de_insertar_una_nueva copyImpact=copy_tracked traceId={} originId={} triggerOriginId={} userId={} wallet={} symbol={} copyId={} orderId={} qty={}",
                traceId, target.originId(), triggerOriginId, userId, target.walletId(), target.symbol(), existingCopy.getIdOperation(), response.getOrderId(), safeQty(copyTradingMapper.resolveFilledQty(response)).toPlainString());
        return reopened;
    }

    private boolean shouldSkipSmallIncrease(CopyOperationDto currentCopy, TargetLeg target, String triggerOriginId, BigDecimal deltaQty, UserDetailDto userDetail) {
        if (!skipSmallRebalanceIncrease || target == null || deltaQty == null || deltaQty.compareTo(ZERO) <= 0) {
            return false;
        }

        final BigDecimal priceRef = target.priceRef();
        if (priceRef == null || priceRef.compareTo(ZERO) <= 0) {
            return false;
        }

        final String userId = userDetail.getUser().getId().toString();
        final String traceId = activeCopyOperationCache.traceId(target.originId(), userId, target.walletId(), target.symbol());
        final BinanceFuturesSymbolInfoClientDto symbolInfo = getSymbolsBySymbol().get(target.symbol());
        final SymbolRules rules = extractRules(symbolInfo);
        if (rules == null || rules.effectiveMinNotional == null || rules.effectiveMinNotional.compareTo(ZERO) <= 0) {
            log.warn("event=rebalance.copy.increase_guard_unavailable category=rebalance reasonAlias=symbol_rules_missing friendlyReason=no_se_pudo_validar_minimo_binance explanation=no_hay_reglas_de_simbolo_para_validar_si_el_aumento_supera_el_minimo_de_binance copyImpact=order_may_be_sent traceId={} originId={} triggerOriginId={} userId={} wallet={} symbol={}",
                    traceId, target.originId(), triggerOriginId, userId, target.walletId(), target.symbol());
            return false;
        }

        final BigDecimal deltaNotional = deltaQty.multiply(priceRef);
        if (deltaNotional.compareTo(rules.effectiveMinNotional) >= 0) {
            return false;
        }

        log.info("event=rebalance.copy.increase_skip category=rebalance reasonCode=delta_below_min_notional reasonAlias=delta_below_min_notional friendlyReason=aumento_menor_al_minimo_binance explanation=no_se_envio_el_aumento_porque_binance_rechazaria_una_orden_menor_al_minimo copyImpact=no_copy_order traceId={} originId={} triggerOriginId={} userId={} wallet={} symbol={} currentQty={} targetQty={} deltaQty={} priceRef={} deltaNotional={} minNotional={}",
                traceId,
                target.originId(),
                triggerOriginId,
                userId,
                target.walletId(),
                target.symbol(),
                toPlain(currentCopy == null ? null : currentCopy.getSizePar()),
                toPlain(target.targetQty()),
                deltaQty.toPlainString(),
                priceRef.toPlainString(),
                deltaNotional.toPlainString(),
                rules.effectiveMinNotional.toPlainString());
        return true;
    }

    private CopyOperationDto executeIncrease(CopyOperationDto currentCopy, TargetLeg target, String triggerOriginId, UserDetailDto userDetail, UserCopyAllocationEntity fallbackAllocation) {
        final BigDecimal currentQty = safeQty(currentCopy.getSizePar());
        final UserCopyAllocationEntity allocation = fallbackAllocation;
        final String strategyCode = strategyOf(allocation);
        final String traceId = activeCopyOperationCache.traceId(target.originId(), userDetail.getUser().getId().toString(), target.walletId(), target.symbol(), strategyCode);
        if (!allocationAllowsNewExposure(allocation, traceId, target.originId(), userDetail.getUser().getId().toString(), target.walletId(), target.symbol(), "rebalance_increase_allocation_not_openable")) {
            return currentCopy;
        }
        final BigDecimal deltaQty = target.targetQty().subtract(currentQty);
        if (shouldSkipSmallIncrease(currentCopy, target, triggerOriginId, deltaQty, userDetail)) {
            return currentCopy;
        }
        final OperationDto dto = buildOpenOrIncreaseOrder(
                target,
                deltaQty,
                userDetail,
                IdempotencyKeyUtil.rebalanceIncreaseClientOrderId(triggerOriginId, target.originId(), userDetail.getUser().getId().toString(), target.walletId(), target.targetQty().stripTrailingZeros().toPlainString(), allocation == null ? null : allocation.getCopyStrategyCode()),
                false
        );
        log.info("event=rebalance.copy.increase_send category=rebalance reasonAlias=resize_increase friendlyReason=aumento_de_copia explanation=se_envia_orden_para_aumentar_una_copia_existente copyImpact=copy_order_sent traceId={} originId={} triggerOriginId={} userId={} wallet={} symbol={} currentQty={} targetQty={} deltaQty={} positionSide={} clientOrderId={}",
                traceId, target.originId(), triggerOriginId, userDetail.getUser().getId(), target.walletId(), target.symbol(),
                currentQty.toPlainString(), target.targetQty().toPlainString(), deltaQty.toPlainString(), dto.getPositionSide(), dto.getClientOrderId());
        final BinanceFuturesOrderClientResponse response = executeOrShadow(dto, target.priceRef(), allocation, traceId);
        if (!isValidOrderResponse(response)) {
            throw new CopyBinanceClientException(
                    "Respuesta inválida de ms-binance al aumentar copia existente",
                    orderResponseDetails(target.originId(), userDetail.getUser().getId().toString(), target.walletId(), target.symbol(), response)
            );
        }
        final BigDecimal filledQty = safeQty(copyTradingMapper.resolveFilledQty(response));
        if (filledQty.compareTo(ZERO) <= 0) {
            return currentCopy;
        }
        final BigDecimal fillPrice = resolvePriceRef(response.getAvgPrice(), target.priceRef());
        final BigDecimal oldUsd = safeUsd(currentCopy.getSiseUsd(), currentQty, currentCopy.getPriceEntry());
        final BigDecimal addUsd = filledQty.multiply(fillPrice);
        final BigDecimal newQty = currentQty.add(filledQty);
        final BigDecimal newUsd = oldUsd.add(addUsd);
        final BigDecimal newEntry = newQty.compareTo(ZERO) <= 0 ? currentCopy.getPriceEntry() : newUsd.divide(newQty, DEFAULT_CALC_SCALE, RoundingMode.HALF_UP);

        recordCopyOperationEvent(
                "INCREASE",
                "ADJUST",
                target.originId(),
                userDetail.getUser().getId().toString(),
                target.walletId(),
                target.symbol(),
                target.side(),
                dto.getSide(),
                dto.getClientOrderId(),
                response,
                deltaQty,
                currentQty,
                newQty,
                currentCopy.getPriceEntry(),
                traceId,
                "rebalance_increase",
                null,
                allocation
        );

        final CopyOperationDto updated = CopyOperationDto.builder()
                .idOperation(currentCopy.getIdOperation())
                .idOrden(currentCopy.getIdOrden())
                .idUser(currentCopy.getIdUser())
                .idOrderOrigin(currentCopy.getIdOrderOrigin())
                .idWalletOrigin(currentCopy.getIdWalletOrigin())
                .parsymbol(target.symbol())
                .typeOperation(target.side().name())
                .leverage(BigDecimal.valueOf(target.leverage()))
                .siseUsd(newUsd)
                .sizePar(newQty)
                .priceEntry(newEntry)
                .priceClose(null)
                .dateCreation(currentCopy.getDateCreation())
                .dateClose(null)
                .active(true)
                .build();
        applyCopyMetadata(updated, allocation);
        persistActiveAfterOrder(updated, activeCopyOperationCache.traceId(updated.getIdOrderOrigin(), updated.getIdUser(), updated.getIdWalletOrigin(), updated.getParsymbol(), strategyOf(allocation)), "rebalance_update_persist_failed");
        log.info("event=rebalance.copy.increase_ok category=rebalance reasonAlias=resize_increase_applied friendlyReason=aumento_de_copia_aplicado explanation=la_copia_existente_quedo_actualizada_con_la_cantidad_ejecutada copyImpact=copy_tracked traceId={} originId={} triggerOriginId={} userId={} wallet={} symbol={} orderId={} filledQty={} newQty={} newNotional={}",
                traceId, target.originId(), triggerOriginId, userDetail.getUser().getId(), target.walletId(), target.symbol(),
                response.getOrderId(), filledQty.toPlainString(), newQty.toPlainString(), newUsd.toPlainString());
        return updated;
    }

    private CopyOperationDto executePartialReduce(CopyOperationDto currentCopy, TargetLeg target, String triggerOriginId, UserDetailDto userDetail, UserCopyAllocationEntity fallbackAllocation) {
        final BigDecimal currentQty = safeQty(currentCopy.getSizePar());
        final BigDecimal targetQty = target.targetQty();
        final BigDecimal deltaQty = currentQty.subtract(targetQty);
        final UserCopyAllocationEntity allocation = allocationMetadataFromCopyOrFallback(currentCopy, fallbackAllocation);

        final OperationDto dto = buildReduceOrder(
                target,
                deltaQty,
                userDetail,
                IdempotencyKeyUtil.rebalanceReduceClientOrderId(
                        triggerOriginId,
                        target.originId(),
                        userDetail.getUser().getId().toString(),
                        target.walletId(),
                        targetQty.stripTrailingZeros().toPlainString(),
                        strategyOf(allocation)
                )
        );

        final String traceId = activeCopyOperationCache.traceId(target.originId(), userDetail.getUser().getId().toString(), target.walletId(), target.symbol(), strategyOf(allocation));
        log.info("event=rebalance.copy.reduce_send category=rebalance reasonAlias=resize_reduce friendlyReason=reduccion_de_copia explanation=se_envia_orden_reduceOnly_para_bajar_la_copia_al_tamano_objetivo copyImpact=copy_order_sent traceId={} originId={} triggerOriginId={} userId={} wallet={} symbol={} currentQty={} targetQty={} deltaQty={} positionSide={} clientOrderId={}",
                traceId, target.originId(), triggerOriginId, userDetail.getUser().getId(), target.walletId(), target.symbol(),
                currentQty.toPlainString(), targetQty.toPlainString(), deltaQty.toPlainString(), dto.getPositionSide(), dto.getClientOrderId());
        final BinanceFuturesOrderClientResponse response = executeOrShadow(dto, target.priceRef(), allocation, traceId);

        if (!isValidOrderResponse(response)) {
            throw new EngineException(ErrorCode.EXTERNAL_SERVICE_ERROR, "Respuesta inválida de Binance");
        }

        final BigDecimal filledQty = safeQty(copyTradingMapper.resolveFilledQty(response));
        if (filledQty.compareTo(ZERO) <= 0) {
            return currentCopy;
        }

        final BigDecimal remainingQty = currentQty.subtract(filledQty);
        recordCopyOperationEvent(
                remainingQty.compareTo(ZERO) <= 0 || targetQty.compareTo(ZERO) <= 0 ? "CLOSE" : "REDUCE",
                remainingQty.compareTo(ZERO) <= 0 || targetQty.compareTo(ZERO) <= 0 ? "CLOSE" : "ADJUST",
                target.originId(),
                userDetail.getUser().getId().toString(),
                target.walletId(),
                target.symbol(),
                target.side(),
                dto.getSide(),
                dto.getClientOrderId(),
                response,
                deltaQty,
                currentQty,
                remainingQty.max(ZERO),
                currentCopy.getPriceEntry(),
                traceId,
                "rebalance_reduce",
                null,
                allocation
        );

        final OffsetDateTime eventTime = copyTradingMapper.toUtcOffsetDateTime(response.getUpdateTime());

        if (remainingQty.compareTo(ZERO) <= 0 || targetQty.compareTo(ZERO) <= 0) {
            final CopyOperationDto closed = CopyOperationDto.builder()
                    .idOperation(currentCopy.getIdOperation())
                    .idOrden(currentCopy.getIdOrden())
                    .idUser(currentCopy.getIdUser())
                    .idOrderOrigin(currentCopy.getIdOrderOrigin())
                    .idWalletOrigin(currentCopy.getIdWalletOrigin())
                    .parsymbol(currentCopy.getParsymbol())
                    .typeOperation(currentCopy.getTypeOperation())
                    .leverage(currentCopy.getLeverage())
                    .siseUsd(ZERO)
                    .sizePar(ZERO)
                    .priceEntry(currentCopy.getPriceEntry())
                    .priceClose(response.getAvgPrice())
                    .dateCreation(currentCopy.getDateCreation())
                    .dateClose(eventTime)
                    .active(false)
                    .build();
            applyCopyMetadata(closed, allocation);

            copyOperationService.closeOperation(closed);
            activeCopyOperationCache.markClosed(currentCopy.getIdOrderOrigin(), userDetail.getUser().getId().toString(), strategyOf(allocation));
            log.info("event=rebalance.copy.reduce_to_close_ok category=rebalance reasonAlias=resize_reduce_closed friendlyReason=reduccion_cerro_copia explanation=la_reduccion_dejo_la_copia_en_cero_y_se_marco_como_cerrada copyImpact=copy_closed traceId={} originId={} triggerOriginId={} userId={} wallet={} symbol={} orderId={} filledQty={}",
                    traceId, target.originId(), triggerOriginId, userDetail.getUser().getId(), target.walletId(), target.symbol(), response.getOrderId(), filledQty.toPlainString());
            return closed;
        }

        final BigDecimal currentUsd = safeUsd(currentCopy.getSiseUsd(), currentQty, currentCopy.getPriceEntry());
        final BigDecimal unitUsd = currentQty.compareTo(ZERO) <= 0
                ? ZERO
                : currentUsd.divide(currentQty, DEFAULT_CALC_SCALE, RoundingMode.HALF_UP);
        final BigDecimal remainingUsd = unitUsd.multiply(remainingQty);

        final CopyOperationDto updated = CopyOperationDto.builder()
                .idOperation(currentCopy.getIdOperation())
                .idOrden(currentCopy.getIdOrden())
                .idUser(currentCopy.getIdUser())
                .idOrderOrigin(currentCopy.getIdOrderOrigin())
                .idWalletOrigin(currentCopy.getIdWalletOrigin())
                .parsymbol(currentCopy.getParsymbol())
                .typeOperation(currentCopy.getTypeOperation())
                .leverage(currentCopy.getLeverage())
                .siseUsd(remainingUsd)
                .sizePar(remainingQty)
                .priceEntry(currentCopy.getPriceEntry())
                .priceClose(null)
                .dateCreation(currentCopy.getDateCreation())
                .dateClose(null)
                .active(true)
                .build();
        applyCopyMetadata(updated, allocation);

        persistActiveAfterOrder(updated, activeCopyOperationCache.traceId(updated.getIdOrderOrigin(), updated.getIdUser(), updated.getIdWalletOrigin(), updated.getParsymbol(), strategyOf(allocation)), "rebalance_update_persist_failed");
        log.info("event=rebalance.copy.reduce_ok category=rebalance reasonAlias=resize_reduce_applied friendlyReason=reduccion_de_copia_aplicada explanation=la_copia_quedo_actualizada_con_la_cantidad_restante copyImpact=copy_tracked traceId={} originId={} triggerOriginId={} userId={} wallet={} symbol={} orderId={} filledQty={} remainingQty={}",
                traceId, target.originId(), triggerOriginId, userDetail.getUser().getId(), target.walletId(), target.symbol(),
                response.getOrderId(), filledQty.toPlainString(), remainingQty.max(ZERO).toPlainString());
        return updated;
    }

    private void executeFullClose(CopyOperationDto copyOperation, UserDetailDto userDetail) {
        executeFullClose(copyOperation, userDetail, "CLOSE", "rebalance_full_close", null);
    }

    private void executeFullClose(CopyOperationDto copyOperation, UserDetailDto userDetail, String copyIntent, String source, String reasonCode) {
        executeCloseOperation(copyOperation, userDetail, copyIntent, source, reasonCode);
    }

    private OperationDto buildOpenOrIncreaseOrder(TargetLeg target, BigDecimal quantity, UserDetailDto userDetail, String clientOrderId, boolean configureAccountSettings) {
        final Side orderSide = target.side() == PositionSide.LONG ? Side.BUY : Side.SELL;
        return OperationDto.builder()
                .symbol(target.symbol())
                .side(orderSide)
                .type(OrderType.MARKET)
                .positionSide(target.side())
                .quantity(quantity.toPlainString())
                .leverage(configureAccountSettings ? target.leverage() : null)
                .reduceOnly(false)
                .configureAccountSettings(configureAccountSettings)
                .clientOrderId(clientOrderId)
                .originId(target.originId())
                .userId(userDetail.getUser().getId().toString())
                .walletId(target.walletId())
                .apiKey(userDetail.getUserApiKey().getApiKey())
                .secret(userDetail.getUserApiKey().getApiSecret())
                .build();
    }

    private OperationDto buildReduceOrder(TargetLeg target, BigDecimal quantity, UserDetailDto userDetail, String clientOrderId) {
        final Side orderSide = target.side() == PositionSide.LONG ? Side.SELL : Side.BUY;
        return OperationDto.builder()
                .symbol(target.symbol())
                .side(orderSide)
                .type(OrderType.MARKET)
                .positionSide(target.side())
                .quantity(quantity.toPlainString())
                .leverage(null)
                .reduceOnly(true)
                .configureAccountSettings(false)
                .clientOrderId(clientOrderId)
                .originId(target.originId())
                .userId(userDetail.getUser().getId().toString())
                .walletId(target.walletId())
                .apiKey(userDetail.getUserApiKey().getApiKey())
                .secret(userDetail.getUserApiKey().getApiSecret())
                .build();
    }

    private BigDecimal computeBufferedMargin(BigDecimal qty, BigDecimal price, int leverage) {
        if (qty == null || price == null || leverage <= 0) return ZERO;
        final BigDecimal notional = qty.multiply(price);
        return computeBufferedMarginForNotional(notional, leverage);
    }

    private BigDecimal computeBufferedMarginForNotional(BigDecimal notional, int leverage) {
        if (notional == null || leverage <= 0 || notional.compareTo(ZERO) <= 0) return ZERO;
        final BigDecimal margin = notional.divide(BigDecimal.valueOf(leverage), DEFAULT_CALC_SCALE, RoundingMode.HALF_UP);
        return margin.multiply(ONE.add(safePct(marginSafetyBufferPct, MAX_MARGIN_SAFETY_PCT)));
    }

    private BigDecimal sumBufferedMargin(java.util.Collection<CopyOperationDto> operations) {
        BigDecimal total = ZERO;
        if (operations == null) return total;
        for (CopyOperationDto op : operations) {
            total = total.add(computeCopyBufferedMargin(op));
        }
        return total;
    }

    private BigDecimal computeCopyBufferedMargin(CopyOperationDto op) {
        if (op == null || !op.isActive()) return ZERO;
        final BigDecimal qty = safeQty(op.getSizePar());
        if (qty.compareTo(ZERO) <= 0) return ZERO;
        final BigDecimal usd = safeUsd(op.getSiseUsd(), qty, op.getPriceEntry());
        final BigDecimal lev = op.getLeverage() == null || op.getLeverage().compareTo(ZERO) <= 0 ? ONE : op.getLeverage();
        final BigDecimal margin = usd.divide(lev, DEFAULT_CALC_SCALE, RoundingMode.HALF_UP);
        return margin.multiply(ONE.add(safePct(marginSafetyBufferPct, MAX_MARGIN_SAFETY_PCT)));
    }

    private BigDecimal safeUsd(BigDecimal explicitUsd, BigDecimal qty, BigDecimal entryPrice) {
        if (explicitUsd != null && explicitUsd.compareTo(ZERO) > 0) return explicitUsd;
        if (qty != null && entryPrice != null) return qty.multiply(entryPrice);
        return ZERO;
    }

    private BigDecimal safeQty(BigDecimal qty) {
        return qty == null || qty.compareTo(ZERO) <= 0 ? ZERO : qty;
    }

    private void recordCopyOperationEvent(String eventType,
                                          String copyIntent,
                                          String originId,
                                          String userId,
                                          String walletId,
                                          String symbol,
                                          PositionSide positionSide,
                                          Side side,
                                          String clientOrderId,
                                          BinanceFuturesOrderClientResponse order,
                                          BigDecimal requestedQty,
                                          BigDecimal previousQty,
                                          BigDecimal resultingQty,
                                          BigDecimal entryPrice,
                                          String traceId,
                                          String source,
                                          String reasonCode) {
        recordCopyOperationEvent(eventType, copyIntent, originId, userId, walletId, symbol, positionSide, side,
                clientOrderId, order, requestedQty, previousQty, resultingQty, entryPrice, traceId, source, reasonCode, null);
    }

    private void recordCopyOperationEvent(String eventType,
                                          String copyIntent,
                                          String originId,
                                          String userId,
                                          String walletId,
                                          String symbol,
                                          PositionSide positionSide,
                                          Side side,
                                          String clientOrderId,
                                          BinanceFuturesOrderClientResponse order,
                                          BigDecimal requestedQty,
                                          BigDecimal previousQty,
                                          BigDecimal resultingQty,
                                          BigDecimal entryPrice,
                                          String traceId,
                                          String source,
                                          String reasonCode,
                                          UserCopyAllocationEntity allocation) {
        final BigDecimal executedQty = order == null ? ZERO : safeQty(copyTradingMapper.resolveFilledQty(order));
        final BigDecimal price = resolvePriceRef(order == null ? null : order.getAvgPrice(), entryPrice);
        final BigDecimal notional = price.compareTo(ZERO) <= 0 ? ZERO : executedQty.multiply(price);
        final BigDecimal realizedPnl = estimateRealizedPnl(eventType, positionSide, entryPrice, price, executedQty);

        final String executionMode = executionModeOf(allocation);
        final boolean shadow = isShadowMode(allocation);

        copyOperationEventService.record(CopyOperationEventRecordCommand.builder()
                .userCopyAllocationId(allocation == null ? null : allocation.getId())
                .copyStrategyCode(allocation == null ? null : allocation.getCopyStrategyCode())
                .executionMode(executionMode)
                .shadow(shadow)
                .decision("ALLOW_" + eventType)
                .decisionReason(shadow ? "shadow_dummy_execution_real_price" : "live_execution")
                .sourceMovementKey(originId)
                .idOrderOrigin(originId)
                .idUser(userId)
                .idWalletOrigin(walletId)
                .parsymbol(symbol)
                .typeOperation(positionSide == null ? null : positionSide.name())
                .eventType(eventType)
                .copyIntent(copyIntent)
                .binanceOrderId(order == null || order.getOrderId() == null ? null : order.getOrderId().toString())
                .clientOrderId(firstNonBlank(clientOrderId, order == null ? null : order.getClientOrderId()))
                .side(side == null ? null : side.name())
                .positionSide(positionSide == null ? null : positionSide.name())
                .qtyRequested(requestedQty)
                .qtyExecuted(executedQty)
                .price(price.compareTo(ZERO) <= 0 ? null : price)
                .notionalUsd(notional)
                .previousQty(previousQty)
                .resultingQty(resultingQty)
                .realizedPnlUsd(realizedPnl)
                .traceId(firstNonBlank(traceId, MDC.get("traceId")))
                .source(source)
                .reasonCode(reasonCode)
                .eventTime(orderEventTime(order))
                .build());
    }

    private OffsetDateTime orderEventTime(BinanceFuturesOrderClientResponse order) {
        if (order == null || order.getUpdateTime() == null) {
            return OffsetDateTime.now(ZoneOffset.UTC);
        }
        return copyTradingMapper.toUtcOffsetDateTime(order.getUpdateTime());
    }

    private BigDecimal estimateRealizedPnl(String eventType, PositionSide positionSide, BigDecimal entryPrice, BigDecimal fillPrice, BigDecimal qty) {
        if (!("REDUCE".equals(eventType) || "CLOSE".equals(eventType) || "PANIC_CLOSE".equals(eventType))) {
            return null;
        }
        if (positionSide == null || entryPrice == null || fillPrice == null || qty == null || qty.compareTo(ZERO) <= 0) {
            return null;
        }
        return positionSide == PositionSide.LONG
                ? fillPrice.subtract(entryPrice).multiply(qty)
                : entryPrice.subtract(fillPrice).multiply(qty);
    }

    private BigDecimal parseQuantity(String quantity) {
        if (quantity == null || quantity.isBlank()) {
            return null;
        }
        try {
            return new BigDecimal(quantity.trim());
        } catch (NumberFormatException ex) {
            log.warn("event=copy_operation_event.quantity_parse_failed category=audit reasonAlias=quantity_not_numeric friendlyReason=no_se_pudo_leer_cantidad explanation=el_historial_se_guarda_sin_qty_requested_porque_la_cantidad_no_es_numerica rawQuantity=\"{}\"", safeLog(quantity));
            return null;
        }
    }

    private PositionSide parsePositionSide(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return PositionSide.valueOf(value.trim().toUpperCase());
        } catch (IllegalArgumentException ex) {
            log.warn("event=copy_operation_event.position_side_parse_failed category=audit reasonAlias=position_side_unknown friendlyReason=no_se_pudo_leer_lado_de_posicion explanation=el_historial_se_guarda_sin_positionSide_porque_el_valor_no_es_reconocido rawPositionSide=\"{}\"", safeLog(value));
            return null;
        }
    }

    private String firstNonBlank(String... values) {
        if (values == null) {
            return null;
        }
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return null;
    }

    private BigDecimal abs(BigDecimal v) {
        return v == null ? ZERO : v.abs();
    }

    private void tryPanicCloseAfterPersistFailure(String originId, String userId, String walletId,
                                                  PreparedOpen prepared,
                                                  BinanceFuturesOrderClientResponse openResponse,
                                                  UserDetailDto userDetail,
                                                  UserCopyAllocationEntity allocation) {
        if (prepared == null || userDetail == null) {
            return;
        }
        final String strategyCode = strategyOf(allocation);
        final String traceId = activeCopyOperationCache.traceId(originId, userId, walletId, prepared.symbol, strategyCode);

        BigDecimal qty = null;
        if (openResponse != null) {
            qty = openResponse.getExecutedQty();
            if (qty == null || qty.compareTo(ZERO) <= 0) {
                qty = openResponse.getCumQty();
            }
        }
        if (qty == null || qty.compareTo(ZERO) <= 0) {
            try {
                qty = new BigDecimal(prepared.dto.getQuantity());
            } catch (NumberFormatException ignored) {
                // Fallback below uses zero quantity when the prepared DTO quantity is not numeric.
            }
        }
        if (qty == null || qty.compareTo(ZERO) <= 0) {
            log.warn("event=panic_close.skip reasonCode=qty_zero reason=qty_zero traceId={} originId={} userId={} wallet={} symbol={}",
                    traceId, originId, userId, walletId, prepared.symbol);
            return;
        }

        final Side closeSide = prepared.dto.getSide() == Side.BUY ? Side.SELL : Side.BUY;

        final OperationDto close = OperationDto.builder()
                .symbol(prepared.dto.getSymbol())
                .side(closeSide)
                .type(OrderType.MARKET)
                .positionSide(prepared.dto.getPositionSide())
                .quantity(qty.toPlainString())
                .leverage(null)
                .reduceOnly(true)
                .configureAccountSettings(false)
                .clientOrderId(IdempotencyKeyUtil.closeClientOrderId(originId, userId, walletId, strategyCode))
                .originId(originId)
                .userId(userId)
                .walletId(walletId)
                .apiKey(userDetail.getUserApiKey().getApiKey())
                .secret(userDetail.getUserApiKey().getApiSecret())
                .build();

        final BinanceFuturesOrderClientResponse closeResp = executeOrShadow(close, openResponse == null ? null : openResponse.getAvgPrice(), allocation, traceId);
        if (!isValidOrderResponse(closeResp)) {
            throw new EngineException(ErrorCode.EXTERNAL_SERVICE_ERROR, "Panic close inválido");
        }

        recordCopyOperationEvent(
                "PANIC_CLOSE",
                "CLOSE",
                originId,
                userId,
                walletId,
                prepared.dto.getSymbol(),
                prepared.dto.getPositionSide(),
                close.getSide(),
                close.getClientOrderId(),
                closeResp,
                qty,
                qty,
                ZERO,
                openResponse == null ? null : openResponse.getAvgPrice(),
                traceId,
                "panic_close_after_persist_failure",
                "persist_failed_after_binance_order",
                allocation
        );
        activeCopyOperationCache.markClosed(originId, userId, strategyCode);
        log.warn("event=panic_close.ok traceId={} originId={} userId={} wallet={} symbol={} qty={} orderId={}",
                traceId, originId, userId, walletId, prepared.dto.getSymbol(), qty.toPlainString(), closeResp.getOrderId());
    }

    private void executeCloseOperation(CopyOperationDto copyOperation, UserDetailDto userDetail) {
        executeCloseOperation(copyOperation, userDetail, "CLOSE", "direct_close", null, null);
    }

    private void executeCloseOperation(CopyOperationDto copyOperation, UserDetailDto userDetail, String copyIntent, String source, String reasonCode) {
        executeCloseOperation(copyOperation, userDetail, copyIntent, source, reasonCode, null);
    }

    private void executeCloseOperation(CopyOperationDto copyOperation, UserDetailDto userDetail, String copyIntent, String source, String reasonCode, BigDecimal shadowPriceRef) {
        final String originId = copyOperation.getIdOrderOrigin();
        final String userId = userDetail.getUser().getId().toString();
        final String strategyCode = normalizeStrategy(copyOperation.getCopyStrategyCode());
        final String effectiveReasonCode = reasonCode == null ? "direct_close" : reasonCode;
        final boolean flipClose = "FLIP".equalsIgnoreCase(copyIntent);
        final OperationDto closeRequest = copyTradingMapper.buildClosePosition(copyOperation, userDetail);

        final long sendNs = System.nanoTime();
        log.info("event=copy.close.order.send category=copy reasonCode={} reasonAlias={} friendlyReason={} explanation={} copyImpact=copy_order_sent originId={} userId={} wallet={} symbol={} strategy={} allocationId={} qty={} positionSide={} reduceOnly={} clientOrderId={} {}",
                effectiveReasonCode,
                flipClose ? "flip_close_previous_side" : "direct_close",
                flipClose ? "flip_cierra_copia_anterior" : "cierre_de_copia",
                flipClose ? "se_envia_cierre_reduceOnly_del_lado_anterior_antes_de_abrir_el_lado_nuevo" : "se_envia_cierre_reduceOnly_para_salir_de_la_copia",
                originId,
                userId,
                copyOperation.getIdWalletOrigin(),
                closeRequest.getSymbol(),
                strategyCode,
                copyOperation.getUserCopyAllocationId(),
                closeRequest.getQuantity(),
                closeRequest.getPositionSide(),
                closeRequest.isReduceOnly(),
                closeRequest.getClientOrderId(),
                CopyLogAdvice.fields(effectiveReasonCode, CopyLogAdvice.context(null, null, null, null, null, true, activeCopyOperationCache.activeSize(), source == null ? "direct_close" : source)));
        final BinanceFuturesOrderClientResponse order = copyOperation.isShadow()
                ? buildShadowOrderResponse(closeRequest, resolvePriceRef(shadowPriceRef, copyOperation.getPriceEntry()))
                : procesBinanceService.operationPosition(closeRequest);

        if (!isValidOrderResponse(order)) {
            log.warn("event=binance.close.invalid_response originId={} userId={} wallet={} symbol={} orderId={} avgPrice={} executedQty={} cumQty={} origQty={} updateTime={}",
                    originId,
                    userId,
                    copyOperation.getIdWalletOrigin(),
                    copyOperation.getParsymbol(),
                    order == null ? null : order.getOrderId(),
                    order == null ? null : order.getAvgPrice(),
                    order == null ? null : order.getExecutedQty(),
                    order == null ? null : order.getCumQty(),
                    order == null ? null : order.getOrigQty(),
                    order == null ? null : order.getUpdateTime());
            throw new CopyBinanceClientException(
                    "Respuesta inválida de ms-binance cerrando copy_operation",
                    orderResponseDetails(originId, userId, copyOperation.getIdWalletOrigin(), copyOperation.getParsymbol(), order)
            );
        }

        log.info("event=copy.close.order.ok category=copy reasonCode={} reasonAlias={} friendlyReason={} explanation={} copyImpact=copy_closed originId={} userId={} wallet={} symbol={} strategy={} allocationId={} orderId={} elapsedMs={} {}",
                effectiveReasonCode,
                flipClose ? "flip_close_previous_side" : "direct_close",
                flipClose ? "flip_copia_anterior_cerrada" : "copia_cerrada",
                flipClose ? "la_copia_del_lado_anterior_quedo_cerrada_antes_de_abrir_el_lado_nuevo" : "la_copia_quedo_cerrada_en_Binance_y_se_actualizara_el_estado_local",
                originId,
                userId,
                copyOperation.getIdWalletOrigin(),
                closeRequest.getSymbol(),
                strategyCode,
                copyOperation.getUserCopyAllocationId(),
                order.getOrderId(),
                elapsedMsSince(sendNs),
                CopyLogAdvice.fields(effectiveReasonCode, CopyLogAdvice.context(null, null, 1, null, null, true, activeCopyOperationCache.activeSize(), source == null ? "direct_close" : source)));
        final String traceId = activeCopyOperationCache.traceId(originId, userId, copyOperation.getIdWalletOrigin(), copyOperation.getParsymbol(), strategyCode);
        recordCopyOperationEvent(
                "CLOSE",
                copyIntent == null ? "CLOSE" : copyIntent,
                originId,
                userId,
                copyOperation.getIdWalletOrigin(),
                copyOperation.getParsymbol(),
                parsePositionSide(copyOperation.getTypeOperation()),
                closeRequest.getSide(),
                closeRequest.getClientOrderId(),
                order,
                parseQuantity(closeRequest.getQuantity()),
                safeQty(copyOperation.getSizePar()),
                ZERO,
                copyOperation.getPriceEntry(),
                traceId,
                source == null ? "direct_close" : source,
                reasonCode,
                allocationMetadataFromCopy(copyOperation)
        );

        final CopyOperationDto buildCopyOperation = copyTradingMapper.buildCopyCloseOperationDto(copyOperation, order);
        applyCopyMetadata(buildCopyOperation, allocationMetadataFromCopy(copyOperation));
        final long saveNs = System.nanoTime();
        copyOperationService.closeOperation(buildCopyOperation);
        activeCopyOperationCache.markClosed(originId, userId, strategyCode);
        log.info("event=copy.job.phase action=CLOSE phase=save_copy_operation originId={} userId={} wallet={} symbol={} elapsedMs={}",
                originId, userId, copyOperation.getIdWalletOrigin(), copyOperation.getParsymbol(), elapsedMsSince(saveNs));

        log.info(LOG_CLOSE_OK,
                originId,
                userId,
                copyOperation.getIdWalletOrigin(),
                copyOperation.getParsymbol(),
                copyOperation.getSizePar(),
                order.getOrderId());
    }

    private PreparedOpen prepareOpenOperation(OperacionEvent event,
                                              UserDetailDto userDetail,
                                              MetricaWalletDto walletMetric,
                                              UserCopyAllocationEntity allocation) {

        Objects.requireNonNull(event, ERR_EVENT_NULL);
        Objects.requireNonNull(userDetail, ERR_USER_NULL);
        Objects.requireNonNull(walletMetric, "walletMetric no puede ser null");

        final OperacionDto originOperation = requireOperacion(event);
        final String originId = originOperation.getIdOperacion().toString();
        final String userId = userDetail.getUser().getId().toString();
        final String walletId = walletMetric.getWallet().getIdWallet();

        Double capitalShareRaw = walletMetric.getCapitalShare();
        if (capitalShareRaw == null || !Double.isFinite(capitalShareRaw)) {
            log.warn(LOG_PREP_INVALID_METRIC, originId, userId, walletId);
            throw new SkipExecutionException(
                    "metric_invalid",
                    "Métrica inválida: capitalShare es null o no finito",
                    com.apunto.engine.shared.util.LogFmt.kv("wallet", walletId, "capitalShare", capitalShareRaw)
            );
        }
        double capitalShare = Math.max(0.0, Math.min(1.0, capitalShareRaw));
        if (capitalShare <= 0.0) {
            log.warn(LOG_PREP_INVALID_METRIC, originId, userId, walletId);
            throw new SkipExecutionException(
                    "metric_invalid",
                    "Métrica inválida: capitalShare <= 0",
                    com.apunto.engine.shared.util.LogFmt.kv("wallet", walletId, "capitalShare", capitalShare)
            );
        }

        final BigDecimal walletMarginBudget = BigDecimal.valueOf(userDetail.getDetail().getCapital() * capitalShare);
        if (walletMarginBudget.compareTo(ZERO) <= 0) {
            log.warn(LOG_PREP_INVALID_BUDGET, originId, userId, walletId);
            throw new SkipExecutionException(
                    "budget_invalid",
                    "Presupuesto inválido: walletMarginBudget <= 0",
                    com.apunto.engine.shared.util.LogFmt.kv(
                            "wallet", walletId,
                            "capitalTotal", userDetail.getDetail().getCapital(),
                            "capitalShare", capitalShare,
                            "walletMarginBudget", walletMarginBudget
                    )
            );
        }

        final BigDecimal capitalReference = resolveCapitalReference(walletMetric);
        if (capitalReference.compareTo(ZERO) <= 0) {
            log.warn(LOG_PREP_INVALID_CAPITAL_REFERENCE, originId, userId, walletId, capitalReference.toPlainString());
            throw new SkipExecutionException(
                    "capital_reference_invalid",
                    "Capital de referencia inválido para sizing",
                    LogFmt.kv(
                            "wallet", walletId,
                            "capitalReference", capitalReference
                    )
            );
        }

        final BigDecimal sourceMargin = resolveSourceMarginForSizing(originOperation, walletMetric);
        if (sourceMargin.compareTo(ZERO) <= 0) {
            log.warn(LOG_PREP_INVALID_SOURCE_MARGIN, originId, userId, walletId, sourceMargin.toPlainString());
            throw new SkipExecutionException(
                    "source_margin_invalid",
                    "Operación inválida: no se pudo resolver margen fuente > 0",
                    LogFmt.kv(
                            "wallet", walletId,
                            "sourceMargin", sourceMargin,
                            "marginUsedUsd", originOperation.getMarginUsedUsd(),
                            "notionalUsd", originOperation.getNotionalUsd(),
                            "size", originOperation.getSize(),
                            "sizeQty", originOperation.getSizeQty()
                    )
            );
        }

        BigDecimal fractionOfBaseUsed = copySizingCalculator.computeFraction(
                sourceMargin,
                capitalReference,
                maxFractionPerTrade
        );
        if (fractionOfBaseUsed.compareTo(ZERO) <= 0) {
            log.warn(LOG_PREP_INVALID_FRACTION, originId, userId, walletId, fractionOfBaseUsed.toPlainString());
            throw new SkipExecutionException(
                    "fraction_invalid",
                    "Sizing inválido: fracción calculada <= 0",
                    LogFmt.kv(
                            "wallet", walletId,
                            "fraction", fractionOfBaseUsed,
                            "sourceMargin", sourceMargin,
                            "capitalReference", capitalReference
                    )
            );
        }

        final BigDecimal originEntryPrice = originOperation.getPrecioEntrada();
        if (originEntryPrice == null || originEntryPrice.compareTo(ZERO) <= 0) {
            log.warn(LOG_PREP_INVALID_ENTRY, originId, userId, walletId);
            throw new SkipExecutionException(
                    "entry_price_invalid",
                    "Precio de entrada inválido (<=0)",
                    com.apunto.engine.shared.util.LogFmt.kv("wallet", walletId, "entryPrice", originEntryPrice)
            );
        }

        final String rawSymbol = originOperation.getParSymbol();
        final long symbolMetadataNs = System.nanoTime();
        final SymbolContractResolution symbolContract = resolveSymbolContract(rawSymbol, resolveCapitalAsset(userDetail));
        final String symbol = symbolContract.canonicalSymbol();
        final BigDecimal entryPrice = symbolContract.executionPrice(originEntryPrice);
        if (entryPrice.compareTo(ZERO) <= 0) {
            throw new SkipExecutionException(
                    "contract_price_invalid",
                    "Precio convertido a contrato Binance inválido",
                    LogFmt.kv(
                            "wallet", walletId,
                            "rawSymbol", rawSymbol,
                            "canonicalSymbol", symbol,
                            "originEntryPrice", originEntryPrice,
                            "contractMultiplier", symbolContract.priceMultiplier(),
                            "contractEntryPrice", entryPrice
                    )
            );
        }
        final Map<String, BinanceFuturesSymbolInfoClientDto> symbolsBySymbol = getSymbolsBySymbol();
        log.info("event=copy.job.phase action=OPEN phase=load_symbol_metadata originId={} userId={} wallet={} rawSymbol={} symbol={} originEntryPrice={} contractEntryPrice={} contractMultiplier={} elapsedMs={} humanMessage=ya_se_cual_contrato_de_binance_usar_y_con_que_precio_calcular_la_cantidad",
                originId, userId, walletId, safeLog(rawSymbol), safeLog(symbol), originEntryPrice.toPlainString(), entryPrice.toPlainString(), symbolContract.priceMultiplier().toPlainString(), elapsedMsSince(symbolMetadataNs));
        if (rawSymbol == null || rawSymbol.isBlank()) {
            log.warn(LOG_PREP_INVALID_SYMBOL, originId, userId, walletId);
            throw new SkipExecutionException(
                    "symbol_blank",
                    "Símbolo vacío/null",
                    com.apunto.engine.shared.util.LogFmt.kv("wallet", walletId, "symbol", rawSymbol)
            );
        }

        final String normalizedRawSymbol = normalizeSymbolKey(rawSymbol);
        if (!symbol.equals(normalizedRawSymbol)) {
            log.info("event=symbol.resolved originId={} userId={} wallet={} rawSymbol={} canonicalSymbol={} contractMultiplier={} originBase={} canonicalBase={} humanMessage=el_simbolo_de_hyperliquid_se_mapeo_a_un_contrato_de_binance",
                    originId, userId, walletId, rawSymbol, symbol, symbolContract.priceMultiplier(), symbolContract.rawBase(), symbolContract.canonicalBase());
        }

        final int leverage = resolveUserLeverage(userDetail, allocation);

        final BigDecimal safety = safePct(marginSafetyBufferPct, MAX_MARGIN_SAFETY_PCT);
        final BigDecimal usedMargin = copyOperationService.sumBufferedMarginActive(userId, walletId, safety);
        final BigDecimal reserve = walletMarginBudget.multiply(safePct(walletReservePct, ONE));
        final BigDecimal hardCap = walletMarginBudget.multiply(ONE.add(safePct(walletHardcapOverPct, ONE)));
        final BigDecimal availableBufferedMargin = hardCap.subtract(reserve).subtract(usedMargin).max(ZERO);

        if (availableBufferedMargin.compareTo(ZERO) <= 0) {
            log.warn(LOG_PREP_SKIP_NO_ROOM,
                    originId,
                    userId,
                    walletId,
                    symbol,
                    walletMarginBudget.toPlainString(),
                    usedMargin.toPlainString(),
                    reserve.toPlainString(),
                    hardCap.toPlainString());
            throw new SkipExecutionException(
                    "wallet_budget_exhausted",
                    "No hay presupuesto disponible en la wallet para una nueva apertura",
                    LogFmt.kv(
                            "wallet", walletId,
                            "walletBudget", walletMarginBudget,
                            "usedMargin", usedMargin,
                            "reserve", reserve,
                            "hardCap", hardCap,
                            "availableBufferedMargin", availableBufferedMargin
                    )
            );
        }

        final BigDecimal requestedMarginThisTrade = walletMarginBudget.multiply(fractionOfBaseUsed);
        BigDecimal allowedMarginThisTrade = availableBufferedMargin
                .divide(ONE.add(safety), DEFAULT_CALC_SCALE, RoundingMode.HALF_UP)
                .max(ZERO);
        BigDecimal marginThisTrade = requestedMarginThisTrade.min(allowedMarginThisTrade);

        if (marginThisTrade.compareTo(ZERO) <= 0) {
            log.warn(LOG_PREP_SKIP_NO_ROOM,
                    originId,
                    userId,
                    walletId,
                    symbol,
                    walletMarginBudget.toPlainString(),
                    usedMargin.toPlainString(),
                    reserve.toPlainString(),
                    hardCap.toPlainString());
            throw new SkipExecutionException(
                    "wallet_budget_exhausted",
                    "El margen disponible luego de buffers es 0",
                    LogFmt.kv(
                            "wallet", walletId,
                            "walletBudget", walletMarginBudget,
                            "usedMargin", usedMargin,
                            "reserve", reserve,
                            "hardCap", hardCap,
                            "availableBufferedMargin", availableBufferedMargin,
                            "requestedMargin", requestedMarginThisTrade,
                            "allowedMargin", allowedMarginThisTrade
                    )
            );
        }

        if (marginThisTrade.compareTo(requestedMarginThisTrade) < 0) {
            log.info(LOG_PREP_BUDGET_CLAMPED,
                    originId,
                    userId,
                    walletId,
                    symbol,
                    requestedMarginThisTrade.toPlainString(),
                    marginThisTrade.toPlainString(),
                    usedMargin.toPlainString(),
                    walletMarginBudget.toPlainString(),
                    reserve.toPlainString(),
                    hardCap.toPlainString());
        }

        BigDecimal notionalMax = marginThisTrade.multiply(BigDecimal.valueOf(leverage));
        if (notionalMax.compareTo(ZERO) <= 0) {
            log.warn(LOG_PREP_SKIP_NOTIONAL_ZERO, originId, userId, walletId, symbol);
            throw new SkipExecutionException(
                    "notional_max_zero",
                    "Notional máximo <= 0 (no hay margen para operar)",
                    com.apunto.engine.shared.util.LogFmt.kv(
                            "wallet", walletId,
                            "symbol", symbol,
                            "marginThisTrade", marginThisTrade,
                            "leverage", leverage,
                            "notionalMax", notionalMax
                    )
            );
        }

        final BigDecimal buf = safePct(notionalBufferPct, new BigDecimal("0.30"));
        BigDecimal targetNotional = notionalMax.multiply(ONE.subtract(buf));
        final BigDecimal initialTargetNotional = targetNotional;
        boolean copyByMinNotional = false;

        final BinanceFuturesSymbolInfoClientDto symbolInfo = symbolsBySymbol.get(symbol);
        if (symbolInfo == null) {
            log.warn(LOG_PREP_SKIP_SYMBOL_RULES, originId, userId, walletId, symbol, targetNotional.toPlainString());
            throw new SkipExecutionException(
                    "symbol_rules_missing",
                    "No existen reglas de Binance para el símbolo (cache/endpoint sin data)",
                    com.apunto.engine.shared.util.LogFmt.kv(
                            "wallet", walletId,
                            "rawSymbol", rawSymbol,
                            "canonicalSymbol", symbol,
                            "targetNotional", targetNotional,
                            "notionalMax", notionalMax
                    )
            );
        }

        final SymbolRules rules = extractRules(symbolInfo);
        CopyMinNotionalPolicy minNotionalPolicy = CopyMinNotionalPolicy.skip();
        BigDecimal copyByMinOriginalTargetNotional = null;
        if (rules == null || rules.effectiveMinNotional == null) {
            log.warn(LOG_PREP_SKIP_SYMBOL_RULES, originId, userId, walletId, symbol, targetNotional.toPlainString());
            throw new SkipExecutionException(
                    "symbol_rules_invalid",
                    "Reglas de Binance inválidas/incompletas (effectiveMinNotional null)",
                    com.apunto.engine.shared.util.LogFmt.kv(
                            "wallet", walletId,
                            "symbol", symbol,
                            "targetNotional", targetNotional,
                            "exchangeMinNotional", rules == null ? null : rules.exchangeMinNotional,
                            "effectiveMinNotional", rules == null ? null : rules.effectiveMinNotional,
                            "minNotionalFloor", configuredMinNotionalFloor == null ? ZERO : configuredMinNotionalFloor
                    )
            );
        }

        if (targetNotional.compareTo(rules.effectiveMinNotional) < 0) {
            final BigDecimal originalTargetNotional = targetNotional;
            copyByMinOriginalTargetNotional = originalTargetNotional;
            minNotionalPolicy = copyMinNotionalPolicyResolver.resolve(userDetail, walletId, walletMetric);

            validateCopyByMinNotionalPolicy(
                    minNotionalPolicy,
                    rules.effectiveMinNotional,
                    LogFmt.kv(
                            "wallet", walletId,
                            "symbol", symbol,
                            "candidateNotional", targetNotional,
                            "minNotional", rules.effectiveMinNotional,
                            "exchangeMinNotional", rules.exchangeMinNotional,
                            "effectiveMinNotional", rules.effectiveMinNotional,
                            "minNotionalFloor", configuredMinNotionalFloor == null ? ZERO : configuredMinNotionalFloor,
                            "entryPrice", entryPrice,
                            "leverage", leverage,
                            "notionalMax", notionalMax,
                            "marginThisTrade", marginThisTrade,
                            "walletMarginBudget", walletMarginBudget,
                            "fraction", fractionOfBaseUsed,
                            "bufPct", buf,
                            "sourceMargin", sourceMargin,
                            "capitalReference", capitalReference,
                            "usedMargin", usedMargin,
                            "availableBufferedMargin", availableBufferedMargin
                    )
            );

            final BigDecimal minMarginRequired = computeBufferedMarginForNotional(rules.effectiveMinNotional, leverage);
            if (minMarginRequired.compareTo(availableBufferedMargin) > 0) {
                throw new SkipExecutionException(
                        "copy_by_min_budget_exceeded",
                        "El margen disponible no alcanza para copiar por el mínimo Binance",
                        LogFmt.kv(
                                "wallet", walletId,
                                "symbol", symbol,
                                "candidateNotional", originalTargetNotional,
                                "minNotional", rules.effectiveMinNotional,
                                "minMarginRequired", minMarginRequired,
                                "availableBufferedMargin", availableBufferedMargin,
                                "walletBudget", walletMarginBudget,
                                "usedMargin", usedMargin,
                                "allocationId", minNotionalPolicy.allocationId()
                        )
                );
            }

            targetNotional = rules.effectiveMinNotional;
            marginThisTrade = targetNotional.divide(BigDecimal.valueOf(leverage), DEFAULT_CALC_SCALE, RoundingMode.HALF_UP);
            notionalMax = availableBufferedMargin
                    .divide(ONE.add(safety), DEFAULT_CALC_SCALE, RoundingMode.HALF_UP)
                    .multiply(BigDecimal.valueOf(leverage));
            copyByMinNotional = true;

            log.info(LOG_PREP_COPY_BY_MIN_NOTIONAL,
                    originId,
                    userId,
                    walletId,
                    symbol,
                    originalTargetNotional.toPlainString(),
                    targetNotional.toPlainString(),
                    rules.effectiveMinNotional.toPlainString(),
                    minNotionalPolicy.mode(),
                    minNotionalPolicy.allocationId(),
                    minNotionalPolicy.score(),
                    minNotionalPolicy.minScore(),
                    minNotionalPolicy.historyDays(),
                    minNotionalPolicy.minHistoryDays(),
                    minNotionalPolicy.operationsCount(),
                    minNotionalPolicy.minOperations(),
                    minNotionalPolicy.maxNotionalUsdt(),
                    notionalMax.toPlainString());
        }

        final String sourcePositionSide = originOperation.getTipoOperacion() == null
                ? "NA"
                : originOperation.getTipoOperacion().name();
        final String orderSide = switch (sourcePositionSide) {
            case "LONG" -> Side.BUY.name();
            case "SHORT" -> Side.SELL.name();
            default -> "NA";
        };

        final BigDecimal quantityRaw = targetNotional.divide(entryPrice, DEFAULT_CALC_SCALE, RoundingMode.DOWN);
        final BigDecimal quantityAfterRules = adjustQuantityToBinanceRules(symbol, quantityRaw, entryPrice, rules, notionalMax);
        BigDecimal quantity = quantityAfterRules;
        if (copyByMinNotional || quantity.multiply(entryPrice).compareTo(rules.effectiveMinNotional) < 0) {
            quantity = adjustQuantityUpToMinNotional(
                    symbol,
                    quantity,
                    entryPrice,
                    rules,
                    rules.effectiveMinNotional,
                    notionalMax
            );
        }
        if (quantity.compareTo(ZERO) <= 0) {
            log.info(LOG_PREP_SKIP_TOO_SMALL, originId, userId, walletId, symbol, targetNotional.toPlainString());
            throw new SkipExecutionException(
                    "qty_adjusted_zero",
                    "Cantidad quedó en 0 tras aplicar reglas Binance (step/minQty/precision/budget)",
                    LogFmt.kv(
                            "wallet", walletId,
                            "symbol", symbol,
                            "side", orderSide,
                            "positionSide", sourcePositionSide,
                            "qty", quantity,
                            "qtyRaw", quantityRaw,
                            "qtyAfterRules", quantityAfterRules,
                            "qtyFinal", quantity,
                            "targetNotional", targetNotional,
                            "originalTargetNotional", initialTargetNotional,
                            "entryPrice", entryPrice,
                            "notionalFinal", quantity.multiply(entryPrice),
                            "notionalMax", notionalMax,
                            "effectiveMinNotional", rules.effectiveMinNotional,
                            "exchangeMinNotional", rules.exchangeMinNotional,
                            "walletBudget", walletMarginBudget,
                            "availableBufferedMargin", availableBufferedMargin,
                            "usedMargin", usedMargin,
                            "sourceMargin", sourceMargin,
                            "capitalReference", capitalReference,
                            "fraction", fractionOfBaseUsed,
                            "leverage", leverage,
                            "copyByMinNotional", copyByMinNotional,
                            "stepSize", rules.stepSize,
                            "minQty", rules.minQty,
                            "qtyScale", rules.qtyScale
                    )
            );
        }

        final BigDecimal notionalFinal = quantity.multiply(entryPrice);
        if (symbolContract.usesDifferentContractUnit()) {
            log.info("event=copy.symbol_contract.sizing originId={} userId={} wallet={} rawSymbol={} canonicalSymbol={} originEntryPrice={} contractEntryPrice={} contractMultiplier={} targetNotional={} qtyFinal={} finalNotional={} humanMessage=calcule_la_cantidad_usando_el_precio_del_contrato_para_no_copiar_de_mas",
                    originId,
                    userId,
                    walletId,
                    safeLog(rawSymbol),
                    safeLog(symbol),
                    originEntryPrice.toPlainString(),
                    entryPrice.toPlainString(),
                    symbolContract.priceMultiplier().toPlainString(),
                    targetNotional.toPlainString(),
                    quantity.toPlainString(),
                    notionalFinal.toPlainString());
        }
        if (copyByMinNotional && minNotionalPolicy.maxNotionalUsdt() != null
                && notionalFinal.compareTo(minNotionalPolicy.maxNotionalUsdt()) > 0) {
            throw new SkipExecutionException(
                    "copy_by_min_cap_exceeded",
                    "Notional ejecutable real supera el máximo USDT permitido para copiar por mínimo",
                    LogFmt.kv(
                            "wallet", walletId,
                            "symbol", symbol,
                            "candidateNotional", copyByMinOriginalTargetNotional,
                            "exchangeMinNotional", rules.exchangeMinNotional,
                            "effectiveMinNotional", rules.effectiveMinNotional,
                            "finalNotional", notionalFinal,
                            "maxForcedNotional", minNotionalPolicy.maxNotionalUsdt(),
                            "qtyFinal", quantity,
                            "entryPrice", entryPrice,
                            "stepSize", rules.stepSize,
                            "minQty", rules.minQty,
                            "qtyScale", rules.qtyScale,
                            "allocationId", minNotionalPolicy.allocationId()
                    )
            );
        }
        if (copyByMinNotional) {
            log.info("event=operation.open.copy_by_min_notional.final originId={} userId={} wallet={} symbol={} candidateNotional={} exchangeMinNotional={} effectiveMinNotional={} finalNotional={} qtyFinal={} entryPrice={} stepSize={} minQty={} qtyScale={} maxForcedNotional={}",
                    originId,
                    userId,
                    walletId,
                    symbol,
                    copyByMinOriginalTargetNotional,
                    rules.exchangeMinNotional,
                    rules.effectiveMinNotional,
                    notionalFinal.toPlainString(),
                    quantity.toPlainString(),
                    entryPrice.toPlainString(),
                    rules.stepSize,
                    rules.minQty,
                    rules.qtyScale,
                    minNotionalPolicy.maxNotionalUsdt());
        }
        if (notionalFinal.compareTo(rules.effectiveMinNotional) < 0) {
            log.info(LOG_PREP_SKIP_TOO_SMALL, originId, userId, walletId, symbol, notionalFinal.toPlainString());
            final BigDecimal missing = rules.effectiveMinNotional.subtract(notionalFinal);
            throw new SkipExecutionException(
                    "notional_too_small",
                    "Operación demasiado pequeña tras ajuste: faltan " + missing.max(ZERO).toPlainString()
                            + " de notional para minNotional=" + rules.effectiveMinNotional.toPlainString(),
                    com.apunto.engine.shared.util.LogFmt.kv(
                            "wallet", walletId,
                            "symbol", symbol,
                            "candidateNotional", targetNotional,
                            "notionalFinal", notionalFinal,
                            "minNotional", rules.effectiveMinNotional,
                            "exchangeMinNotional", rules.exchangeMinNotional,
                            "effectiveMinNotional", rules.effectiveMinNotional,
                            "minNotionalFloor", configuredMinNotionalFloor == null ? ZERO : configuredMinNotionalFloor,
                            "missingNotional", missing,
                            "entryPrice", entryPrice,
                            "qtyFinal", quantity,
                            "qtyScale", rules.qtyScale,
                            "stepSize", rules.stepSize,
                            "minQty", rules.minQty
                    )
            );
        }

        final BigDecimal marginRequired = notionalFinal
                .divide(BigDecimal.valueOf(leverage), DEFAULT_CALC_SCALE, RoundingMode.HALF_UP)
                .multiply(ONE.add(safety));

        if (marginRequired.compareTo(availableBufferedMargin) > 0) {
            log.warn(LOG_OPEN_SKIP_BUDGET,
                    originId,
                    userId,
                    walletId,
                    symbol,
                    marginRequired.toPlainString(),
                    walletMarginBudget.toPlainString(),
                    usedMargin.toPlainString());
            throw new SkipExecutionException(
                    "wallet_budget_exceeded",
                    "La operación excede el presupuesto restante de la wallet",
                    LogFmt.kv(
                            "wallet", walletId,
                            "symbol", symbol,
                            "marginRequired", marginRequired,
                            "availableBufferedMargin", availableBufferedMargin,
                            "walletBudget", walletMarginBudget,
                            "usedMargin", usedMargin,
                            "reserve", reserve,
                            "hardCap", hardCap
                    )
            );
        }

        final String userWalletKey = userWalletKey(userId, walletId);

        log.debug(LOG_PREP_PREPARED,
                originId,
                userId,
                walletId,
                symbol,
                sourceMargin.toPlainString(),
                capitalReference.toPlainString(),
                fractionOfBaseUsed.toPlainString(),
                walletMarginBudget.toPlainString(),
                usedMargin.toPlainString(),
                availableBufferedMargin.toPlainString(),
                marginThisTrade.toPlainString(),
                leverage,
                notionalFinal.toPlainString(),
                marginRequired.toPlainString(),
                copyByMinNotional);

        final OperationDto dto = buildBuyAndSellPosition(symbol, event, quantity, userDetail, leverage, allocation);

        return new PreparedOpen(
                dto,
                symbol,
                leverage,
                notionalFinal,
                marginRequired,
                walletMarginBudget,
                entryPrice,
                userWalletKey
        );
    }

    private OperationDto buildBuyAndSellPosition(String symbol,
                                                 OperacionEvent event,
                                                 BigDecimal quantity,
                                                 UserDetailDto userDetail,
                                                 int leverage,
                                                 UserCopyAllocationEntity allocation) {

        final PositionSide side = event.getOperacion().getTipoOperacion();

        if (side == null) {
            throw new SkipExecutionException(
                    "side_missing",
                    "tipoOperacion inválido/null",
                    com.apunto.engine.shared.util.LogFmt.kv("symbol", symbol)
            );
        }

        final Side orderSide = side.equals(PositionSide.LONG) ? Side.BUY : Side.SELL;

        return OperationDto.builder()
                .symbol(symbol)
                .side(orderSide)
                .type(OrderType.MARKET)
                .positionSide(side)
                .quantity(quantity.toPlainString())
                .leverage(leverage)
                .reduceOnly(false)
                .configureAccountSettings(true)
                .clientOrderId(IdempotencyKeyUtil.openClientOrderId(event.getOperacion().getIdOperacion().toString(), userDetail.getUser().getId().toString(), event.getOperacion().getIdCuenta(), allocation == null ? null : allocation.getCopyStrategyCode()))
                .originId(event.getOperacion().getIdOperacion().toString())
                .userId(userDetail.getUser().getId().toString())
                .walletId(event.getOperacion().getIdCuenta())
                .apiKey(userDetail.getUserApiKey().getApiKey())
                .secret(userDetail.getUserApiKey().getApiSecret())
                .build();
    }


    private MetricaWalletDto getWalletMetricForOperation(String idWalletOperation, List<MetricaWalletDto> metrics) {
        if (metrics == null || metrics.isEmpty() || idWalletOperation == null) {
            return null;
        }

        final String walletKey = normalizeSymbolKey(idWalletOperation);
        return metrics.stream()
                .filter(m -> m != null
                        && m.getWallet() != null
                        && m.getWallet().getIdWallet() != null
                        && normalizeSymbolKey(m.getWallet().getIdWallet()).equals(walletKey))
                .findFirst()
                .orElse(null);
    }

    private SymbolsCache getOrRefreshSymbolsCache() {
        final long now = System.currentTimeMillis();
        final SymbolsCache snapshot = symbolsCache;

        if (hasSymbols(snapshot) && snapshot.expiresAtMs > now) {
            log.debug("event=binance.symbols.cache.hit source=local cacheHit=true stale=false size={} ttlMs={}",
                    snapshot.bySymbol.size(), symbolsCacheTtlMs);
            return snapshot;
        }

        if (hasSymbols(snapshot)) {
            log.info("event=binance.symbols.cache.hit source=local cacheHit=true stale=true size={} ttlMs={} action=refresh_async",
                    snapshot.bySymbol.size(), symbolsCacheTtlMs);
            refreshSymbolsCacheAsync("stale_hit");
            return snapshot;
        }

        log.info("event=binance.symbols.cache.miss source=local cacheHit=false action=refresh_sync");
        synchronized (symbolsCacheLock) {
            final SymbolsCache snapshot2 = symbolsCache;
            if (hasSymbols(snapshot2)) {
                log.info("event=binance.symbols.cache.hit source=local cacheHit=true stale={} size={}",
                        snapshot2.expiresAtMs <= System.currentTimeMillis(), snapshot2.bySymbol.size());
                refreshSymbolsCacheAsync("post_lock_stale_check");
                return snapshot2;
            }
            return refreshSymbolsCacheSync("empty_cache");
        }
    }

    private boolean hasSymbols(SymbolsCache cache) {
        return cache != null && cache.bySymbol != null && !cache.bySymbol.isEmpty();
    }

    private void refreshSymbolsCacheAsync(String phase) {
        if (!symbolsRefreshInFlight.compareAndSet(false, true)) {
            return;
        }
        Thread.ofVirtual().name("copy-symbols-refresh-", 0).start(() -> {
            try {
                refreshSymbolsCacheSync(phase);
            } catch (EngineException | IllegalStateException ex) {
                log.warn("event=binance.symbols.refresh.failed phase={} errClass={} errMsg=\"{}\" cacheSize={}",
                        phase, ex.getClass().getSimpleName(), safeLog(ex.getMessage()), symbolsCache.bySymbol.size());
            } finally {
                symbolsRefreshInFlight.set(false);
            }
        });
    }

    private SymbolsCache refreshSymbolsCacheSync(String phase) {
        final long startedNs = System.nanoTime();
        try {
            final List<BinanceFuturesSymbolInfoClientDto> symbols = procesBinanceService.getSymbols(symbolsApiKey);
            final SymbolsCache loaded = buildSymbolsCache(symbols, System.currentTimeMillis() + symbolsCacheTtlMs);
            symbolsCache = loaded;
            log.info("event=binance.symbols.refresh.ok phase={} totalSymbols={} ttlMs={} elapsedMs={}",
                    phase, loaded.bySymbol.size(), symbolsCacheTtlMs, elapsedMsSince(startedNs));
            return loaded;
        } catch (EngineException | IllegalStateException ex) {
            final SymbolsCache stale = symbolsCache;
            if (hasSymbols(stale)) {
                log.warn("event=binance.symbols.refresh.failed phase={} action=use_stale cacheSize={} errClass={} errMsg=\"{}\" elapsedMs={}",
                        phase, stale.bySymbol.size(), ex.getClass().getSimpleName(), safeLog(ex.getMessage()), elapsedMsSince(startedNs));
                return stale;
            }
            throw new CopySymbolMetadataException(
                    "No se pudo cargar metadata de símbolos desde ms-binance",
                    ex,
                    Map.of("phase", phase, "elapsedMs", elapsedMsSince(startedNs))
            );
        }
    }

    private SymbolsCache buildSymbolsCache(List<BinanceFuturesSymbolInfoClientDto> symbols, long expiresAtMs) {
        final Map<String, BinanceFuturesSymbolInfoClientDto> bySymbol = symbols == null
                ? Collections.emptyMap()
                : symbols.stream()
                .filter(Objects::nonNull)
                .filter(this::isCopyTradableSymbol)
                .filter(s -> s.getSymbol() != null && !s.getSymbol().isBlank())
                .collect(Collectors.toMap(
                        s -> normalizeSymbolKey(s.getSymbol()),
                        s -> s,
                        (a, b) -> a
                ));

        if (bySymbol.isEmpty()) {
            throw new CopySymbolMetadataException(
                    "ms-binance devolvió 0 símbolos",
                    Map.of("symbolsNull", symbols == null)
            );
        }

        final Map<String, String> aliasToCanonical = new HashMap<>();
        final Set<String> ambiguousAliases = new HashSet<>();

        for (String canonical : bySymbol.keySet()) {
            registerAlias(aliasToCanonical, ambiguousAliases, canonical, canonical);
            for (String alias : deriveAliases(canonical)) {
                registerAlias(aliasToCanonical, ambiguousAliases, alias, canonical);
            }
        }

        return new SymbolsCache(expiresAtMs, bySymbol, aliasToCanonical, ambiguousAliases);
    }

    private Map<String, BinanceFuturesSymbolInfoClientDto> getSymbolsBySymbol() {
        return getOrRefreshSymbolsCache().bySymbol;
    }

    private boolean isCopyTradableSymbol(BinanceFuturesSymbolInfoClientDto symbolInfo) {
        if (symbolInfo == null) {
            return false;
        }
        String status = symbolInfo.getStatus();
        if (status == null || !"TRADING".equalsIgnoreCase(status.trim())) {
            return false;
        }
        String contractType = symbolInfo.getContractType();
        if (contractType != null && !"PERPETUAL".equalsIgnoreCase(contractType.trim())) {
            return false;
        }
        if (symbolInfo.getOrderTypes() != null
                && symbolInfo.getOrderTypes().stream().noneMatch(orderType -> "MARKET".equalsIgnoreCase(String.valueOf(orderType)))) {
            return false;
        }
        String symbol = normalizeSymbolKey(symbolInfo.getSymbol());
        String quote = normalizeSymbolKey(symbolInfo.getQuoteAsset());
        String margin = normalizeSymbolKey(symbolInfo.getMarginAsset());
        return symbol != null && quote != null && margin != null && symbol.endsWith(quote) && quote.equals(margin);
    }


    private SymbolContractResolution resolveSymbolContract(String rawSymbol, FuturesCapitalAsset preferredAsset) {
        if (rawSymbol == null || rawSymbol.isBlank()) {
            throw new SkipExecutionException(
                    "symbol_blank",
                    "Símbolo vacío/null",
                    LogFmt.kv("rawSymbol", rawSymbol)
            );
        }

        final SymbolsCache cache = getOrRefreshSymbolsCache();
        final String normalizedRaw = normalizeSymbolKey(rawSymbol);

        for (String candidate : buildSymbolCandidates(rawSymbol, preferredAsset)) {
            if (candidate == null || candidate.isBlank()) {
                continue;
            }

            if (cache.bySymbol.containsKey(candidate)) {
                return symbolContractResolution(rawSymbol, normalizedRaw, candidate, cache.bySymbol.get(candidate));
            }

            if (cache.ambiguousAliases.contains(candidate)) {
                throw new SkipExecutionException(
                        "symbol_alias_ambiguous",
                        "Alias ambiguo, no se puede resolver de forma segura",
                        LogFmt.kv("rawSymbol", rawSymbol, "candidate", candidate)
                );
            }

            final String canonical = cache.aliasToCanonical.get(candidate);
            if (canonical != null && cache.bySymbol.containsKey(canonical)) {
                return symbolContractResolution(rawSymbol, normalizedRaw, canonical, cache.bySymbol.get(canonical));
            }
        }

        throw new SkipExecutionException(
                "symbol_alias_not_found",
                "No se pudo resolver el símbolo contra exchangeInfo",
                LogFmt.kv("rawSymbol", rawSymbol)
        );
    }

    private SymbolContractResolution symbolContractResolution(String rawSymbol,
                                                              String normalizedRaw,
                                                              String canonicalSymbol,
                                                              BinanceFuturesSymbolInfoClientDto symbolInfo) {
        final String rawBase = baseWithoutQuote(normalizedRaw);
        final String canonicalBase = baseWithoutQuote(canonicalSymbol);
        final String rawAsset = stripKnownContractMultiplier(rawBase);
        final String canonicalAsset = stripKnownContractMultiplier(canonicalBase);
        BigDecimal rawMultiplier = leadingKnownContractMultiplier(rawBase);
        BigDecimal canonicalMultiplier = leadingKnownContractMultiplier(canonicalBase);
        BigDecimal priceMultiplier = ONE;
        if (rawAsset != null && rawAsset.equals(canonicalAsset)
                && rawMultiplier.compareTo(ZERO) > 0
                && canonicalMultiplier.compareTo(ZERO) > 0) {
            priceMultiplier = canonicalMultiplier.divide(rawMultiplier, DEFAULT_CALC_SCALE, RoundingMode.HALF_UP).stripTrailingZeros();
        }
        if (priceMultiplier.compareTo(ZERO) <= 0) {
            priceMultiplier = ONE;
        }
        return new SymbolContractResolution(
                rawSymbol,
                canonicalSymbol,
                rawBase,
                canonicalBase,
                rawMultiplier,
                canonicalMultiplier,
                priceMultiplier,
                symbolInfo
        );
    }

    private String baseWithoutQuote(String symbol) {
        String normalized = normalizeSymbolKey(symbol);
        if (normalized == null || normalized.isBlank()) {
            return null;
        }
        String quote = extractQuote(normalized);
        if (quote == null || normalized.length() <= quote.length()) {
            return normalized;
        }
        return normalized.substring(0, normalized.length() - quote.length());
    }

    private String stripKnownContractMultiplier(String base) {
        if (base == null || base.isBlank()) {
            return null;
        }
        for (String multiplier : CONTRACT_MULTIPLIERS) {
            if (base.startsWith(multiplier) && base.length() > multiplier.length()) {
                return base.substring(multiplier.length());
            }
        }
        return base;
    }

    private BigDecimal leadingKnownContractMultiplier(String base) {
        if (base == null || base.isBlank()) {
            return ONE;
        }
        final Matcher matcher = LEADING_MULTIPLIER_PATTERN.matcher(base);
        if (!matcher.matches()) {
            return ONE;
        }
        final String multiplier = matcher.group(1);
        return CONTRACT_MULTIPLIERS.contains(multiplier) ? new BigDecimal(multiplier) : ONE;
    }

    private void registerAlias(Map<String, String> aliasToCanonical,
                               Set<String> ambiguousAliases,
                               String alias,
                               String canonical) {
        if (alias == null || alias.isBlank() || canonical == null || canonical.isBlank()) {
            return;
        }

        final String aliasKey = normalizeSymbolKey(alias);
        final String canonicalKey = normalizeSymbolKey(canonical);

        final String existing = aliasToCanonical.putIfAbsent(aliasKey, canonicalKey);
        if (existing != null && !existing.equals(canonicalKey)) {
            aliasToCanonical.remove(aliasKey);
            ambiguousAliases.add(aliasKey);
        }
    }

    private List<String> deriveAliases(String canonicalSymbol) {
        final List<String> aliases = new ArrayList<>();
        if (canonicalSymbol == null || canonicalSymbol.isBlank()) {
            return aliases;
        }

        final String symbol = normalizeSymbolKey(canonicalSymbol);
        final String quote = extractQuote(symbol);
        if (quote == null) {
            return aliases;
        }

        final String base = symbol.substring(0, symbol.length() - quote.length());
        final Matcher matcher = LEADING_MULTIPLIER_PATTERN.matcher(base);
        if (!matcher.matches()) {
            return aliases;
        }

        final String multiplierRaw = matcher.group(1);
        final String asset = matcher.group(2);
        if (asset == null || asset.isBlank()) {
            return aliases;
        }

        aliases.add(asset + quote);

        final String compactPrefix = compactMultiplier(multiplierRaw);
        if (compactPrefix != null && !compactPrefix.isBlank()) {
            aliases.add(compactPrefix + asset + quote);
        }

        if ("1000".equals(multiplierRaw)) {
            aliases.add("K" + asset + quote);
        }
        if ("1000000".equals(multiplierRaw)) {
            aliases.add("M" + asset + quote);
        }
        if ("1000000000".equals(multiplierRaw)) {
            aliases.add("B" + asset + quote);
        }

        return aliases;
    }

    private String extractQuote(String symbol) {
        if (symbol == null || symbol.isBlank()) {
            return null;
        }
        for (String quote : KNOWN_QUOTES) {
            if (symbol.endsWith(quote)) {
                return quote;
            }
        }
        return null;
    }

    private String compactMultiplier(String multiplierRaw) {
        try {
            final BigDecimal value = new BigDecimal(multiplierRaw);
            final BigDecimal thousand = new BigDecimal("1000");
            final BigDecimal million = new BigDecimal("1000000");
            final BigDecimal billion = new BigDecimal("1000000000");

            if (value.compareTo(billion) >= 0 && value.remainder(billion).compareTo(ZERO) == 0) {
                final BigDecimal units = value.divide(billion).stripTrailingZeros();
                return units.compareTo(ONE) == 0 ? "B" : units.toPlainString() + "B";
            }
            if (value.compareTo(million) >= 0 && value.remainder(million).compareTo(ZERO) == 0) {
                final BigDecimal units = value.divide(million).stripTrailingZeros();
                return units.compareTo(ONE) == 0 ? "M" : units.toPlainString() + "M";
            }
            if (value.compareTo(thousand) >= 0 && value.remainder(thousand).compareTo(ZERO) == 0) {
                final BigDecimal units = value.divide(thousand).stripTrailingZeros();
                return units.compareTo(ONE) == 0 ? "K" : units.toPlainString() + "K";
            }

            return multiplierRaw;
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    private String normalizeSymbolKey(String raw) {
        if (raw == null) {
            return null;
        }
        return raw.trim()
                .toUpperCase()
                .replace("-", "")
                .replace("_", "")
                .replace("/", "")
                .replace(".", "")
                .replace(" ", "");
    }

    private SymbolRules extractRules(BinanceFuturesSymbolInfoClientDto symbolDetail) {
        if (symbolDetail == null || symbolDetail.getFilters() == null) {
            return null;
        }

        final Integer quantityPrecision = symbolDetail.getQuantityPrecision();
        final int qtyScaleFromBinance = quantityPrecision != null ? Math.max(0, quantityPrecision) : 0;

        BigDecimal stepSize = null;
        BigDecimal minQty = null;

        for (BinanceFuturesSymbolFilterDto f : symbolDetail.getFilters()) {
            if (f == null || f.getFilterType() == null) {
                continue;
            }

            if (FILTER_TYPE_LOT_SIZE.equalsIgnoreCase(f.getFilterType())
                    || FILTER_TYPE_MARKET_LOT_SIZE.equalsIgnoreCase(f.getFilterType())) {
                stepSize = safeBigDecimal(f.getStepSize());
                minQty = safeBigDecimal(f.getMinQty());
                break;
            }
        }

        final int stepScale = stepSize == null ? 0 : Math.max(0, stepSize.stripTrailingZeros().scale());
        final int finalScale = Math.max(qtyScaleFromBinance, stepScale);

        BigDecimal exchangeMinNotional = null;
        for (BinanceFuturesSymbolFilterDto f : symbolDetail.getFilters()) {
            if (f == null || f.getFilterType() == null) {
                continue;
            }

            if (FILTER_TYPE_MIN_NOTIONAL.equalsIgnoreCase(f.getFilterType())
                    || FILTER_TYPE_NOTIONAL.equalsIgnoreCase(f.getFilterType())) {
                exchangeMinNotional = safeBigDecimal(f.getNotional());
                break;
            }
        }

        if (exchangeMinNotional == null) {
            exchangeMinNotional = MIN_NOTIONAL_FALLBACK;
        }

        final BigDecimal minNotionalFloor =
                configuredMinNotionalFloor == null ? ZERO : configuredMinNotionalFloor.max(ZERO);

        final BigDecimal baseMinNotional = exchangeMinNotional.max(minNotionalFloor);
        final BigDecimal safetyPct = safePct(minNotionalSafetyPct, new BigDecimal("0.20"));
        final BigDecimal effectiveMinNotional = baseMinNotional
                .multiply(ONE.add(safetyPct))
                .setScale(DEFAULT_CALC_SCALE, RoundingMode.CEILING)
                .stripTrailingZeros();

        return new SymbolRules(stepSize, minQty, exchangeMinNotional, effectiveMinNotional, finalScale);
    }

    private BigDecimal adjustQuantityToBinanceRules(String symbol,
                                                    BigDecimal quantity,
                                                    BigDecimal entryPrice,
                                                    SymbolRules rules,
                                                    BigDecimal notionalMax) {

        if (quantity == null || entryPrice == null || rules == null || notionalMax == null) {
            return ZERO;
        }
        if (quantity.compareTo(ZERO) <= 0 || entryPrice.compareTo(ZERO) <= 0 || notionalMax.compareTo(ZERO) <= 0) {
            return ZERO;
        }

        BigDecimal adjusted = quantity;

        if (rules.stepSize != null && rules.stepSize.compareTo(ZERO) > 0) {
            adjusted = adjusted.divide(rules.stepSize, 0, RoundingMode.DOWN).multiply(rules.stepSize);
        }

        if (rules.minQty != null && adjusted.compareTo(rules.minQty) < 0) {
            adjusted = rules.minQty;
        }

        if (rules.qtyScale >= 0) {
            adjusted = adjusted.setScale(rules.qtyScale, RoundingMode.DOWN);
        }

        if (adjusted.compareTo(ZERO) <= 0) {
            return ZERO;
        }

        BigDecimal notionalFinal = adjusted.multiply(entryPrice);

        if (notionalFinal.compareTo(notionalMax) > 0) {
            final BigDecimal buf = safePct(notionalBufferPct, new BigDecimal("0.30"));
            final BigDecimal targetNotional = notionalMax.multiply(ONE.subtract(buf));

            BigDecimal q = targetNotional.divide(entryPrice, DEFAULT_CALC_SCALE, RoundingMode.DOWN);

            if (rules.stepSize != null && rules.stepSize.compareTo(ZERO) > 0) {
                q = q.divide(rules.stepSize, 0, RoundingMode.DOWN).multiply(rules.stepSize);
            }

            if (rules.minQty != null && q.compareTo(rules.minQty) < 0) {
                q = rules.minQty;
            }

            if (rules.qtyScale >= 0) {
                q = q.setScale(rules.qtyScale, RoundingMode.DOWN);
            }

            adjusted = q;
            if (adjusted.compareTo(ZERO) <= 0) {
                return ZERO;
            }

            notionalFinal = adjusted.multiply(entryPrice);

            if (notionalFinal.compareTo(notionalMax) > 0) {
                log.info("event=binance.qty_skip_over_budget symbol={} notionalFinal={} notionalMax={}",
                        symbol, notionalFinal.toPlainString(), notionalMax.toPlainString());
                return ZERO;
            }
        }

        log.debug(LOG_QTY_ADJUSTED,
                symbol,
                quantity.toPlainString(),
                adjusted.toPlainString(),
                notionalFinal.toPlainString(),
                notionalMax.toPlainString());

        return adjusted;
    }

    private BigDecimal adjustQuantityUpToMinNotional(String symbol,
                                                     BigDecimal quantity,
                                                     BigDecimal entryPrice,
                                                     SymbolRules rules,
                                                     BigDecimal minNotional,
                                                     BigDecimal notionalMax) {
        if (quantity == null || entryPrice == null || rules == null || minNotional == null || notionalMax == null) {
            return ZERO;
        }
        if (entryPrice.compareTo(ZERO) <= 0 || minNotional.compareTo(ZERO) <= 0 || notionalMax.compareTo(ZERO) <= 0) {
            return ZERO;
        }

        BigDecimal adjusted = quantity.max(ZERO);
        if (adjusted.multiply(entryPrice).compareTo(minNotional) >= 0) {
            return adjusted;
        }

        BigDecimal required = minNotional.divide(entryPrice, DEFAULT_CALC_SCALE, RoundingMode.CEILING);

        if (rules.stepSize != null && rules.stepSize.compareTo(ZERO) > 0) {
            required = required.divide(rules.stepSize, 0, RoundingMode.CEILING).multiply(rules.stepSize);
        }

        if (rules.minQty != null && required.compareTo(rules.minQty) < 0) {
            required = rules.minQty;
        }

        if (rules.qtyScale >= 0) {
            required = required.setScale(rules.qtyScale, RoundingMode.CEILING);
        }

        final BigDecimal finalNotional = required.multiply(entryPrice);
        if (finalNotional.compareTo(notionalMax) > 0) {
            log.info("event=binance.qty_raise_to_min_notional.skip reason=budget_exceeded symbol={} qtyCandidate={} finalNotional={} minNotional={} notionalMax={} stepSize={} minQty={} qtyScale={}",
                    symbol,
                    required.toPlainString(),
                    finalNotional.toPlainString(),
                    minNotional.toPlainString(),
                    notionalMax.toPlainString(),
                    rules.stepSize,
                    rules.minQty,
                    rules.qtyScale);
            return ZERO;
        }

        log.info("event=binance.qty_raise_to_min_notional.ok symbol={} qtyOriginal={} qtyFinal={} finalNotional={} minNotional={} notionalMax={}",
                symbol,
                quantity.toPlainString(),
                required.toPlainString(),
                finalNotional.toPlainString(),
                minNotional.toPlainString(),
                notionalMax.toPlainString());

        return required;
    }

    private void validateCopyByMinNotionalPolicy(CopyMinNotionalPolicy policy,
                                                 BigDecimal minNotional,
                                                 String details) {
        final CopyMinNotionalPolicy effectivePolicy = policy == null ? CopyMinNotionalPolicy.skip() : policy;

        if (!effectivePolicy.isCopyByBinanceMinEnabled()) {
            throw new SkipExecutionException(
                    "notional_too_small",
                    "Operación bajo el mínimo de Binance y la política efectiva es SKIP",
                    copyMinPolicyDetails(effectivePolicy, minNotional, details)
            );
        }

        if (effectivePolicy.minScore() != null
                && (effectivePolicy.score() == null || effectivePolicy.score() < effectivePolicy.minScore())) {
            throw new SkipExecutionException(
                    "copy_by_min_score_blocked",
                    "Score de wallet bajo el mínimo configurado para copiar por mínimo Binance",
                    copyMinPolicyDetails(effectivePolicy, minNotional, details)
            );
        }

        if (effectivePolicy.minHistoryDays() != null
                && (effectivePolicy.historyDays() == null
                || effectivePolicy.historyDays().compareTo(BigDecimal.valueOf(effectivePolicy.minHistoryDays())) < 0)) {
            throw new SkipExecutionException(
                    "copy_by_min_history_blocked",
                    "Historial de wallet bajo el mínimo configurado para copiar por mínimo Binance",
                    copyMinPolicyDetails(effectivePolicy, minNotional, details)
            );
        }

        if (effectivePolicy.minOperations() != null
                && (effectivePolicy.operationsCount() == null || effectivePolicy.operationsCount() < effectivePolicy.minOperations())) {
            throw new SkipExecutionException(
                    "copy_by_min_operations_blocked",
                    "Cantidad de operaciones de wallet bajo el mínimo configurado para copiar por mínimo Binance",
                    copyMinPolicyDetails(effectivePolicy, minNotional, details)
            );
        }

        if (effectivePolicy.maxNotionalUsdt() != null
                && minNotional != null
                && minNotional.compareTo(effectivePolicy.maxNotionalUsdt()) > 0) {
            throw new SkipExecutionException(
                    "copy_by_min_cap_exceeded",
                    "Mínimo Binance supera el máximo USDT permitido para copiar por mínimo",
                    copyMinPolicyDetails(effectivePolicy, minNotional, details)
            );
        }
    }

    private String copyMinPolicyDetails(CopyMinNotionalPolicy policy, BigDecimal minNotional, String details) {
        final CopyMinNotionalPolicy effectivePolicy = policy == null ? CopyMinNotionalPolicy.skip() : policy;
        return LogFmt.kv(
                "policyMode", effectivePolicy.mode(),
                "allocationId", effectivePolicy.allocationId(),
                "score", effectivePolicy.score(),
                "minScore", effectivePolicy.minScore(),
                "historyDays", effectivePolicy.historyDays(),
                "minHistoryDays", effectivePolicy.minHistoryDays(),
                "operationsCount", effectivePolicy.operationsCount(),
                "minOperations", effectivePolicy.minOperations(),
                "maxForcedNotional", effectivePolicy.maxNotionalUsdt(),
                "minNotional", minNotional,
                "details", details
        );
    }

    private void persistActiveAfterOrder(CopyOperationDto operation, String traceId, String reasonCode) {
        try {
            copyOperationService.upsertActiveOperation(operation);
        } catch (EngineException | DataAccessException | IllegalStateException | IllegalArgumentException ex) {
            activeCopyOperationCache.markUncertain(operation, traceId, reasonCode);
            throw ex;
        }
    }

    private void createNewOperation(BinanceFuturesOrderClientResponse order,
                                    String idWallet,
                                    UUID idOperation,
                                    String idUser,
                                    int leverage) {
        createNewOperation(order, idWallet, idOperation, idUser, leverage, null);
    }

    private void createNewOperation(BinanceFuturesOrderClientResponse order,
                                    String idWallet,
                                    UUID idOperation,
                                    String idUser,
                                    int leverage,
                                    UserCopyAllocationEntity allocation) {

        if (!isValidOrderResponse(order)) {
            log.warn(LOG_COPY_CREATE_INVALID, idUser, idWallet, idOperation);
            throw new CopyBinanceClientException(
                    "Respuesta inválida de ms-binance al crear copy_operation",
                    orderResponseDetails(
                            idOperation == null ? null : idOperation.toString(),
                            idUser,
                            idWallet,
                            order == null ? null : order.getSymbol(),
                            order
                    )
            );
        }

        final CopyOperationDto buildCopyOperation = copyTradingMapper.buildCopyNewOperationDto(order, idWallet, idOperation, idUser, leverage);
        applyCopyMetadata(buildCopyOperation, allocation);
        copyOperationService.newOperation(buildCopyOperation);
    }

    private void applyCopyMetadata(CopyOperationDto dto, UserCopyAllocationEntity allocation) {
        if (dto == null) return;
        dto.setUserCopyAllocationId(allocation == null ? null : allocation.getId());
        dto.setCopyStrategyCode(allocation == null ? null : allocation.getCopyStrategyCode());
        dto.setExecutionMode(executionModeOf(allocation));
        dto.setShadow(isShadowMode(allocation));
        dto.setShadowStatus(isShadowMode(allocation) ? "FILLED" : null);
    }

    private UserCopyAllocationEntity allocationMetadataFromCopy(CopyOperationDto copy) {
        if (copy == null) return null;
        return UserCopyAllocationEntity.builder()
                .id(copy.getUserCopyAllocationId())
                .copyStrategyCode(copy.getCopyStrategyCode())
                .executionMode(copy.getExecutionMode())
                .status(UserCopyAllocationEntity.Status.ACTIVE)
                .isActive(true)
                .build();
    }

    private UserCopyAllocationEntity allocationMetadataFromCopyOrFallback(CopyOperationDto copy, UserCopyAllocationEntity fallback) {
        UserCopyAllocationEntity fromCopy = allocationMetadataFromCopy(copy);
        if (fallback != null) {
            if (fromCopy == null || fromCopy.getId() == null || Objects.equals(fromCopy.getId(), fallback.getId())) {
                return fallback;
            }
            String fromStrategy = fromCopy.getCopyStrategyCode() == null ? null : fromCopy.getCopyStrategyCode().trim().toUpperCase(java.util.Locale.ROOT).replace('-', '_');
            String fallbackStrategy = fallback.getCopyStrategyCode() == null ? null : fallback.getCopyStrategyCode().trim().toUpperCase(java.util.Locale.ROOT).replace('-', '_');
            if (fromStrategy != null && fromStrategy.equals(fallbackStrategy)) {
                return fallback;
            }
        }
        if (fromCopy == null || (fromCopy.getId() == null && fromCopy.getCopyStrategyCode() == null)) {
            return fallback;
        }
        return fromCopy;
    }

    private boolean allocationAllowsNewExposure(UserCopyAllocationEntity allocation, String traceId, String originId, String userId, String walletId, String symbol, String reasonAlias) {
        if (allocation == null || allocation.getId() == null) {
            log.info("event=copy.exposure.skip reasonCode=allocation_missing reasonAlias={} traceId={} originId={} userId={} wallet={} symbol={}",
                    reasonAlias, traceId, originId, userId, walletId, symbol);
            return false;
        }
        if (allocation.allowsNewEntries(OffsetDateTime.now())) {
            return true;
        }
        log.info("event=copy.exposure.skip reasonCode=allocation_not_openable reasonAlias={} traceId={} originId={} userId={} wallet={} symbol={} allocationId={} strategy={} status={} cooldownUntil={} executionMode={}",
                reasonAlias, traceId, originId, userId, walletId, symbol,
                allocation.getId(), allocation.getCopyStrategyCode(), allocation.getStatus(), allocation.getStatusCooldownUntil(), allocation.getExecutionMode());
        return false;
    }

    private String strategyOf(UserCopyAllocationEntity allocation) {
        return normalizeStrategy(allocation == null ? null : allocation.getCopyStrategyCode());
    }

    private String normalizeStrategy(String strategyCode) {
        if (strategyCode == null || strategyCode.isBlank()) {
            return CopyStrategyRuntimeRouter.DEFAULT_STRATEGY_CODE;
        }
        return strategyCode.trim().toUpperCase(java.util.Locale.ROOT).replace('-', '_');
    }

    private String executionModeOf(UserCopyAllocationEntity allocation) {
        if (allocation == null || allocation.getExecutionMode() == null || allocation.getExecutionMode().isBlank()) {
            return "LIVE";
        }
        String mode = allocation.getExecutionMode().trim().toUpperCase(java.util.Locale.ROOT).replace('-', '_');
        return "SHADOW".equals(mode) ? "SHADOW" : "LIVE";
    }

    private boolean isShadowMode(UserCopyAllocationEntity allocation) {
        return allocation != null && allocation.isShadowMode();
    }

    private BinanceFuturesOrderClientResponse executeOrShadow(OperationDto dto, BigDecimal priceRef, UserCopyAllocationEntity allocation, String traceId) {
        if (!isShadowMode(allocation)) {
            return procesBinanceService.operationPosition(dto);
        }
        BinanceFuturesOrderClientResponse response = buildShadowOrderResponse(dto, priceRef);
        log.info("event=copy.shadow.order_filled traceId={} userId={} wallet={} strategy={} symbol={} side={} positionSide={} qty={} price={} clientOrderId={} shadowOrderId={} mode=SHADOW",
                traceId, dto == null ? null : dto.getUserId(), dto == null ? null : dto.getWalletId(),
                allocation == null ? null : allocation.getCopyStrategyCode(), dto == null ? null : dto.getSymbol(),
                dto == null ? null : dto.getSide(), dto == null ? null : dto.getPositionSide(),
                dto == null ? null : dto.getQuantity(), response.getAvgPrice(),
                dto == null ? null : dto.getClientOrderId(), response.getOrderId());
        return response;
    }

    private BinanceFuturesOrderClientResponse buildShadowOrderResponse(OperationDto dto, BigDecimal priceRef) {
        BigDecimal qty = parseQuantity(dto == null ? null : dto.getQuantity());
        if (qty == null || qty.compareTo(ZERO) <= 0) {
            qty = ZERO;
        }
        BigDecimal price = resolvePriceRef(priceRef, safeBigDecimal(dto == null ? null : dto.getPrice()));
        if (price.compareTo(ZERO) <= 0) {
            price = ONE;
        }
        BinanceFuturesOrderClientResponse response = new BinanceFuturesOrderClientResponse();
        response.setOrderId(shadowOrderId(dto));
        response.setSymbol(dto == null ? null : dto.getSymbol());
        response.setStatus("FILLED");
        response.setClientOrderId(dto == null ? null : dto.getClientOrderId());
        response.setPrice(price);
        response.setAvgPrice(price);
        response.setOrigQty(qty);
        response.setExecutedQty(qty);
        response.setCumQty(qty);
        response.setCumQuote(qty.multiply(price));
        response.setType(dto == null || dto.getType() == null ? null : dto.getType().name());
        response.setSide(dto == null || dto.getSide() == null ? null : dto.getSide().name());
        response.setPositionSide(dto == null || dto.getPositionSide() == null ? null : dto.getPositionSide().name());
        response.setReduceOnly(dto != null && dto.isReduceOnly());
        response.setClosePosition(false);
        response.setUpdateTime(System.currentTimeMillis());
        return response;
    }

    private long shadowOrderId(OperationDto dto) {
        String seed = dto == null ? java.util.UUID.randomUUID().toString() : firstNonBlank(dto.getClientOrderId(), dto.getOriginId(), java.util.UUID.randomUUID().toString());
        return -Math.abs((long) seed.hashCode() * 1000003L + System.currentTimeMillis() % 1000003L);
    }

    private Map<String, Object> orderResponseDetails(String originId,
                                                     String userId,
                                                     String walletId,
                                                     String symbol,
                                                     BinanceFuturesOrderClientResponse order) {
        final Map<String, Object> details = new HashMap<>();
        details.put("originId", originId);
        details.put("userId", userId);
        details.put("wallet", walletId);
        details.put("symbol", symbol);
        details.put("orderPresent", order != null);
        details.put("orderId", order == null ? null : order.getOrderId());
        details.put("avgPrice", order == null ? null : order.getAvgPrice());
        details.put("executedQty", order == null ? null : order.getExecutedQty());
        details.put("cumQty", order == null ? null : order.getCumQty());
        details.put("origQty", order == null ? null : order.getOrigQty());
        details.put("updateTime", order == null ? null : order.getUpdateTime());
        details.put("status", order == null ? null : order.getStatus());
        return details;
    }

    private boolean isValidOrderResponse(BinanceFuturesOrderClientResponse r) {
        return r != null
                && r.getOrderId() != null
                && r.getSymbol() != null
                && (r.getExecutedQty() != null || r.getCumQty() != null || r.getOrigQty() != null)
                && r.getAvgPrice() != null
                && r.getUpdateTime() != null;
    }


    private boolean passesWalletFilters(MetricaWalletDto walletMetric) {
        if (walletMetric == null || walletMetric.getScoring() == null) {
            return false;
        }
        return Boolean.TRUE;
    }

    private String userWalletKey(String userId, String walletId) {
        return userId + "::" + walletId;
    }

    private String distLockKey(String userId, String walletId) {
        return "copy-wallet-lock::" + userWalletKey(userId, walletId);
    }

    private BigDecimal safeBigDecimal(String raw) {
        try {
            if (raw == null || raw.isBlank()) {
                return null;
            }
            return new BigDecimal(raw);
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    private record TargetLeg(String originId,
                             String walletId,
                             String symbol,
                             PositionSide side,
                             int leverage,
                             BigDecimal priceRef,
                             BigDecimal targetQty,
                             BigDecimal targetNotional,
                             BigDecimal entryPrice,
                             OffsetDateTime sourceCreatedAt,
                             OffsetDateTime sourceUpdatedAt,
                             OffsetDateTime sourceTs) {
    }

    private record SymbolContractResolution(String rawSymbol,
                                            String canonicalSymbol,
                                            String rawBase,
                                            String canonicalBase,
                                            BigDecimal rawMultiplier,
                                            BigDecimal canonicalMultiplier,
                                            BigDecimal priceMultiplier,
                                            BinanceFuturesSymbolInfoClientDto symbolInfo) {
        BigDecimal executionPrice(BigDecimal originUnitPrice) {
            if (originUnitPrice == null || originUnitPrice.compareTo(ZERO) <= 0) {
                return ZERO;
            }
            BigDecimal multiplier = priceMultiplier == null || priceMultiplier.compareTo(ZERO) <= 0 ? ONE : priceMultiplier;
            return originUnitPrice.multiply(multiplier).setScale(DEFAULT_CALC_SCALE, RoundingMode.HALF_UP).stripTrailingZeros();
        }

        boolean usesDifferentContractUnit() {
            BigDecimal multiplier = priceMultiplier == null ? ONE : priceMultiplier;
            return multiplier.compareTo(ONE) != 0;
        }
    }

    private static final class SymbolRules {
        private final BigDecimal stepSize;
        private final BigDecimal minQty;
        private final BigDecimal exchangeMinNotional;
        private final BigDecimal effectiveMinNotional;
        private final int qtyScale;

        private SymbolRules(BigDecimal stepSize,
                            BigDecimal minQty,
                            BigDecimal exchangeMinNotional,
                            BigDecimal effectiveMinNotional,
                            int qtyScale) {
            this.stepSize = stepSize;
            this.minQty = minQty;
            this.exchangeMinNotional = exchangeMinNotional;
            this.effectiveMinNotional = effectiveMinNotional;
            this.qtyScale = qtyScale;
        }
    }

    private static final class PreparedOpen {
        private final OperationDto dto;
        private final String symbol;
        private final int leverage;
        private final BigDecimal notional;
        private final BigDecimal marginRequired;
        private final BigDecimal walletMarginBudget;
        private final BigDecimal entryPrice;
        private final String userWalletKey;

        private PreparedOpen(OperationDto dto,
                             String symbol,
                             int leverage,
                             BigDecimal notional,
                             BigDecimal marginRequired,
                             BigDecimal walletMarginBudget,
                             BigDecimal entryPrice,
                             String userWalletKey) {
            this.dto = dto;
            this.symbol = symbol;
            this.leverage = leverage;
            this.notional = notional;
            this.marginRequired = marginRequired;
            this.walletMarginBudget = walletMarginBudget;
            this.entryPrice = entryPrice;
            this.userWalletKey = userWalletKey;
        }
    }

    private static final class SymbolsCache {
        private final long expiresAtMs;
        private final Map<String, BinanceFuturesSymbolInfoClientDto> bySymbol;
        private final Map<String, String> aliasToCanonical;
        private final Set<String> ambiguousAliases;

        private SymbolsCache(long expiresAtMs,
                             Map<String, BinanceFuturesSymbolInfoClientDto> bySymbol,
                             Map<String, String> aliasToCanonical,
                             Set<String> ambiguousAliases) {
            this.expiresAtMs = expiresAtMs;
            this.bySymbol = bySymbol;
            this.aliasToCanonical = aliasToCanonical;
            this.ambiguousAliases = ambiguousAliases;
        }
    }

    private BigDecimal safePct(BigDecimal raw, BigDecimal max) {
        if (raw == null) {
            return ZERO;
        }
        if (raw.compareTo(ZERO) < 0) {
            return ZERO;
        }
        if (raw.compareTo(max) > 0) {
            return max;
        }
        return raw;
    }

    private List<MetricaWalletDto> normalizeCapitalShares(List<MetricaWalletDto> metrics) {
        if (metrics == null || metrics.isEmpty()) {
            return metrics;
        }

        double total = 0.0;
        for (MetricaWalletDto m : metrics) {
            if (m == null) {
                continue;
            }
            Double s = m.getCapitalShare();
            if (s != null && Double.isFinite(s) && s > 0) {
                total += s;
            }
        }

        if (total <= 0.0) {
            return metrics;
        }

        if (total > 1.0) {
            for (MetricaWalletDto m : metrics) {
                if (m == null) {
                    continue;
                }
                Double s = m.getCapitalShare();
                if (s == null || !Double.isFinite(s) || s <= 0) {
                    m.setCapitalShare(0.0);
                } else {
                    m.setCapitalShare(s / total);
                }
            }
        } else {
            for (MetricaWalletDto m : metrics) {
                if (m == null) {
                    continue;
                }
                Double s = m.getCapitalShare();
                if (s == null || !Double.isFinite(s) || s < 0) {
                    m.setCapitalShare(0.0);
                }
            }
        }

        return metrics;
    }

    private List<String> buildSymbolCandidates(String rawSymbol) {
        return buildSymbolCandidates(rawSymbol, FuturesCapitalAsset.defaultAsset());
    }

    private List<String> buildSymbolCandidates(String rawSymbol, FuturesCapitalAsset preferredAsset) {
        final LinkedHashSet<String> candidates = new LinkedHashSet<>();

        final String key = normalizeSymbolKey(rawSymbol);
        if (key == null || key.isBlank()) {
            return List.of();
        }

        final FuturesCapitalAsset safePreferredAsset = preferredAsset == null ? FuturesCapitalAsset.defaultAsset() : preferredAsset;
        final String preferredQuote = safePreferredAsset.name();

        final String strippedVersion = stripVersionSuffix(key);
        addPreferredQuoteCandidates(candidates, key, preferredQuote);
        addPreferredQuoteCandidates(candidates, strippedVersion, preferredQuote);

        addIfSameQuoteOrQuoteMissing(candidates, key, preferredQuote);
        addIfSameQuoteOrQuoteMissing(candidates, strippedVersion, preferredQuote);

        final String manualAlias = MANUAL_SYMBOL_ALIASES.get(key);
        if (manualAlias != null && !manualAlias.isBlank()) {
            addPreferredQuoteCandidates(candidates, normalizeSymbolKey(manualAlias), preferredQuote);
        }

        final String manualAliasStripped = MANUAL_SYMBOL_ALIASES.get(strippedVersion);
        if (manualAliasStripped != null && !manualAliasStripped.isBlank()) {
            addPreferredQuoteCandidates(candidates, normalizeSymbolKey(manualAliasStripped), preferredQuote);
        }

        addUsdToPreferredQuoteCandidates(candidates, key, preferredQuote);
        addUsdToPreferredQuoteCandidates(candidates, strippedVersion, preferredQuote);

        return new ArrayList<>(candidates);
    }

    private void addUsdToPreferredQuoteCandidates(Set<String> candidates, String symbol, String preferredQuote) {
        if (symbol == null || symbol.isBlank()) {
            return;
        }

        if (symbol.endsWith("USD") && !symbol.endsWith("USDT")) {
            candidates.add(symbol.substring(0, symbol.length() - 3) + preferredQuote);
        }
    }

    private void addPreferredQuoteCandidates(Set<String> candidates, String symbol, String preferredQuote) {
        if (symbol == null || symbol.isBlank() || preferredQuote == null || preferredQuote.isBlank()) {
            return;
        }
        String quote = extractQuote(symbol);
        if (quote == null) {
            addUsdToPreferredQuoteCandidates(candidates, symbol, preferredQuote);
            return;
        }
        String base = symbol.substring(0, symbol.length() - quote.length());
        if (!base.isBlank()) {
            candidates.add(base + preferredQuote);
        }
    }

    private void addIfSameQuoteOrQuoteMissing(Set<String> candidates, String symbol, String preferredQuote) {
        if (symbol == null || symbol.isBlank()) {
            return;
        }
        String quote = extractQuote(symbol);
        if (quote == null || quote.equals(preferredQuote)) {
            candidates.add(symbol);
        }
    }

    private FuturesCapitalAsset resolveCapitalAsset(UserDetailDto userDetail) {
        if (userDetail == null || userDetail.getDetail() == null) {
            return FuturesCapitalAsset.defaultAsset();
        }
        return FuturesCapitalAsset.fromNullable(userDetail.getDetail().getCapitalAsset());
    }

    private String stripVersionSuffix(String symbol) {
        if (symbol == null || symbol.isBlank()) {
            return symbol;
        }

        return symbol.replaceAll("V\\d+(?=USD$|USDT$|USDC$|FDUSD$|BUSD$)", "");
    }

    private static final Map<String, String> MANUAL_SYMBOL_ALIASES = Map.ofEntries(
            Map.entry("XAUTV2USD", "XAUTUSDT"),
            Map.entry("SPX6900USD", "SPXUSDT"),
            Map.entry("PIUSD", "PIUSDT")
    );

    private BigDecimal resolveCapitalReference(MetricaWalletDto walletMetric) {
        if (walletMetric == null) {
            return BigDecimal.valueOf(DEFAULT_BASE_CAPITAL);
        }

        if (walletMetric.getCapitalModel() != null) {
            final BigDecimal sourceTotalCapital = safeMetricDecimal(walletMetric.getCapitalModel().getSourceTotalCapitalUSDT());
            if (sourceTotalCapital.compareTo(ZERO) > 0) {
                return sourceTotalCapital;
            }

            final BigDecimal capitalReference = safeMetricDecimal(walletMetric.getCapitalModel().getCapitalReferenceUSDT());
            if (capitalReference.compareTo(ZERO) > 0) {
                return capitalReference;
            }

            final BigDecimal capitalReferenceBuffered = safeMetricDecimal(walletMetric.getCapitalModel().getCapitalReferenceBufferedUSDT());
            if (capitalReferenceBuffered.compareTo(ZERO) > 0) {
                return capitalReferenceBuffered;
            }
        }

        if (walletMetric.getExposureAndCapacity() != null) {
            final BigDecimal capitalRequired = safeMetricDecimal(walletMetric.getExposureAndCapacity().getCapitalRequired());
            if (capitalRequired.compareTo(ZERO) > 0) {
                return capitalRequired;
            }
        }

        return BigDecimal.valueOf(DEFAULT_BASE_CAPITAL);
    }

    private BigDecimal resolveSourceMarginForSizing(OriginBasketPositionDto leg, MetricaWalletDto walletMetric) {
        if (leg == null) {
            return ZERO;
        }

        BigDecimal margin = abs(leg.getMarginUsedUsd());
        if (margin.compareTo(ZERO) > 0) {
            return margin;
        }

        final int leverageRef = resolveSourceLeverageReference(walletMetric);

        BigDecimal notional = abs(leg.getNotionalUsd());
        if (notional.compareTo(ZERO) > 0 && leverageRef > 0) {
            return notional.divide(BigDecimal.valueOf(leverageRef), DEFAULT_CALC_SCALE, RoundingMode.HALF_UP);
        }

        final BigDecimal priceRef = resolvePriceRef(leg.getMarkPrice(), leg.getEntryPrice());
        notional = safeQty(leg.getSizeQty()).multiply(priceRef);
        if (notional.compareTo(ZERO) > 0 && leverageRef > 0) {
            return notional.divide(BigDecimal.valueOf(leverageRef), DEFAULT_CALC_SCALE, RoundingMode.HALF_UP);
        }

        return abs(leg.getSizeLegacy());
    }

    private BigDecimal resolveSourceMarginForSizing(OperacionDto originOperation, MetricaWalletDto walletMetric) {
        if (originOperation == null) {
            return ZERO;
        }

        BigDecimal margin = abs(originOperation.getMarginUsedUsd());
        if (margin.compareTo(ZERO) > 0) {
            return margin;
        }

        final int leverageRef = resolveSourceLeverageReference(walletMetric);

        BigDecimal notional = abs(originOperation.getNotionalUsd());
        if (notional.compareTo(ZERO) > 0 && leverageRef > 0) {
            return notional.divide(BigDecimal.valueOf(leverageRef), DEFAULT_CALC_SCALE, RoundingMode.HALF_UP);
        }

        final BigDecimal priceRef = resolvePriceRef(originOperation.getPrecioMercado(), originOperation.getPrecioEntrada());
        notional = safeQty(originOperation.getSizeQty()).multiply(priceRef);
        if (notional.compareTo(ZERO) > 0 && leverageRef > 0) {
            return notional.divide(BigDecimal.valueOf(leverageRef), DEFAULT_CALC_SCALE, RoundingMode.HALF_UP);
        }

        return abs(originOperation.getSize());
    }

    private int resolveSourceLeverageReference(MetricaWalletDto walletMetric) {
        if (walletMetric != null && walletMetric.getExposureAndCapacity() != null) {
            final Integer leverageMedian = walletMetric.getExposureAndCapacity().getLeverageMedian();
            if (leverageMedian != null && leverageMedian > 0) {
                return leverageMedian;
            }
        }
        return USER_LEVERAGE_MIN;
    }

    private BigDecimal safeMetricDecimal(Double value) {
        if (value == null || !Double.isFinite(value) || value <= 0) {
            return ZERO;
        }
        return BigDecimal.valueOf(value);
    }

    private long elapsedMsSince(long startedNs) {
        return (System.nanoTime() - startedNs) / 1_000_000L;
    }

    private boolean shouldLogStacktrace(Throwable ex) {
        return !(ex instanceof EngineException);
    }

    private String safeLog(String s) {
        return LogFmt.sanitize(s == null ? "" : s);
    }
}
