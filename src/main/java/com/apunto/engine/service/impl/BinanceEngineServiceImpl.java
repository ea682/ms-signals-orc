package com.apunto.engine.service.impl;

import com.apunto.engine.dto.CopyOperationDto;
import com.apunto.engine.dto.FuturesPositionDto;
import com.apunto.engine.dto.OriginBasketPositionDto;
import com.apunto.engine.dto.OperacionDto;
import com.apunto.engine.dto.OperationDto;
import com.apunto.engine.dto.UserDetailDto;
import com.apunto.engine.dto.client.BinanceFuturesOrderClientResponse;
import com.apunto.engine.dto.client.BinanceFuturesSymbolFilterDto;
import com.apunto.engine.dto.client.BinanceFuturesSymbolInfoClientDto;
import com.apunto.engine.dto.client.MetricaWalletDto;
import com.apunto.engine.events.OperacionEvent;
import com.apunto.engine.mapper.CopyTradingMapper;
import com.apunto.engine.service.BinanceCopyExecutionService;
import com.apunto.engine.service.BinanceEngineService;
import com.apunto.engine.service.CopyOperationService;
import com.apunto.engine.service.DistributedLockService;
import com.apunto.engine.service.FuturesPositionService;
import com.apunto.engine.service.MetricWalletService;
import com.apunto.engine.service.ProcesBinanceService;
import com.apunto.engine.shared.enums.OrderType;
import com.apunto.engine.shared.enums.PositionSide;
import com.apunto.engine.shared.enums.PositionStatus;
import com.apunto.engine.shared.enums.Side;
import com.apunto.engine.shared.exception.EngineException;
import com.apunto.engine.shared.exception.ErrorCode;
import com.apunto.engine.shared.exception.SkipExecutionException;
import com.apunto.engine.shared.util.LogFmt;
import com.apunto.engine.shared.util.IdempotencyKeyUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class BinanceEngineServiceImpl implements BinanceEngineService, BinanceCopyExecutionService {

    private static final double DEFAULT_BASE_CAPITAL = 1_000.0;
    private static final BigDecimal MIN_NOTIONAL_FALLBACK = new BigDecimal("7");
    private static final BigDecimal USER_MIN_NOTIONAL_USDT = new BigDecimal("7");
    private static final BigDecimal ZERO = BigDecimal.ZERO;
    private static final BigDecimal REBALANCE_TOLERANCE_PCT = new BigDecimal("0.02");

    private static final String SYMBOLS_API_KEY = "1llJ9n3dloLfy0MoYLnQbiPxfvWmxS4CyyqUo1otzEWO56BLUW3Ij9dbcepqHAWb";
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
    private static final String LOG_PREP_SKIP_TOO_SMALL = "event=operation.open.skip_too_small originId={} userId={} wallet={} symbol={} notionalMax={}";
    private static final String LOG_PREP_SKIP_SYMBOL_RULES = "event=operation.open.skip_symbol_rules originId={} userId={} wallet={} symbol={} notionalMax={}";
    private static final String LOG_PREP_SKIP_NOTIONAL_ZERO = "event=operation.open.skip_notional_zero originId={} userId={} wallet={} symbol={}";
    private static final String LOG_PREP_PREPARED = "event=operation.open.prepared originId={} userId={} wallet={} symbol={} sourceMargin={} capitalReference={} fraction={} walletBudget={} usedMargin={} availableBufferedMargin={} marginThisTrade={} leverage={} notionalFinal={} marginRequired={}";
    private static final BigDecimal MAX_MARGIN_SAFETY_PCT = new BigDecimal("0.30");

    private static final String LOG_QTY_ADJUSTED = "event=binance.qty_adjusted symbol={} qtyOriginal={} qtyFinal={} notionalFinal={} notionalMax={}";
    private static final String LOG_SYMBOLS_CACHE_REFRESH = "event=binance.symbols_cache.refresh size={} ttlMs={}";

    private static final String LOG_COPY_CREATE_INVALID = "event=copyop.create.invalid_order idUser={} idWallet={} idOperation={}";
    private static final String LOG_CLOSE_INVALID_RESPONSE = "event=binance.close.invalid_response originId={} userId={} symbol={}";
    private static final String LOG_CLOSE_OK = "event=binance.close.ok originId={} userId={} symbol={} qty={} orderId={}";
    private static final String LOG_CLOSE_SKIPPED_ORIGIN_STILL_VALIDATION_ACTIVE = "event=copy_close_skipped reason=origin_still_active originId={} userId={} originActive={}";
    private static final String LOG_OPEN_START = "event=copy_open_start originId={} userId={}";
    private static final String LOG_OPEN_VALIDATION_OK = "event=copy_open_ok originId={} userId={}";

    private static final int USER_LEVERAGE_MIN = 1;
    private static final int USER_LEVERAGE_MAX = 20;

    private static final List<String> KNOWN_QUOTES = List.of(
            "USDT", "USDC", "FDUSD", "BUSD", "BTC", "ETH"
    );

    private static final Pattern LEADING_MULTIPLIER_PATTERN =
            Pattern.compile("^(\\d+)([A-Z0-9]+)$");

    private final ProcesBinanceService procesBinanceService;
    private final ThreadPoolTaskScheduler binanceTaskScheduler;
    private final MetricWalletService metricWalletService;
    private final CopyOperationService copyOperationService;
    private final DistributedLockService distributedLockService;

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

    @Value("${engine.copy.sizing.max-fraction-per-trade:0.50}")
    private BigDecimal maxFractionPerTrade;

    @Value("${engine.copy.sizing.notional-buffer-pct:0.03}")
    private BigDecimal notionalBufferPct;

    private final CopyTradingMapper copyTradingMapper;
    private final FuturesPositionService futuresPositionService;

    private final ConcurrentMap<String, Long> processedOperations = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, BigDecimal> usedMarginByUserWallet = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Reservation> reservationByCopyKey = new ConcurrentHashMap<>();

    private final Object symbolsCacheLock = new Object();
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

        if (!isNew[0]) {
            log.warn(LOG_OPEN_DUPLICATE_INMEM, originId);
            return;
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
            if (alreadyCopiedUsers.contains(userId)) {
                log.info(LOG_OPEN_SKIP_ALREADY, originId, userId);
                continue;
            }

            final long delay = (long) scheduled * delayBetweenMs;
            final Instant executionTime = Instant.ofEpochMilli(baseTime + delay);
            binanceTaskScheduler.schedule(() -> executeOpenForUser(event, userDetail), executionTime);
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

        final Map<String, CopyOperationDto> copyByUser = copyOperationService.findOperationsByOrigin(originId)
                .stream()
                .collect(Collectors.toMap(
                        CopyOperationDto::getIdUser,
                        java.util.function.Function.identity(),
                        (a, b) -> a
                ));

        if (copyByUser.isEmpty()) {
            log.warn(LOG_CLOSE_NO_COPIES, originId, usersDetail.size());
            return;
        }

        int scheduled = 0;
        for (UserDetailDto userDetail : usersDetail) {
            final String userId = userDetail.getUser().getId().toString();
            if (!copyByUser.containsKey(userId)) {
                log.warn(LOG_CLOSE_COPY_MISSING, originId, userId);
                continue;
            }
            final long delay = (long) scheduled * delayBetweenMs;
            final Instant executionTime = Instant.ofEpochMilli(baseTime + delay);
            binanceTaskScheduler.schedule(() -> executeCloseForUser(event, userDetail), executionTime);
            scheduled++;
        }

        log.info(LOG_CLOSE_SCHEDULED, originId, usersDetail.size(), scheduled, delayBetweenMs);
    }

    @Override
    public void executeOpenForUser(OperacionEvent event, UserDetailDto userDetail) {
        Objects.requireNonNull(event, ERR_EVENT_NULL);
        Objects.requireNonNull(userDetail, ERR_USER_NULL);

        final OperacionDto originOperation = requireOperacion(event);
        final String originId = originOperation.getIdOperacion().toString();
        final UUID userId = userDetail.getUser().getId();
        final Optional<FuturesPositionDto> positionOriginDto = futuresPositionService.getIdFuturesPosition(originId);

        log.debug(LOG_OPEN_START, originId, userId);

        if (!originOperation.isOperacionActiva()) {
            log.info("event=copy_open_skipped reason=origin_not_active originId={} userId={}", originId, userId);
            return;
        }

        final boolean staleOpen = positionOriginDto
                .map(dto -> !PositionStatus.OPEN.equals(dto.getIsActive()))
                .orElse(false);

        if (staleOpen) {
            log.info("event=copy_open_skipped reason=stale_open_event originId={} userId={}", originId, userId);
            return;
        }

        executeDirectOpenForUser(event, userDetail);

        log.debug(LOG_OPEN_VALIDATION_OK, originId, userId);
    }

    @Override
    public void executeCloseForUser(OperacionEvent event, UserDetailDto userDetail) {
        Objects.requireNonNull(event, ERR_EVENT_NULL);
        Objects.requireNonNull(userDetail, ERR_USER_NULL);

        final OperacionDto originOperation = requireOperacion(event);
        final String originId = originOperation.getIdOperacion().toString();
        final UUID userId = userDetail.getUser().getId();

        final Optional<FuturesPositionDto> positionOriginDto = futuresPositionService.getIdFuturesPosition(originId);
        final boolean originStillActiveInEvent = originOperation.isOperacionActiva();
        final boolean originStillOpenInPosition = positionOriginDto
                .map(dto -> PositionStatus.OPEN.equals(dto.getIsActive()))
                .orElse(false);

        if (originStillActiveInEvent) {
            log.warn(
                    "event=copy_close_skipped reason=origin_still_active originId={} userId={} originActive={} dbStatusStillOpen={}",
                    originId,
                    userId,
                    originStillActiveInEvent,
                    originStillOpenInPosition
            );
            return;
        }

        if (originStillOpenInPosition) {
            log.info("event=copy_close_db_lag originId={} userId={} dbStatusStillOpen=true", originId, userId);
        }

        executeDirectCloseForUser(event, userDetail);
    }

    private void executeDirectOpenForUser(OperacionEvent event, UserDetailDto userDetail) {
        final OperacionDto originOperation = requireOperacion(event);
        final String originId = originOperation.getIdOperacion().toString();
        final String userId = userDetail.getUser().getId().toString();
        final String walletId = originOperation.getIdCuenta();

        if (walletId == null || walletId.isBlank()) {
            throw new SkipExecutionException("wallet_missing La operación origen no trae idCuenta/wallet");
        }

        distributedLockService.withLock(distLockKey(userId, walletId), Duration.ofSeconds(120), () -> {
            final CopyOperationDto existingCopy = copyOperationService.findOperationForUser(originId, userId);
            if (existingCopy != null) {
                final String reason = existingCopy.isActive() ? "copy_already_active" : "copy_already_recorded";
                log.info("event=copy_open_skipped reason={} originId={} userId={} wallet={} active={}",
                        reason,
                        originId,
                        userId,
                        walletId,
                        existingCopy.isActive());
                return null;
            }

            final MetricaWalletDto walletMetric = resolveWalletMetric(walletId, userDetail);
            if (walletMetric == null) {
                log.info("event=copy_open_skipped reason=metric_missing originId={} userId={} wallet={}",
                        originId,
                        userId,
                        walletId);
                return null;
            }

            if (!passesWalletFilters(walletMetric)) {
                log.info("event=copy_open_skipped reason=wallet_filtered originId={} userId={} wallet={}",
                        originId,
                        userId,
                        walletId);
                return null;
            }

            final PreparedOpen prepared = prepareOpenOperation(event, userDetail, walletMetric);
            BinanceFuturesOrderClientResponse order = null;
            try {
                order = procesBinanceService.operationPosition(prepared.dto);
                createNewOperation(order, walletId, originOperation.getIdOperacion(), userId, prepared.leverage);
                log.info("event=copy_open_completed originId={} userId={} wallet={} symbol={} orderId={} qty={} notional={}",
                        originId,
                        userId,
                        walletId,
                        prepared.symbol,
                        order.getOrderId(),
                        safeQty(copyTradingMapper.resolveFilledQty(order)).toPlainString(),
                        prepared.notional.toPlainString());
                return null;
            } catch (RuntimeException ex) {
                if (order != null) {
                    try {
                        tryPanicCloseAfterPersistFailure(originId, userId, walletId, prepared, order, userDetail);
                    } catch (Exception panicEx) {
                        log.error("event=copy_open_panic_close_failed originId={} userId={} wallet={} symbol={} panicErrClass={} panicErr=\"{}\"",
                                originId,
                                userId,
                                walletId,
                                prepared.symbol,
                                panicEx.getClass().getSimpleName(),
                                safeLog(panicEx.getMessage()),
                                panicEx);
                    }
                }
                throw ex;
            }
        });
    }

    private void executeDirectCloseForUser(OperacionEvent event, UserDetailDto userDetail) {
        final OperacionDto originOperation = requireOperacion(event);
        final String originId = originOperation.getIdOperacion().toString();
        final String userId = userDetail.getUser().getId().toString();
        final String walletId = originOperation.getIdCuenta();

        if (walletId == null || walletId.isBlank()) {
            throw new SkipExecutionException("wallet_missing La operación origen no trae idCuenta/wallet");
        }

        distributedLockService.withLock(distLockKey(userId, walletId), Duration.ofSeconds(120), () -> {
            final CopyOperationDto currentCopy = copyOperationService.findOperationForUser(originId, userId);
            if (currentCopy == null) {
                log.info("event=copy_close_skipped reason=copy_missing originId={} userId={} wallet={}",
                        originId,
                        userId,
                        walletId);
                return null;
            }

            if (!currentCopy.isActive()) {
                log.info("event=copy_close_skipped reason=copy_already_inactive originId={} userId={} wallet={}",
                        originId,
                        userId,
                        walletId);
                return null;
            }

            executeCloseOperation(currentCopy, userDetail);
            log.info("event=copy_close_completed originId={} userId={} wallet={} symbol={} qty={}",
                    originId,
                    userId,
                    walletId,
                    currentCopy.getParsymbol(),
                    safeQty(currentCopy.getSizePar()).toPlainString());
            return null;
        });
    }

    private OperacionDto requireOperacion(OperacionEvent event) {
        Objects.requireNonNull(event, ERR_EVENT_NULL);

        final OperacionDto operacion = event.getOperacion();
        if (operacion == null) {
            throw new SkipExecutionException("operation_event_without_operacion");
        }
        return operacion;
    }

    private void reconcileWalletBasketForUser(OperacionEvent event, UserDetailDto userDetail) {
        final OperacionDto originOperation = requireOperacion(event);
        final String triggerOriginId = originOperation.getIdOperacion().toString();
        final String walletId = originOperation.getIdCuenta();
        final String userId = userDetail.getUser().getId().toString();

        if (walletId == null || walletId.isBlank()) {
            throw new SkipExecutionException("wallet_missing La operación origen no trae idCuenta/wallet");
        }

        distributedLockService.withLock(distLockKey(userId, walletId), Duration.ofSeconds(120), () -> {
            final MetricaWalletDto walletMetric = resolveWalletMetric(walletId, userDetail);
            final BigDecimal walletBudget = resolveWalletBudget(userDetail, walletMetric);
            final int leverage = resolveUserLeverage(userDetail);

            final List<OriginBasketPositionDto> sourceBasket = patchSourceBasket(
                    futuresPositionService.getOpenBasketByWallet(walletId),
                    originOperation
            );
            final Set<String> sourceOriginIds = sourceBasket.stream()
                    .map(OriginBasketPositionDto::getOriginId)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toSet());

            final Map<String, TargetLeg> targets = buildTargetBasket(sourceBasket, walletBudget, walletMetric, leverage);
            final Map<String, CopyOperationDto> current = copyOperationService
                    .findActiveOperationsByUserAndWallet(userId, walletId)
                    .stream()
                    .filter(Objects::nonNull)
                    .collect(Collectors.toMap(CopyOperationDto::getIdOrderOrigin, java.util.function.Function.identity(), (a, b) -> a, HashMap::new));

            for (CopyOperationDto copy : new ArrayList<>(current.values())) {
                if (copy == null) continue;
                final String copyOriginId = copy.getIdOrderOrigin();
                if (copyOriginId == null) continue;

                if (!sourceOriginIds.contains(copyOriginId)) {
                    executeFullClose(copy, userDetail);
                    current.remove(copyOriginId);
                    continue;
                }

                final TargetLeg target = targets.get(copyOriginId);
                if (target == null) {
                    continue;
                }

                final BigDecimal currentQty = safeQty(copy.getSizePar());
                final BigDecimal targetQty = safeQty(target.targetQty());
                if (shouldReduce(currentQty, targetQty)) {
                    if (targetQty.compareTo(ZERO) <= 0) {
                        executeFullClose(copy, userDetail);
                        current.remove(copyOriginId);
                    } else {
                        CopyOperationDto updated = executePartialReduce(copy, target, triggerOriginId, userDetail);
                        if (updated == null || !updated.isActive()) {
                            current.remove(copyOriginId);
                        } else {
                            current.put(copyOriginId, updated);
                        }
                    }
                }
            }

            BigDecimal usedMargin = sumBufferedMargin(current.values());
            final BigDecimal hardCap = walletBudget.multiply(BigDecimal.ONE.add(walletHardcapOverPct == null ? ZERO : walletHardcapOverPct));
            final BigDecimal reserve = walletBudget.multiply(walletReservePct == null ? ZERO : walletReservePct);

            for (TargetLeg target : targets.values()) {
                if (target == null || safeQty(target.targetQty()).compareTo(ZERO) <= 0) {
                    continue;
                }

                final CopyOperationDto currentCopy = current.get(target.originId());
                if (currentCopy == null) {
                    final BigDecimal required = computeBufferedMargin(target.targetQty(), target.priceRef(), target.leverage());
                    if (walletBudget.compareTo(ZERO) <= 0 || usedMargin.add(required).add(reserve).compareTo(hardCap) > 0) {
                        log.warn(LOG_OPEN_SKIP_BUDGET, target.originId(), userId, walletId, target.symbol(), required.toPlainString(), walletBudget.toPlainString(), usedMargin.toPlainString());
                        continue;
                    }
                    CopyOperationDto created = executeOpenTarget(target, userDetail);
                    current.put(target.originId(), created);
                    usedMargin = usedMargin.add(computeCopyBufferedMargin(created));
                    continue;
                }

                final BigDecimal currentQty = safeQty(currentCopy.getSizePar());
                final BigDecimal targetQty = safeQty(target.targetQty());
                if (shouldIncrease(currentQty, targetQty)) {
                    final BigDecimal deltaQty = targetQty.subtract(currentQty);
                    final BigDecimal required = computeBufferedMargin(deltaQty, target.priceRef(), target.leverage());
                    if (walletBudget.compareTo(ZERO) <= 0 || usedMargin.add(required).add(reserve).compareTo(hardCap) > 0) {
                        log.warn(LOG_OPEN_SKIP_BUDGET, target.originId(), userId, walletId, target.symbol(), required.toPlainString(), walletBudget.toPlainString(), usedMargin.toPlainString());
                        continue;
                    }
                    CopyOperationDto updated = executeIncrease(currentCopy, target, triggerOriginId, userDetail);
                    current.put(target.originId(), updated);
                    usedMargin = sumBufferedMargin(current.values());
                }
            }
            return null;
        });
    }

    private MetricaWalletDto resolveWalletMetric(String walletId, UserDetailDto userDetail) {
        final List<MetricaWalletDto> metrics = normalizeCapitalShares(metricWalletService.getCandidatesUser(userDetail.getUser().getId()));
        return getWalletMetricForOperation(walletId, metrics);
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
        return Math.min(USER_LEVERAGE_MAX, Math.max(USER_LEVERAGE_MIN, userDetail.getDetail().getLeverage()));
    }

    private List<OriginBasketPositionDto> patchSourceBasket(List<OriginBasketPositionDto> currentBasket, OperacionDto originOperation) {
        final Map<String, OriginBasketPositionDto> byOriginId = new HashMap<>();
        if (currentBasket != null) {
            for (OriginBasketPositionDto p : currentBasket) {
                if (p == null || p.getOriginId() == null) continue;
                byOriginId.put(p.getOriginId(), p);
            }
        }

        final String originId = originOperation.getIdOperacion().toString();
        if (originOperation.isOperacionActiva()) {
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
                    .build());
        } else {
            byOriginId.remove(originId);
        }
        return new ArrayList<>(byOriginId.values());
    }

    private Map<String, TargetLeg> buildTargetBasket(List<OriginBasketPositionDto> sourceBasket,
                                                     BigDecimal walletBudget,
                                                     MetricaWalletDto walletMetric,
                                                     int leverage) {
        if (sourceBasket == null || sourceBasket.isEmpty()) {
            return Map.of();
        }

        final Map<String, BigDecimal> bases = new HashMap<>();
        BigDecimal totalBase = ZERO;
        for (OriginBasketPositionDto leg : sourceBasket) {
            final BigDecimal base = computeSourceWeightBase(leg);
            if (leg == null || leg.getOriginId() == null || base.compareTo(ZERO) <= 0) {
                continue;
            }
            bases.put(leg.getOriginId(), base);
            totalBase = totalBase.add(base);
        }

        if (totalBase.compareTo(ZERO) <= 0) {
            return Map.of();
        }

        final BigDecimal sourceUtilization = computeSourceUtilization(sourceBasket, walletMetric);
        final BigDecimal allocatableMargin = walletBudget.compareTo(ZERO) <= 0
                ? ZERO
                : walletBudget.multiply(sourceUtilization);

        final Map<String, TargetLeg> result = new HashMap<>();
        for (OriginBasketPositionDto leg : sourceBasket) {
            if (leg == null || leg.getOriginId() == null || !bases.containsKey(leg.getOriginId())) {
                continue;
            }
            try {
                final String symbol = resolveCanonicalSymbol(leg.getSymbol());
                final BigDecimal priceRef = resolvePriceRef(leg.getMarkPrice(), leg.getEntryPrice());
                if (priceRef.compareTo(ZERO) <= 0 || leg.getSide() == null) {
                    continue;
                }

                final BinanceFuturesSymbolInfoClientDto symbolInfo = getSymbolsBySymbol().get(symbol);
                final SymbolRules rules = extractRules(symbolInfo);
                if (symbolInfo == null || rules == null) {
                    continue;
                }

                final BigDecimal weight = bases.get(leg.getOriginId())
                        .divide(totalBase, DEFAULT_CALC_SCALE, RoundingMode.HALF_UP);

                final BigDecimal targetMargin = allocatableMargin.multiply(weight);
                final BigDecimal notionalMax = targetMargin.multiply(BigDecimal.valueOf(leverage));

                final BigDecimal buf = safePct(notionalBufferPct, new BigDecimal("0.30"));
                final BigDecimal targetNotional = notionalMax.multiply(BigDecimal.ONE.subtract(buf));

                BigDecimal rawQty = targetNotional.compareTo(ZERO) <= 0
                        ? ZERO
                        : targetNotional.divide(priceRef, rules.qtyScale, RoundingMode.DOWN);

                BigDecimal targetQty = adjustQuantityToBinanceRules(symbol, rawQty, priceRef, rules, notionalMax);
                if (targetQty == null) {
                    targetQty = ZERO;
                }

                BigDecimal targetNotionalFinal = targetQty.multiply(priceRef);
                if (targetQty.compareTo(ZERO) > 0
                        && rules.minNotional != null
                        && targetNotionalFinal.compareTo(rules.minNotional) < 0) {
                    throw new SkipExecutionException(
                            "notional_too_small",
                            "Target descartado porque el notional final quedó bajo el mínimo de Binance",
                            LogFmt.kv(
                                    "rawSymbol", leg.getSymbol(),
                                    "symbol", symbol,
                                    "targetQty", targetQty,
                                    "priceRef", priceRef,
                                    "targetNotional", targetNotionalFinal,
                                    "minNotional", rules.minNotional
                            )
                    );
                }

                result.put(leg.getOriginId(), new TargetLeg(
                        leg.getOriginId(),
                        leg.getWalletId(),
                        symbol,
                        leg.getSide(),
                        leverage,
                        priceRef,
                        targetQty,
                        targetNotionalFinal
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
            } catch (Exception ex) {
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

    private BigDecimal computeSourceWeightBase(OriginBasketPositionDto leg) {
        if (leg == null) return ZERO;

        BigDecimal v = abs(leg.getNotionalUsd());
        if (v.compareTo(ZERO) > 0) return v;

        final BigDecimal price = resolvePriceRef(leg.getMarkPrice(), leg.getEntryPrice());
        v = safeQty(leg.getSizeQty()).multiply(price);
        if (v.compareTo(ZERO) > 0) return v;

        v = abs(leg.getSizeLegacy());
        if (v.compareTo(ZERO) > 0) return v;

        return abs(leg.getMarginUsedUsd());
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

    private CopyOperationDto executeOpenTarget(TargetLeg target, UserDetailDto userDetail) {
        final OperationDto dto = buildOpenOrIncreaseOrder(target, target.targetQty(), userDetail,
                IdempotencyKeyUtil.openClientOrderId(target.originId(), userDetail.getUser().getId().toString(), target.walletId()));
        final BinanceFuturesOrderClientResponse response = procesBinanceService.operationPosition(dto);
        if (!isValidOrderResponse(response)) {
            throw new EngineException(ErrorCode.EXTERNAL_SERVICE_ERROR, "Respuesta inválida de Binance");
        }
        final CopyOperationDto created = copyTradingMapper.buildCopyNewOperationDto(
                response,
                target.walletId(),
                UUID.fromString(target.originId()),
                userDetail.getUser().getId().toString(),
                target.leverage()
        );
        copyOperationService.newOperation(created);
        return created;
    }

    private CopyOperationDto executeIncrease(CopyOperationDto currentCopy, TargetLeg target, String triggerOriginId, UserDetailDto userDetail) {
        final BigDecimal currentQty = safeQty(currentCopy.getSizePar());
        final BigDecimal deltaQty = target.targetQty().subtract(currentQty);
        final OperationDto dto = buildOpenOrIncreaseOrder(
                target,
                deltaQty,
                userDetail,
                IdempotencyKeyUtil.rebalanceIncreaseClientOrderId(triggerOriginId, target.originId(), userDetail.getUser().getId().toString(), target.walletId(), target.targetQty().stripTrailingZeros().toPlainString())
        );
        final BinanceFuturesOrderClientResponse response = procesBinanceService.operationPosition(dto);
        if (!isValidOrderResponse(response)) {
            throw new EngineException(ErrorCode.EXTERNAL_SERVICE_ERROR, "Respuesta inválida de Binance");
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
        copyOperationService.upsertActiveOperation(updated);
        return updated;
    }

    private CopyOperationDto executePartialReduce(CopyOperationDto currentCopy, TargetLeg target, String triggerOriginId, UserDetailDto userDetail) {
        final BigDecimal currentQty = safeQty(currentCopy.getSizePar());
        final BigDecimal targetQty = target.targetQty();
        final BigDecimal deltaQty = currentQty.subtract(targetQty);
        final OperationDto dto = buildReduceOrder(
                target,
                deltaQty,
                userDetail,
                IdempotencyKeyUtil.rebalanceReduceClientOrderId(triggerOriginId, target.originId(), userDetail.getUser().getId().toString(), target.walletId(), targetQty.stripTrailingZeros().toPlainString())
        );
        final BinanceFuturesOrderClientResponse response = procesBinanceService.operationPosition(dto);
        if (!isValidOrderResponse(response)) {
            throw new EngineException(ErrorCode.EXTERNAL_SERVICE_ERROR, "Respuesta inválida de Binance");
        }
        final BigDecimal filledQty = safeQty(copyTradingMapper.resolveFilledQty(response));
        if (filledQty.compareTo(ZERO) <= 0) {
            return currentCopy;
        }
        final BigDecimal remainingQty = currentQty.subtract(filledQty);
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
                    .dateClose(OffsetDateTime.now())
                    .active(false)
                    .build();
            copyOperationService.closeOperation(closed);
            return closed;
        }

        final BigDecimal currentUsd = safeUsd(currentCopy.getSiseUsd(), currentQty, currentCopy.getPriceEntry());
        final BigDecimal unitUsd = currentQty.compareTo(ZERO) <= 0 ? ZERO : currentUsd.divide(currentQty, DEFAULT_CALC_SCALE, RoundingMode.HALF_UP);
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
        copyOperationService.upsertActiveOperation(updated);
        return updated;
    }

    private void executeFullClose(CopyOperationDto copyOperation, UserDetailDto userDetail) {
        executeCloseOperation(copyOperation, userDetail);
    }

    private OperationDto buildOpenOrIncreaseOrder(TargetLeg target, BigDecimal quantity, UserDetailDto userDetail, String clientOrderId) {
        final Side orderSide = target.side() == PositionSide.LONG ? Side.BUY : Side.SELL;
        return OperationDto.builder()
                .symbol(target.symbol())
                .side(orderSide)
                .type(OrderType.MARKET)
                .positionSide(target.side())
                .quantity(quantity.toPlainString())
                .leverage(target.leverage())
                .reduceOnly(false)
                .clientOrderId(clientOrderId)
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
                .leverage(target.leverage())
                .reduceOnly(true)
                .clientOrderId(clientOrderId)
                .apiKey(userDetail.getUserApiKey().getApiKey())
                .secret(userDetail.getUserApiKey().getApiSecret())
                .build();
    }

    private BigDecimal computeBufferedMargin(BigDecimal qty, BigDecimal price, int leverage) {
        if (qty == null || price == null || leverage <= 0) return ZERO;
        final BigDecimal notional = qty.multiply(price);
        final BigDecimal margin = notional.divide(BigDecimal.valueOf(leverage), DEFAULT_CALC_SCALE, RoundingMode.HALF_UP);
        return margin.multiply(BigDecimal.ONE.add(safePct(marginSafetyBufferPct, MAX_MARGIN_SAFETY_PCT)));
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
        final BigDecimal lev = op.getLeverage() == null || op.getLeverage().compareTo(ZERO) <= 0 ? BigDecimal.ONE : op.getLeverage();
        final BigDecimal margin = usd.divide(lev, DEFAULT_CALC_SCALE, RoundingMode.HALF_UP);
        return margin.multiply(BigDecimal.ONE.add(safePct(marginSafetyBufferPct, MAX_MARGIN_SAFETY_PCT)));
    }

    private BigDecimal safeUsd(BigDecimal explicitUsd, BigDecimal qty, BigDecimal entryPrice) {
        if (explicitUsd != null && explicitUsd.compareTo(ZERO) > 0) return explicitUsd;
        if (qty != null && entryPrice != null) return qty.multiply(entryPrice);
        return ZERO;
    }

    private BigDecimal safeQty(BigDecimal qty) {
        return qty == null || qty.compareTo(ZERO) <= 0 ? ZERO : qty;
    }

    private BigDecimal abs(BigDecimal v) {
        return v == null ? ZERO : v.abs();
    }

    private void tryPanicCloseAfterPersistFailure(String originId, String userId, String walletId,
                                                  PreparedOpen prepared,
                                                  BinanceFuturesOrderClientResponse openResponse,
                                                  UserDetailDto userDetail) {
        if (prepared == null || userDetail == null) {
            return;
        }

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
            } catch (Exception ignored) {
            }
        }
        if (qty == null || qty.compareTo(ZERO) <= 0) {
            log.warn("event=panic_close.skip reason=qty_zero symbol={}", prepared.symbol);
            return;
        }

        final Side closeSide = prepared.dto.getSide() == Side.BUY ? Side.SELL : Side.BUY;

        final OperationDto close = OperationDto.builder()
                .symbol(prepared.dto.getSymbol())
                .side(closeSide)
                .type(OrderType.MARKET)
                .positionSide(prepared.dto.getPositionSide())
                .quantity(qty.toPlainString())
                .leverage(prepared.leverage)
                .reduceOnly(true)
                .clientOrderId(IdempotencyKeyUtil.closeClientOrderId(originId, userId, walletId))
                .apiKey(userDetail.getUserApiKey().getApiKey())
                .secret(userDetail.getUserApiKey().getApiSecret())
                .build();

        final BinanceFuturesOrderClientResponse closeResp = procesBinanceService.operationPosition(close);
        if (!isValidOrderResponse(closeResp)) {
            throw new EngineException(ErrorCode.EXTERNAL_SERVICE_ERROR, "Panic close inválido");
        }

        log.warn("event=panic_close.ok symbol={} qty={} orderId={}",
                prepared.dto.getSymbol(), qty.toPlainString(), closeResp.getOrderId());
    }

    private void executeCloseOperation(CopyOperationDto copyOperation, UserDetailDto userDetail) {
        final String originId = copyOperation.getIdOrderOrigin();
        final String userId = userDetail.getUser().getId().toString();

        final BinanceFuturesOrderClientResponse order =
                procesBinanceService.operationPosition(copyTradingMapper.buildClosePosition(copyOperation, userDetail));

        if (!isValidOrderResponse(order)) {
            log.warn(LOG_CLOSE_INVALID_RESPONSE, originId, userId, copyOperation.getParsymbol());
            throw new EngineException(ErrorCode.EXTERNAL_SERVICE_ERROR, "Respuesta inválida de Binance");
        }

        final CopyOperationDto buildCopyOperation = copyTradingMapper.buildCopyCloseOperationDto(copyOperation, order);
        copyOperationService.closeOperation(buildCopyOperation);

        log.info(LOG_CLOSE_OK,
                originId,
                userId,
                copyOperation.getParsymbol(),
                copyOperation.getSizePar(),
                order.getOrderId());
    }

    private PreparedOpen prepareOpenOperation(OperacionEvent event,
                                              UserDetailDto userDetail,
                                              MetricaWalletDto walletMetric) {

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

        BigDecimal fractionOfBaseUsed = sourceMargin.divide(capitalReference, DEFAULT_CALC_SCALE, RoundingMode.HALF_UP);
        if (fractionOfBaseUsed.compareTo(BigDecimal.ONE) > 0) {
            fractionOfBaseUsed = BigDecimal.ONE;
        }
        if (maxFractionPerTrade != null && maxFractionPerTrade.compareTo(ZERO) > 0) {
            fractionOfBaseUsed = fractionOfBaseUsed.min(maxFractionPerTrade);
        }
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

        final BigDecimal entryPrice = originOperation.getPrecioEntrada();
        if (entryPrice == null || entryPrice.compareTo(ZERO) <= 0) {
            log.warn(LOG_PREP_INVALID_ENTRY, originId, userId, walletId);
            throw new SkipExecutionException(
                    "entry_price_invalid",
                    "Precio de entrada inválido (<=0)",
                    com.apunto.engine.shared.util.LogFmt.kv("wallet", walletId, "entryPrice", entryPrice)
            );
        }

        final String rawSymbol = originOperation.getParSymbol();
        if (rawSymbol == null || rawSymbol.isBlank()) {
            log.warn(LOG_PREP_INVALID_SYMBOL, originId, userId, walletId);
            throw new SkipExecutionException(
                    "symbol_blank",
                    "Símbolo vacío/null",
                    com.apunto.engine.shared.util.LogFmt.kv("wallet", walletId, "symbol", rawSymbol)
            );
        }

        final String symbol = resolveCanonicalSymbol(rawSymbol);
        final String normalizedRawSymbol = normalizeSymbolKey(rawSymbol);
        if (!symbol.equals(normalizedRawSymbol)) {
            log.info("event=symbol.resolved originId={} userId={} wallet={} rawSymbol={} canonicalSymbol={}",
                    originId, userId, walletId, rawSymbol, symbol);
        }

        final int leverage = Math.min(
                USER_LEVERAGE_MAX,
                Math.max(USER_LEVERAGE_MIN, userDetail.getDetail().getLeverage())
        );

        final BigDecimal safety = safePct(marginSafetyBufferPct, MAX_MARGIN_SAFETY_PCT);
        final BigDecimal usedMargin = copyOperationService.sumBufferedMarginActive(userId, walletId, safety);
        final BigDecimal reserve = walletMarginBudget.multiply(safePct(walletReservePct, BigDecimal.ONE));
        final BigDecimal hardCap = walletMarginBudget.multiply(BigDecimal.ONE.add(safePct(walletHardcapOverPct, BigDecimal.ONE)));
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
                .divide(BigDecimal.ONE.add(safety), DEFAULT_CALC_SCALE, RoundingMode.HALF_UP)
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

        final BigDecimal notionalMax = marginThisTrade.multiply(BigDecimal.valueOf(leverage));
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
        final BigDecimal targetNotional = notionalMax.multiply(BigDecimal.ONE.subtract(buf));

        final BinanceFuturesSymbolInfoClientDto symbolInfo = getSymbolsBySymbol().get(symbol);
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
        if (rules == null || rules.minNotional == null) {
            log.warn(LOG_PREP_SKIP_SYMBOL_RULES, originId, userId, walletId, symbol, targetNotional.toPlainString());
            throw new SkipExecutionException(
                    "symbol_rules_invalid",
                    "Reglas de Binance inválidas/incompletas (minNotional null)",
                    com.apunto.engine.shared.util.LogFmt.kv(
                            "wallet", walletId,
                            "symbol", symbol,
                            "targetNotional", targetNotional
                    )
            );
        }

        if (targetNotional.compareTo(rules.minNotional) < 0) {
            log.info(LOG_PREP_SKIP_TOO_SMALL, originId, userId, walletId, symbol, targetNotional.toPlainString());
            final BigDecimal missing = rules.minNotional.subtract(targetNotional);
            throw new SkipExecutionException(
                    "notional_too_small",
                    "Operación demasiado pequeña: faltan " + missing.max(ZERO).toPlainString()
                            + " de notional para minNotional=" + rules.minNotional.toPlainString(),
                    com.apunto.engine.shared.util.LogFmt.kv(
                            "wallet", walletId,
                            "symbol", symbol,
                            "candidateNotional", targetNotional,
                            "minNotional", rules.minNotional,
                            "missingNotional", missing,
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
        }

        BigDecimal quantity = targetNotional.divide(entryPrice, rules.qtyScale, RoundingMode.DOWN);
        if (quantity.compareTo(ZERO) <= 0) {
            log.info(LOG_PREP_SKIP_TOO_SMALL, originId, userId, walletId, symbol, targetNotional.toPlainString());
            throw new SkipExecutionException(
                    "qty_zero",
                    "Cantidad calculada es 0 (por escala/rounding)",
                    com.apunto.engine.shared.util.LogFmt.kv(
                            "wallet", walletId,
                            "symbol", symbol,
                            "candidateNotional", targetNotional,
                            "entryPrice", entryPrice,
                            "qtyScale", rules.qtyScale
                    )
            );
        }

        quantity = adjustQuantityToBinanceRules(symbol, quantity, entryPrice, rules, notionalMax);
        if (quantity.compareTo(ZERO) <= 0) {
            log.info(LOG_PREP_SKIP_TOO_SMALL, originId, userId, walletId, symbol, targetNotional.toPlainString());
            throw new SkipExecutionException(
                    "qty_adjusted_zero",
                    "Cantidad quedó en 0 tras aplicar reglas Binance (step/minQty/precision/budget)",
                    com.apunto.engine.shared.util.LogFmt.kv(
                            "wallet", walletId,
                            "symbol", symbol,
                            "qtyRaw", quantity,
                            "entryPrice", entryPrice,
                            "notionalMax", notionalMax,
                            "stepSize", rules.stepSize,
                            "minQty", rules.minQty,
                            "qtyScale", rules.qtyScale
                    )
            );
        }

        final BigDecimal notionalFinal = quantity.multiply(entryPrice);
        if (notionalFinal.compareTo(rules.minNotional) < 0) {
            log.info(LOG_PREP_SKIP_TOO_SMALL, originId, userId, walletId, symbol, notionalFinal.toPlainString());
            final BigDecimal missing = rules.minNotional.subtract(notionalFinal);
            throw new SkipExecutionException(
                    "notional_too_small",
                    "Operación demasiado pequeña tras ajuste: faltan " + missing.max(ZERO).toPlainString()
                            + " de notional para minNotional=" + rules.minNotional.toPlainString(),
                    com.apunto.engine.shared.util.LogFmt.kv(
                            "wallet", walletId,
                            "symbol", symbol,
                            "candidateNotional", targetNotional,
                            "notionalFinal", notionalFinal,
                            "minNotional", rules.minNotional,
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
                .multiply(BigDecimal.ONE.add(safety));

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
                marginRequired.toPlainString());

        final OperationDto dto = buildBuyAndSellPosition(symbol, event, quantity, userDetail, leverage);
        dto.setClientOrderId(IdempotencyKeyUtil.openClientOrderId(originId, userId, walletId));

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
                                                 int leverage) {

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

        if (snapshot.expiresAtMs > now && snapshot.bySymbol != null && !snapshot.bySymbol.isEmpty()) {
            return snapshot;
        }

        synchronized (symbolsCacheLock) {
            final SymbolsCache snapshot2 = symbolsCache;
            if (snapshot2.expiresAtMs > now && snapshot2.bySymbol != null && !snapshot2.bySymbol.isEmpty()) {
                return snapshot2;
            }

            final List<BinanceFuturesSymbolInfoClientDto> symbols = procesBinanceService.getSymbols(SYMBOLS_API_KEY);

            final Map<String, BinanceFuturesSymbolInfoClientDto> bySymbol = symbols == null
                    ? Collections.emptyMap()
                    : symbols.stream()
                    .filter(s -> s.getSymbol() != null && !s.getSymbol().isBlank())
                    .collect(Collectors.toMap(
                            s -> normalizeSymbolKey(s.getSymbol()),
                            s -> s,
                            (a, b) -> a
                    ));

            final Map<String, String> aliasToCanonical = new HashMap<>();
            final Set<String> ambiguousAliases = new HashSet<>();

            for (String canonical : bySymbol.keySet()) {
                registerAlias(aliasToCanonical, ambiguousAliases, canonical, canonical);
                for (String alias : deriveAliases(canonical)) {
                    registerAlias(aliasToCanonical, ambiguousAliases, alias, canonical);
                }
            }

            symbolsCache = new SymbolsCache(
                    now + symbolsCacheTtlMs,
                    bySymbol,
                    aliasToCanonical,
                    ambiguousAliases
            );
            log.debug(LOG_SYMBOLS_CACHE_REFRESH, bySymbol.size(), symbolsCacheTtlMs);

            return symbolsCache;
        }
    }

    private Map<String, BinanceFuturesSymbolInfoClientDto> getSymbolsBySymbol() {
        return getOrRefreshSymbolsCache().bySymbol;
    }

    private String resolveCanonicalSymbol(String rawSymbol) {
        if (rawSymbol == null || rawSymbol.isBlank()) {
            return rawSymbol;
        }

        final SymbolsCache cache = getOrRefreshSymbolsCache();

        for (String candidate : buildSymbolCandidates(rawSymbol)) {
            if (candidate == null || candidate.isBlank()) {
                continue;
            }

            if (cache.bySymbol.containsKey(candidate)) {
                return candidate;
            }

            if (cache.ambiguousAliases.contains(candidate)) {
                throw new SkipExecutionException(
                        "symbol_alias_ambiguous",
                        "Alias ambiguo, no se puede resolver de forma segura",
                        com.apunto.engine.shared.util.LogFmt.kv("rawSymbol", rawSymbol, "candidate", candidate)
                );
            }

            final String canonical = cache.aliasToCanonical.get(candidate);
            if (canonical != null) {
                return canonical;
            }
        }

        throw new SkipExecutionException(
                "symbol_alias_not_found",
                "No se pudo resolver el símbolo contra exchangeInfo",
                com.apunto.engine.shared.util.LogFmt.kv("rawSymbol", rawSymbol)
        );
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
                return units.compareTo(BigDecimal.ONE) == 0 ? "B" : units.toPlainString() + "B";
            }
            if (value.compareTo(million) >= 0 && value.remainder(million).compareTo(ZERO) == 0) {
                final BigDecimal units = value.divide(million).stripTrailingZeros();
                return units.compareTo(BigDecimal.ONE) == 0 ? "M" : units.toPlainString() + "M";
            }
            if (value.compareTo(thousand) >= 0 && value.remainder(thousand).compareTo(ZERO) == 0) {
                final BigDecimal units = value.divide(thousand).stripTrailingZeros();
                return units.compareTo(BigDecimal.ONE) == 0 ? "K" : units.toPlainString() + "K";
            }

            return multiplierRaw;
        } catch (Exception e) {
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

        BigDecimal minNotional = null;
        for (BinanceFuturesSymbolFilterDto f : symbolDetail.getFilters()) {
            if (f == null || f.getFilterType() == null) {
                continue;
            }

            if (FILTER_TYPE_MIN_NOTIONAL.equalsIgnoreCase(f.getFilterType())
                    || FILTER_TYPE_NOTIONAL.equalsIgnoreCase(f.getFilterType())) {
                minNotional = safeBigDecimal(f.getNotional());
                break;
            }
        }

        if (minNotional == null) {
            minNotional = MIN_NOTIONAL_FALLBACK;
        }
        if (minNotional.compareTo(MIN_NOTIONAL_FALLBACK) < 0) {
            minNotional = MIN_NOTIONAL_FALLBACK;
        }
        minNotional = minNotional.max(USER_MIN_NOTIONAL_USDT).max(MIN_NOTIONAL_FALLBACK);

        return new SymbolRules(stepSize, minQty, minNotional, finalScale);
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
            final BigDecimal targetNotional = notionalMax.multiply(BigDecimal.ONE.subtract(buf));

            BigDecimal q = targetNotional.divide(entryPrice, rules.qtyScale, RoundingMode.DOWN);

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

    private void createNewOperation(BinanceFuturesOrderClientResponse order,
                                    String idWallet,
                                    UUID idOperation,
                                    String idUser,
                                    int leverage) {

        if (!isValidOrderResponse(order)) {
            log.warn(LOG_COPY_CREATE_INVALID, idUser, idWallet, idOperation);
            throw new EngineException(ErrorCode.EXTERNAL_SERVICE_ERROR, "Respuesta inválida de Binance");
        }

        final CopyOperationDto buildCopyOperation = copyTradingMapper.buildCopyNewOperationDto(order, idWallet, idOperation, idUser, leverage);
        copyOperationService.newOperation(buildCopyOperation);
    }

    private boolean isValidOrderResponse(BinanceFuturesOrderClientResponse r) {
        return r != null
                && r.getOrderId() != null
                && r.getSymbol() != null
                && (r.getExecutedQty() != null || r.getCumQty() != null || r.getOrigQty() != null)
                && r.getAvgPrice() != null
                && r.getUpdateTime() != null;
    }

    private BigDecimal safeNotional(BinanceFuturesOrderClientResponse response, BigDecimal fallbackPrice) {
        if (response == null) {
            return ZERO;
        }

        BigDecimal qty = response.getExecutedQty();
        if (qty == null || qty.compareTo(ZERO) <= 0) {
            qty = response.getCumQty();
        }
        if (qty == null || qty.compareTo(ZERO) <= 0) {
            qty = response.getOrigQty();
        }
        if (qty == null || qty.compareTo(ZERO) <= 0) {
            return ZERO;
        }

        BigDecimal px = response.getAvgPrice();
        if (px == null || px.compareTo(ZERO) <= 0) {
            px = fallbackPrice;
        }
        if (px == null || px.compareTo(ZERO) <= 0) {
            return ZERO;
        }

        return qty.multiply(px);
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
        } catch (Exception e) {
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
                             BigDecimal targetNotional) {
    }

    private static final class Reservation {
        private final String userWalletKey;
        private final BigDecimal margin;

        private Reservation(String userWalletKey, BigDecimal margin) {
            this.userWalletKey = userWalletKey;
            this.margin = margin;
        }
    }

    private static final class SymbolRules {
        private final BigDecimal stepSize;
        private final BigDecimal minQty;
        private final BigDecimal minNotional;
        private final int qtyScale;

        private SymbolRules(BigDecimal stepSize, BigDecimal minQty, BigDecimal minNotional, int qtyScale) {
            this.stepSize = stepSize;
            this.minQty = minQty;
            this.minNotional = minNotional;
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
        final LinkedHashSet<String> candidates = new LinkedHashSet<>();

        final String key = normalizeSymbolKey(rawSymbol);
        if (key == null || key.isBlank()) {
            return List.of();
        }

        candidates.add(key);

        final String strippedVersion = stripVersionSuffix(key);
        candidates.add(strippedVersion);

        final String manualAlias = MANUAL_SYMBOL_ALIASES.get(key);
        if (manualAlias != null && !manualAlias.isBlank()) {
            candidates.add(normalizeSymbolKey(manualAlias));
        }

        final String manualAliasStripped = MANUAL_SYMBOL_ALIASES.get(strippedVersion);
        if (manualAliasStripped != null && !manualAliasStripped.isBlank()) {
            candidates.add(normalizeSymbolKey(manualAliasStripped));
        }

        addUsdToUsdtCandidates(candidates, key);
        addUsdToUsdtCandidates(candidates, strippedVersion);

        return new ArrayList<>(candidates);
    }

    private void addUsdToUsdtCandidates(Set<String> candidates, String symbol) {
        if (symbol == null || symbol.isBlank()) {
            return;
        }

        if (symbol.endsWith("USD") && !symbol.endsWith("USDT")) {
            candidates.add(symbol.substring(0, symbol.length() - 3) + "USDT");
        }
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

    private BigDecimal computeSourceUtilization(List<OriginBasketPositionDto> sourceBasket,
                                                MetricaWalletDto walletMetric) {
        if (sourceBasket == null || sourceBasket.isEmpty()) {
            return ZERO;
        }

        final BigDecimal capitalReference = resolveCapitalReference(walletMetric);
        if (capitalReference.compareTo(ZERO) <= 0) {
            return BigDecimal.ONE;
        }

        BigDecimal sourceOpenMargin = ZERO;
        for (OriginBasketPositionDto leg : sourceBasket) {
            sourceOpenMargin = sourceOpenMargin.add(computeSourceMarginBase(leg));
        }

        if (sourceOpenMargin.compareTo(ZERO) <= 0) {
            return BigDecimal.ONE;
        }

        BigDecimal utilization = sourceOpenMargin.divide(capitalReference, DEFAULT_CALC_SCALE, RoundingMode.HALF_UP);

        if (utilization.compareTo(ZERO) < 0) {
            return ZERO;
        }
        if (utilization.compareTo(BigDecimal.ONE) > 0) {
            return BigDecimal.ONE;
        }
        return utilization;
    }

    private BigDecimal computeSourceMarginBase(OriginBasketPositionDto leg) {
        if (leg == null) {
            return ZERO;
        }

        BigDecimal v = abs(leg.getMarginUsedUsd());
        if (v.compareTo(ZERO) > 0) {
            return v;
        }

        v = abs(leg.getNotionalUsd());
        if (v.compareTo(ZERO) > 0) {
            return v;
        }

        final BigDecimal price = resolvePriceRef(leg.getMarkPrice(), leg.getEntryPrice());
        v = safeQty(leg.getSizeQty()).multiply(price);
        if (v.compareTo(ZERO) > 0) {
            return v;
        }

        return abs(leg.getSizeLegacy());
    }


    private String safeLog(String s) {
        return LogFmt.sanitize(s == null ? "" : s);
    }
}
