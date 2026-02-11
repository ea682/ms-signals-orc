package com.apunto.engine.service.impl;

import com.apunto.engine.dto.CopyOperationDto;
import com.apunto.engine.dto.OperationDto;
import com.apunto.engine.dto.UserDetailDto;
import com.apunto.engine.dto.client.BinanceFuturesOrderClientResponse;
import com.apunto.engine.dto.client.BinanceFuturesSymbolFilterDto;
import com.apunto.engine.dto.client.BinanceFuturesSymbolInfoClientDto;
import com.apunto.engine.dto.client.MetricaWalletDto;
import com.apunto.engine.events.OperacionEvent;
import com.apunto.engine.mapper.CopyTradingMapper;
import com.apunto.engine.service.*;
import com.apunto.engine.shared.exception.SkipExecutionException;
import com.apunto.engine.shared.util.IdempotencyKeyUtil;
import com.apunto.engine.shared.enums.OrderType;
import com.apunto.engine.shared.enums.PositionSide;
import com.apunto.engine.shared.enums.Side;
import com.apunto.engine.shared.exception.EngineException;
import com.apunto.engine.shared.exception.ErrorCode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class BinanceEngineServiceImpl implements BinanceEngineService, BinanceCopyExecutionService {

    private static final double DEFAULT_BASE_CAPITAL = 1_000.0;
    private static final BigDecimal MIN_NOTIONAL_FALLBACK = new BigDecimal("7");
    private static final BigDecimal USER_MIN_NOTIONAL_USDT = new BigDecimal("7");
    private static final BigDecimal ZERO = BigDecimal.ZERO;

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
    private static final String LOG_OPEN_NO_METRIC = "event=operation.open.no_metric originId={} userId={} idWallet={} maxWallet={}";
    private static final String LOG_OPEN_SCHEDULED = "event=operation.open.scheduled originId={} users={} scheduled={} uniqueMaxWallets={} delayBetweenMs={}";

    private static final String LOG_CLOSE_NO_COPIES = "event=operation.close.no_copies originId={} users={}";
    private static final String LOG_CLOSE_COPY_MISSING = "event=operation.close.copy_missing originId={} userId={}";
    private static final String LOG_CLOSE_SKIP_INACTIVE = "event=operation.close.skip_inactive originId={} userId={}";
    private static final String LOG_CLOSE_SCHEDULED = "event=operation.close.scheduled originId={} users={} scheduled={} delayBetweenMs={}";
    private static final String LOG_CLOSE_ERROR = "event=operation.close.error originId={} userId={} err={}";

    private static final String LOG_OPEN_SKIP_PERSISTED = "event=operation.open.skip_persisted originId={} userId={}";
    private static final String LOG_OPEN_SKIP_BUDGET = "event=operation.open.skip_budget originId={} userId={} wallet={} symbol={} marginRequired={} marginBudget={} usedMargin={}";
    private static final String LOG_OPEN_SEND = "event=binance.open.send originId={} userId={} wallet={} symbol={} qty={} leverage={} notional={} marginRequired={} marginBudget={}";
    private static final String LOG_OPEN_OK = "event=binance.open.ok originId={} userId={} wallet={} symbol={} orderId={} notional={} marginRequired={}";
    private static final String LOG_OPEN_ERROR = "event=binance.open.error originId={} userId={} wallet={} err={}";

    private static final String LOG_PREP_INVALID_METRIC = "event=operation.open.invalid_metric originId={} userId={} wallet={} reason=invalid_capitalShare";
    private static final String LOG_PREP_INVALID_BUDGET = "event=operation.open.invalid_budget originId={} userId={} wallet={} reason=walletBudget<=0";
    private static final String LOG_PREP_INVALID_SIZE = "event=operation.open.invalid_size originId={} userId={} wallet={} reason=size<=0";
    private static final String LOG_PREP_INVALID_FRACTION = "event=operation.open.invalid_fraction originId={} userId={} wallet={} fraction={}";
    private static final String LOG_PREP_INVALID_ENTRY = "event=operation.open.invalid_entry_price originId={} userId={} wallet={} reason=entryPrice<=0";
    private static final String LOG_PREP_INVALID_SYMBOL = "event=operation.open.invalid_symbol originId={} userId={} wallet={} reason=symbol_blank";
    private static final String LOG_PREP_SKIP_TOO_SMALL = "event=operation.open.skip_too_small originId={} userId={} wallet={} symbol={} notionalMax={}";
    private static final String LOG_PREP_SKIP_SYMBOL_RULES = "event=operation.open.skip_symbol_rules originId={} userId={} wallet={} symbol={} notionalMax={}";
    private static final String LOG_PREP_SKIP_NOTIONAL_ZERO = "event=operation.open.skip_notional_zero originId={} userId={} wallet={} symbol={}";
    private static final String LOG_PREP_PREPARED = "event=operation.open.prepared originId={} userId={} wallet={} symbol={} fraction={} walletBudget={} marginThisTrade={} leverage={} notionalFinal={} marginRequired={}";
    private static final BigDecimal MAX_MARGIN_SAFETY_PCT = new BigDecimal("0.30");

    private static final String LOG_QTY_ADJUSTED = "event=binance.qty_adjusted symbol={} qtyOriginal={} qtyFinal={} notionalFinal={} notionalMax={}";
    private static final String LOG_SYMBOLS_CACHE_REFRESH = "event=binance.symbols_cache.refresh size={} ttlMs={}";

    private static final String LOG_MARGIN_RELEASE = "event=margin.release copyKey={} userWalletKey={} margin={}";
    private static final String LOG_MARGIN_RECONCILE = "event=margin.reconcile copyKey={} userWalletKey={} oldMargin={} newMargin={} delta={}";

    private static final String LOG_COPY_CREATE_INVALID = "event=copyop.create.invalid_order idUser={} idWallet={} idOperation={}";
    private static final String LOG_CLOSE_INVALID_RESPONSE = "event=binance.close.invalid_response originId={} userId={} symbol={}";
    private static final String LOG_CLOSE_OK = "event=binance.close.ok originId={} userId={} symbol={} qty={} orderId={}";
    private static final int USER_LEVERAGE_MIN = 1;
    private static final int USER_LEVERAGE_MAX = 10;
    private static final int COPY_LEVERAGE_FIXED = 5;

    private final ProcesBinanceService procesBinanceService;
    private final ThreadPoolTaskScheduler binanceTaskScheduler;
    private final MetricWalletService metricWalletService;
    private final CopyOperationService copyOperationService;
    private final DistributedLockService distributedLockService;

    @Value("${binance.dispatch.delay-ms:100}")
    private long delayBetweenMs;

    @Value("${engine.copy.margin-safety-buffer-pct:0.05}")
    private BigDecimal marginSafetyBufferPct;

    @Value("${binance.symbols.cache-ttl-ms:60000}")
    private long symbolsCacheTtlMs;

    @Value("${engine.copy.wallet-hardcap-over-pct:0.03}")
    private BigDecimal walletHardcapOverPct; // 3% de sobre-asignación permitida

    @Value("${engine.copy.wallet-reserve-pct:0.01}")
    private BigDecimal walletReservePct; // 1% reservado como colchón interno por wallet

    @Value("${engine.copy.sizing.base-capital-cap:10000}")
    private double sizingBaseCapitalCap; // tope al denominador (evita ballenas incopiables)

    @Value("${engine.copy.sizing.max-fraction-per-trade:0.25}")
    private BigDecimal maxFractionPerTrade;

    @Value("${engine.copy.sizing.notional-buffer-pct:0.03}")
    private BigDecimal notionalBufferPct;

    private final CopyTradingMapper copyTradingMapper;


    private final ConcurrentMap<String, Long> processedOperations = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, BigDecimal> usedMarginByUserWallet = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Reservation> reservationByCopyKey = new ConcurrentHashMap<>();

    private final Object symbolsCacheLock = new Object();
    private volatile SymbolsCache symbolsCache = new SymbolsCache(0L, Collections.emptyMap());

    @Override
    public void openOperation(OperacionEvent event, List<UserDetailDto> usersDetail) {
        Objects.requireNonNull(event, ERR_EVENT_NULL);
        Objects.requireNonNull(usersDetail, ERR_USERS_NULL);

        final String originId = event.getOperacion().getIdOperacion().toString();
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

        final Map<Integer, List<MetricaWalletDto>> metricsByMaxWallet = new HashMap<>();

        for (UserDetailDto userDetail : usersDetail) {
            final String userId = userDetail.getUser().getId().toString();

            if (alreadyCopiedUsers.contains(userId)) {
                log.info(LOG_OPEN_SKIP_ALREADY, originId, userId);
                continue;
            }

            final int maxWallet = userDetail.getDetail().getMaxWallet();
            final List<MetricaWalletDto> metrics = metricsByMaxWallet.computeIfAbsent(
                    maxWallet,
                    mw -> normalizeCapitalShares(metricWalletService.getMetricWallets(mw))
            );

            final MetricaWalletDto walletMetric = getWalletMetricForOperation(event.getOperacion().getIdCuenta(), metrics);

            if (walletMetric == null) {
                log.warn(LOG_OPEN_NO_METRIC, originId, userId, event.getOperacion().getIdCuenta(), maxWallet);
                continue;
            }

            // Las métricas son referencia (ranking), pero el filtro debe bloquear aperturas.
            if (!passesWalletFilters(walletMetric)) {
                log.info("event=operation.open.skip_wallet_filters originId={} userId={} wallet={} reason=passesFilter=false",
                        originId, userId, walletMetric.getWallet().getIdWallet());
                continue;
            }

            final long delay = (long) scheduled * delayBetweenMs;
            final Instant executionTime = Instant.ofEpochMilli(baseTime + delay);

            binanceTaskScheduler.schedule(
                    () -> executeNewOperation(event, userDetail, walletMetric),
                    executionTime
            );

            scheduled++;
        }

        log.info(LOG_OPEN_SCHEDULED, originId, usersDetail.size(), scheduled, metricsByMaxWallet.size(), delayBetweenMs);
    }

    @Override
    public void closeOperation(OperacionEvent event, List<UserDetailDto> usersDetail) {
        Objects.requireNonNull(event, ERR_EVENT_NULL);
        Objects.requireNonNull(usersDetail, ERR_USERS_NULL);

        final long baseTime = System.currentTimeMillis();
        final String originId = event.getOperacion().getIdOperacion().toString();

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
            final CopyOperationDto copyOperation = copyByUser.get(userId);

            if (copyOperation == null) {
                log.warn(LOG_CLOSE_COPY_MISSING, originId, userId);
                continue;
            }

            if (!copyOperation.isActive()) {
                log.info(LOG_CLOSE_SKIP_INACTIVE, originId, userId);
                continue;
            }

            final long delay = (long) scheduled * delayBetweenMs;
            final Instant executionTime = Instant.ofEpochMilli(baseTime + delay);

            binanceTaskScheduler.schedule(
                    () -> safelyExecuteCloseOperationStrict(originId, userDetail),
                    executionTime
            );

            scheduled++;
        }

        log.info(LOG_CLOSE_SCHEDULED, originId, usersDetail.size(), scheduled, delayBetweenMs);
    }

    @Override
    public void executeOpenForUser(OperacionEvent event, UserDetailDto userDetail) {
        Objects.requireNonNull(event, ERR_EVENT_NULL);
        Objects.requireNonNull(userDetail, ERR_USER_NULL);

        final String originId = event.getOperacion().getIdOperacion().toString();
        final String userId = userDetail.getUser().getId().toString();

        if (copyOperationService.existsByOriginAndUser(originId, userId)) {
            return;
        }

        final MetricaWalletDto walletMetric = resolveWalletMetricOrThrowSkip(event, userDetail);
        executeNewOperationStrict(event, userDetail, walletMetric);
    }

    @Override
    public void executeCloseForUser(OperacionEvent event, UserDetailDto userDetail) {
        Objects.requireNonNull(event, ERR_EVENT_NULL);
        Objects.requireNonNull(userDetail, ERR_USER_NULL);

        final String originId = event.getOperacion().getIdOperacion().toString();
        executeCloseOperationStrict(originId, userDetail);
    }

    private void safelyExecuteCloseOperation(CopyOperationDto copyOperation, UserDetailDto userDetail, String originId) {
        try {
            executeCloseOperation(copyOperation, userDetail);
        } catch (Exception ex) {
            log.error(LOG_CLOSE_ERROR, originId, userDetail.getUser().getId(), ex.toString(), ex);
        }
    }

    private void executeNewOperation(OperacionEvent event, UserDetailDto userDetail, MetricaWalletDto walletMetric) {
        final String originId = event.getOperacion().getIdOperacion().toString();
        final String userId = userDetail.getUser().getId().toString();
        final String wallet = walletMetric.getWallet().getIdWallet();

        try {
            if (copyOperationService.existsByOriginAndUser(originId, userId)) {
                log.info(LOG_OPEN_SKIP_PERSISTED, originId, userId);
                return;
            }

            executeNewOperationStrict(event, userDetail, walletMetric);

        } catch (Exception ex) {
            log.error(LOG_OPEN_ERROR, originId, userId, wallet, ex.toString(), ex);
        }
    }

    public void executeNewOperationStrict(OperacionEvent event, UserDetailDto userDetail, MetricaWalletDto walletMetric) {
        final String originId = event.getOperacion().getIdOperacion().toString();
        final String userId = userDetail.getUser().getId().toString();
        final String walletId = walletMetric.getWallet().getIdWallet();

        if (!passesWalletFilters(walletMetric)) {
            throw new SkipExecutionException("wallet_filters_block");
        }

        final PreparedOpen prepared = prepareOpenOperation(event, userDetail, walletMetric);
        if (prepared == null) {
            throw new SkipExecutionException("No se pudo preparar la operación");
        }

        final String lockKey = distLockKey(userId, walletId);

        distributedLockService.withLock(lockKey, Duration.ofSeconds(120), () -> {
            // Re-check dentro del lock (2+ réplicas).
            if (copyOperationService.existsByOriginAndUser(originId, userId)) {
                log.info(LOG_OPEN_SKIP_PERSISTED, originId, userId);
                return null;
            }

            // Presupuesto/margen: fuente de verdad = DB (posiciones activas).
            final BigDecimal safetyForDb = safePct(marginSafetyBufferPct, MAX_MARGIN_SAFETY_PCT);
            final BigDecimal usedFromDb = copyOperationService.sumBufferedMarginActive(userId, walletId, safetyForDb);

            final BigDecimal over = walletHardcapOverPct == null ? ZERO : walletHardcapOverPct;
            final BigDecimal reservePct = walletReservePct == null ? ZERO : walletReservePct;

            final BigDecimal hardCap = prepared.walletMarginBudget.multiply(BigDecimal.ONE.add(over));
            final BigDecimal reserve = prepared.walletMarginBudget.multiply(reservePct);

            if (usedFromDb.add(prepared.marginRequired).add(reserve).compareTo(hardCap) > 0) {
                log.warn(LOG_OPEN_SKIP_BUDGET,
                        originId,
                        userId,
                        walletId,
                        prepared.symbol,
                        prepared.marginRequired.toPlainString(),
                        prepared.walletMarginBudget.toPlainString(),
                        usedFromDb.toPlainString());
                throw new SkipExecutionException("Sin presupuesto/margen disponible");
            }

            boolean orderPlaced = false;
            BinanceFuturesOrderClientResponse response = null;

            try {
                log.info(LOG_OPEN_SEND,
                        originId,
                        userId,
                        walletId,
                        prepared.dto.getSymbol(),
                        prepared.dto.getQuantity(),
                        prepared.leverage,
                        prepared.notional.toPlainString(),
                        prepared.marginRequired.toPlainString(),
                        prepared.walletMarginBudget.toPlainString());

                response = procesBinanceService.operationPosition(prepared.dto);

                if (!isValidOrderResponse(response)) {
                    throw new EngineException(ErrorCode.EXTERNAL_SERVICE_ERROR, "Respuesta inválida de Binance");
                }
                orderPlaced = true;

                final BigDecimal actualNotional = safeNotional(response, prepared.entryPrice);
                final BigDecimal safety = safePct(marginSafetyBufferPct, MAX_MARGIN_SAFETY_PCT);

                final BigDecimal actualMarginBuffered;
                if (actualNotional.compareTo(ZERO) > 0) {
                    final BigDecimal actualMargin = actualNotional
                            .divide(BigDecimal.valueOf(prepared.leverage), DEFAULT_CALC_SCALE, RoundingMode.HALF_UP);
                    actualMarginBuffered = actualMargin.multiply(BigDecimal.ONE.add(safety));
                } else {
                    actualMarginBuffered = prepared.marginRequired;
                }

                createNewOperation(
                        response,
                        walletId,
                        UUID.fromString(originId),
                        userId,
                        prepared.leverage
                );

                log.info(LOG_OPEN_OK,
                        originId,
                        userId,
                        walletId,
                        prepared.symbol,
                        response.getOrderId(),
                        actualNotional.toPlainString(),
                        actualMarginBuffered.toPlainString());

            } catch (Exception ex) {
                if (orderPlaced) {
                    // Si la orden ya se colocó, pero falló persistencia/algún paso posterior,
                    // intentamos cerrar de inmediato (reduceOnly) para no dejar una posición
                    // abierta sin tracking (especialmente con 2+ réplicas).
                    try {
                        tryPanicCloseAfterPersistFailure(originId, userId, walletId, prepared, response, userDetail);
                    } catch (Exception closeEx) {
                        log.error("event=panic_close.failed originId={} userId={} wallet={} err={}",
                                originId, userId, walletId, closeEx.toString(), closeEx);
                        throw ex;
                    }
                }
                throw ex;
            }

            return null;
        });
    }

    private void tryPanicCloseAfterPersistFailure(String originId, String userId, String walletId,
                                                 PreparedOpen prepared,
                                                 BinanceFuturesOrderClientResponse openResponse,
                                                 UserDetailDto userDetail) {
        if (prepared == null || userDetail == null) return;

        BigDecimal qty = null;
        if (openResponse != null) {
            qty = openResponse.getExecutedQty();
            if (qty == null || qty.compareTo(ZERO) <= 0) qty = openResponse.getCumQty();
        }
        if (qty == null || qty.compareTo(ZERO) <= 0) {
            // fallback a la qty calculada
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
    public void executeCloseOperationStrict(String originId, UserDetailDto userDetail) {
        Objects.requireNonNull(originId, "originId no puede ser null");
        Objects.requireNonNull(userDetail, ERR_USER_NULL);

        final String userId = userDetail.getUser().getId().toString();

        // Necesitamos el walletId para el lock; lo leemos primero.
        final CopyOperationDto initial = copyOperationService.findOperationForUser(originId, userId);
        if (initial == null) throw new SkipExecutionException("copy_missing");

        final String walletId = initial.getIdWalletOrigin();
        if (walletId == null || walletId.isBlank()) throw new SkipExecutionException("wallet_missing");

        final String lockKey = distLockKey(userId, walletId);

        distributedLockService.withLock(lockKey, Duration.ofSeconds(120), () -> {
            final CopyOperationDto copyOperation = copyOperationService.findOperationForUser(originId, userId);
            if (copyOperation == null) throw new SkipExecutionException("copy_missing");
            if (!copyOperation.isActive()) throw new SkipExecutionException("copy_inactive");

            executeCloseOperation(copyOperation, userDetail);
            return null;
        });
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

    // 4) prepareOpenOperation CORREGIDA + baseCapitalCap + quantity==ZERO handling + PreparedOpen(8 args)
    private PreparedOpen prepareOpenOperation(OperacionEvent event,
                                              UserDetailDto userDetail,
                                              MetricaWalletDto walletMetric) {

        Objects.requireNonNull(event, ERR_EVENT_NULL);
        Objects.requireNonNull(userDetail, ERR_USER_NULL);
        Objects.requireNonNull(walletMetric, "walletMetric no puede ser null");

        final String originId = event.getOperacion().getIdOperacion().toString();
        final String userId = userDetail.getUser().getId().toString();
        final String walletId = walletMetric.getWallet().getIdWallet();

        // capitalShare clamp (0..1)
        Double capitalShareRaw = walletMetric.getCapitalShare();
        if (capitalShareRaw == null || !Double.isFinite(capitalShareRaw)) {
            log.warn(LOG_PREP_INVALID_METRIC, originId, userId, walletId);
            return null;
        }
        double capitalShare = Math.max(0.0, Math.min(1.0, capitalShareRaw));
        if (capitalShare <= 0.0) {
            log.warn(LOG_PREP_INVALID_METRIC, originId, userId, walletId);
            return null;
        }

        final BigDecimal walletMarginBudget = BigDecimal.valueOf(userDetail.getDetail().getCapital() * capitalShare);
        if (walletMarginBudget.compareTo(ZERO) <= 0) {
            log.warn(LOG_PREP_INVALID_BUDGET, originId, userId, walletId);
            return null;
        }

        final Double capitalRequired =
                walletMetric.getExposureAndCapacity() == null ? null : walletMetric.getExposureAndCapacity().getCapitalRequired();

        double baseCapital = Optional.ofNullable(capitalRequired)
                .filter(required -> required > 0)
                .orElse(DEFAULT_BASE_CAPITAL);

        // cap al denominador (evita ballenas incopiables)
        if (Double.isFinite(sizingBaseCapitalCap) && sizingBaseCapitalCap > 0 && baseCapital > sizingBaseCapitalCap) {
            baseCapital = sizingBaseCapitalCap;
        }

        final BigDecimal sizeOriginal = event.getOperacion().getSize();
        if (sizeOriginal == null || sizeOriginal.compareTo(ZERO) <= 0) {
            log.warn(LOG_PREP_INVALID_SIZE, originId, userId, walletId);
            return null;
        }

        final BigDecimal baseCapitalBd = BigDecimal.valueOf(baseCapital);
        BigDecimal fractionOfBaseUsed = sizeOriginal.divide(baseCapitalBd, DEFAULT_CALC_SCALE, RoundingMode.HALF_UP);

        if (fractionOfBaseUsed.compareTo(BigDecimal.ONE) > 0) {
            fractionOfBaseUsed = BigDecimal.ONE;
        }
        if (maxFractionPerTrade != null && maxFractionPerTrade.compareTo(ZERO) > 0) {
            fractionOfBaseUsed = fractionOfBaseUsed.min(maxFractionPerTrade);
        }
        if (fractionOfBaseUsed.compareTo(ZERO) <= 0) {
            log.warn(LOG_PREP_INVALID_FRACTION, originId, userId, walletId, fractionOfBaseUsed.toPlainString());
            return null;
        }

        final BigDecimal marginThisTrade = walletMarginBudget.multiply(fractionOfBaseUsed);

        final BigDecimal entryPrice = event.getOperacion().getPrecioEntrada();
        if (entryPrice == null || entryPrice.compareTo(ZERO) <= 0) {
            log.warn(LOG_PREP_INVALID_ENTRY, originId, userId, walletId);
            return null;
        }

        final String symbol = event.getOperacion().getParSymbol();
        if (symbol == null || symbol.isBlank()) {
            log.warn(LOG_PREP_INVALID_SYMBOL, originId, userId, walletId);
            return null;
        }

        final int leverage = Math.min(
                USER_LEVERAGE_MAX,
                Math.max(USER_LEVERAGE_MIN, COPY_LEVERAGE_FIXED)
        );

        final BigDecimal notionalMax = marginThisTrade.multiply(BigDecimal.valueOf(leverage));
        if (notionalMax.compareTo(ZERO) <= 0) {
            log.warn(LOG_PREP_SKIP_NOTIONAL_ZERO, originId, userId, walletId, symbol);
            return null;
        }

        // Buffer notional (cap 30%)
        final BigDecimal buf = safePct(notionalBufferPct, new BigDecimal("0.30"));
        final BigDecimal targetNotional = notionalMax.multiply(BigDecimal.ONE.subtract(buf));

        final BinanceFuturesSymbolInfoClientDto symbolInfo = getSymbolsBySymbol().get(symbol.toUpperCase());
        if (symbolInfo == null) {
            log.warn(LOG_PREP_SKIP_SYMBOL_RULES, originId, userId, walletId, symbol, targetNotional.toPlainString());
            return null;
        }

        final SymbolRules rules = extractRules(symbolInfo);
        if (rules == null || rules.minNotional == null) {
            log.warn(LOG_PREP_SKIP_SYMBOL_RULES, originId, userId, walletId, symbol, targetNotional.toPlainString());
            return null;
        }

        if (targetNotional.compareTo(rules.minNotional) < 0) {
            log.info(LOG_PREP_SKIP_TOO_SMALL, originId, userId, walletId, symbol, targetNotional.toPlainString());
            return null;
        }

        BigDecimal quantity = targetNotional.divide(entryPrice, rules.qtyScale, RoundingMode.DOWN);
        if (quantity.compareTo(ZERO) <= 0) {
            log.info(LOG_PREP_SKIP_TOO_SMALL, originId, userId, walletId, symbol, targetNotional.toPlainString());
            return null;
        }

        quantity = adjustQuantityToBinanceRules(symbol, quantity, entryPrice, rules, notionalMax);
        if (quantity.compareTo(ZERO) <= 0) {
            log.info(LOG_PREP_SKIP_TOO_SMALL, originId, userId, walletId, symbol, targetNotional.toPlainString());
            return null;
        }

        final BigDecimal notionalFinal = quantity.multiply(entryPrice);
        if (notionalFinal.compareTo(rules.minNotional) < 0) {
            log.info(LOG_PREP_SKIP_TOO_SMALL, originId, userId, walletId, symbol, notionalFinal.toPlainString());
            return null;
        }

        final BigDecimal safety = safePct(marginSafetyBufferPct, MAX_MARGIN_SAFETY_PCT);

        final BigDecimal marginRequired = notionalFinal
                .divide(BigDecimal.valueOf(leverage), DEFAULT_CALC_SCALE, RoundingMode.HALF_UP)
                .multiply(BigDecimal.ONE.add(safety));

        final String userWalletKey = userWalletKey(userId, walletId);

        log.debug(LOG_PREP_PREPARED,
                originId,
                userId,
                walletId,
                symbol,
                fractionOfBaseUsed.toPlainString(),
                walletMarginBudget.toPlainString(),
                marginThisTrade.toPlainString(),
                leverage,
                notionalFinal.toPlainString(),
                marginRequired.toPlainString());

        final OperationDto dto = buildBuyAndSellPosition(event, quantity, userDetail, leverage);
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

    private OperationDto buildBuyAndSellPosition(OperacionEvent event,
                                                 BigDecimal quantity,
                                                 UserDetailDto userDetail,
                                                 int leverage) {

        final String symbol = event.getOperacion().getParSymbol();
        final PositionSide side = event.getOperacion().getTipoOperacion();

        if (side == null) {
            throw new SkipExecutionException("tipoOperacion inválido/null");
        }

        final Side orderSide = (side.equals(PositionSide.LONG)) ? Side.BUY : Side.SELL;

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


    private MetricaWalletDto resolveWalletMetricOrThrowSkip(OperacionEvent event, UserDetailDto userDetail) {
        final int maxWallet = userDetail.getDetail().getMaxWallet();
        final List<MetricaWalletDto> metrics =
                normalizeCapitalShares(metricWalletService.getMetricWallets(maxWallet));

        final MetricaWalletDto metric = getWalletMetricForOperation(event.getOperacion().getIdCuenta(), metrics);
        if (metric == null) {
            throw new SkipExecutionException("No existe métrica para wallet=" + event.getOperacion().getIdCuenta() + " (maxWallet=" + maxWallet + ")");
        }

        final Double share = metric.getCapitalShare();
        if (share == null || share <= 0) {
            throw new SkipExecutionException("capitalShare inválido para wallet=" + metric.getWallet().getIdWallet());
        }

        return metric;
    }

    private MetricaWalletDto getWalletMetricForOperation(String idWalletOperation, List<MetricaWalletDto> metrics) {
        if (metrics == null || metrics.isEmpty() || idWalletOperation == null) return null;

        final String w = idWalletOperation.trim();
        return metrics.stream()
                .filter(m -> m.getWallet().getIdWallet() != null && m.getWallet().getIdWallet().trim().equalsIgnoreCase(w))
                .findFirst()
                .orElse(null);
    }

    private Map<String, BinanceFuturesSymbolInfoClientDto> getSymbolsBySymbol() {
        final long now = System.currentTimeMillis();
        final SymbolsCache snapshot = symbolsCache;

        if (snapshot.expiresAtMs > now && snapshot.bySymbol != null && !snapshot.bySymbol.isEmpty()) {
            return snapshot.bySymbol;
        }

        synchronized (symbolsCacheLock) {
            final SymbolsCache snapshot2 = symbolsCache;
            if (snapshot2.expiresAtMs > now && snapshot2.bySymbol != null && !snapshot2.bySymbol.isEmpty()) {
                return snapshot2.bySymbol;
            }

            final List<BinanceFuturesSymbolInfoClientDto> symbols = procesBinanceService.getSymbols(SYMBOLS_API_KEY);

            final Map<String, BinanceFuturesSymbolInfoClientDto> map = symbols == null
                    ? Collections.emptyMap()
                    : symbols.stream()
                    .filter(s -> s.getSymbol() != null && !s.getSymbol().isBlank())
                    .collect(Collectors.toMap(
                            s -> s.getSymbol().toUpperCase(),
                            s -> s,
                            (a, b) -> a
                    ));

            symbolsCache = new SymbolsCache(now + symbolsCacheTtlMs, map);
            log.debug(LOG_SYMBOLS_CACHE_REFRESH, map.size(), symbolsCacheTtlMs);

            return map;
        }
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
            if (f == null || f.getFilterType() == null) continue;

            if (FILTER_TYPE_LOT_SIZE.equalsIgnoreCase(f.getFilterType())
                    || FILTER_TYPE_MARKET_LOT_SIZE.equalsIgnoreCase(f.getFilterType())) {
                stepSize = safeBigDecimal(f.getStepSize());
                minQty = safeBigDecimal(f.getMinQty());
                break;
            }
        }

        final int stepScale = (stepSize == null) ? 0 : Math.max(0, stepSize.stripTrailingZeros().scale());
        final int finalScale = Math.max(qtyScaleFromBinance, stepScale);

        BigDecimal minNotional = null;
        for (BinanceFuturesSymbolFilterDto f : symbolDetail.getFilters()) {
            if (f == null || f.getFilterType() == null) continue;

            if (FILTER_TYPE_MIN_NOTIONAL.equalsIgnoreCase(f.getFilterType())
                    || FILTER_TYPE_NOTIONAL.equalsIgnoreCase(f.getFilterType())) {
                minNotional = safeBigDecimal(f.getNotional());
                break;
            }
        }

        if (minNotional == null) minNotional = MIN_NOTIONAL_FALLBACK;
        if (minNotional.compareTo(MIN_NOTIONAL_FALLBACK) < 0) minNotional = MIN_NOTIONAL_FALLBACK;
        minNotional = minNotional.max(USER_MIN_NOTIONAL_USDT).max(MIN_NOTIONAL_FALLBACK);

        return new SymbolRules(stepSize, minQty, minNotional, finalScale);
    }


    // 3) adjustQuantityToBinanceRules CORREGIDA (sin variables "fantasma", sin duplicados)
    private BigDecimal adjustQuantityToBinanceRules(String symbol,
                                                    BigDecimal quantity,
                                                    BigDecimal entryPrice,
                                                    SymbolRules rules,
                                                    BigDecimal notionalMax) {

        if (quantity == null || entryPrice == null || rules == null || notionalMax == null) return ZERO;
        if (quantity.compareTo(ZERO) <= 0 || entryPrice.compareTo(ZERO) <= 0 || notionalMax.compareTo(ZERO) <= 0) return ZERO;

        BigDecimal adjusted = quantity;

        // Step rounding (floor to step)
        if (rules.stepSize != null && rules.stepSize.compareTo(ZERO) > 0) {
            adjusted = adjusted.divide(rules.stepSize, 0, RoundingMode.DOWN).multiply(rules.stepSize);
        }

        // Min qty
        if (rules.minQty != null && adjusted.compareTo(rules.minQty) < 0) {
            adjusted = rules.minQty;
        }

        // Precision
        if (rules.qtyScale >= 0) {
            adjusted = adjusted.setScale(rules.qtyScale, RoundingMode.DOWN);
        }

        if (adjusted.compareTo(ZERO) <= 0) return ZERO;

        BigDecimal notionalFinal = adjusted.multiply(entryPrice);

        // Si se pasa del budget, recalculamos hacia abajo con buffer
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
            if (adjusted.compareTo(ZERO) <= 0) return ZERO;

            notionalFinal = adjusted.multiply(entryPrice);

            // Si incluso después de ajustar sigue sobre el máximo (por minQty/step), abortamos
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
        if (response == null) return ZERO;

        BigDecimal qty = response.getExecutedQty();
        if (qty == null || qty.compareTo(ZERO) <= 0) qty = response.getCumQty();
        if (qty == null || qty.compareTo(ZERO) <= 0) qty = response.getOrigQty();
        if (qty == null || qty.compareTo(ZERO) <= 0) return ZERO;

        BigDecimal px = response.getAvgPrice();
        if (px == null || px.compareTo(ZERO) <= 0) {
            px = fallbackPrice;
        }
        if (px == null || px.compareTo(ZERO) <= 0) return ZERO;

        return qty.multiply(px);
    }

    private boolean passesWalletFilters(MetricaWalletDto walletMetric) {
        if (walletMetric == null || walletMetric.getScoring() == null) return false;
        return Boolean.TRUE;
    }

    private String copyKey(String originId, String userId) {
        return originId + "::" + userId;
    }

    private String userWalletKey(String userId, String walletId) {
        return userId + "::" + walletId;
    }

    private String distLockKey(String userId, String walletId) {
        return "copy-wallet-lock::" + userWalletKey(userId, walletId);
    }

    private boolean reserveMargin(String userWalletKey,
                                  String copyKey,
                                  BigDecimal marginRequired,
                                  BigDecimal walletMarginBudget) {

        if (marginRequired == null || marginRequired.compareTo(ZERO) <= 0) {
            return false;
        }
        if (walletMarginBudget == null || walletMarginBudget.compareTo(ZERO) <= 0) {
            return false;
        }

        final BigDecimal hardCap = walletMarginBudget.multiply(BigDecimal.ONE.add(
                walletHardcapOverPct == null ? ZERO : walletHardcapOverPct
        ));

        final BigDecimal reserve = walletMarginBudget.multiply(
                walletReservePct == null ? ZERO : walletReservePct
        );

        while (true) {
            final BigDecimal current = usedMarginByUserWallet.getOrDefault(userWalletKey, ZERO);
            final BigDecimal next = current.add(marginRequired);


            if (next.add(reserve).compareTo(hardCap) > 0) {
                return false;
            }

            final boolean updated =
                    usedMarginByUserWallet.putIfAbsent(userWalletKey, next) == null
                            || usedMarginByUserWallet.replace(userWalletKey, current, next);

            if (updated) {
                reservationByCopyKey.put(copyKey, new Reservation(userWalletKey, marginRequired));
                return true;
            }
        }
    }

    private void releaseReservation(String copyKey) {
        final Reservation r = reservationByCopyKey.remove(copyKey);
        if (r == null) {
            return;
        }

        usedMarginByUserWallet.compute(r.userWalletKey, (k, v) -> {
            final BigDecimal current = v == null ? ZERO : v;
            final BigDecimal next = current.subtract(r.margin);
            return next.compareTo(ZERO) < 0 ? ZERO : next;
        });

        log.debug(LOG_MARGIN_RELEASE, copyKey, r.userWalletKey, r.margin.toPlainString());
    }

    private void reconcileReservation(String userWalletKey, String copyKey, BigDecimal newMargin) {
        final Reservation existing = reservationByCopyKey.get(copyKey);
        if (existing == null) {
            return;
        }

        final BigDecimal oldMargin = existing.margin;
        final BigDecimal delta = newMargin.subtract(oldMargin);

        if (delta.compareTo(ZERO) == 0) {
            return;
        }

        usedMarginByUserWallet.compute(userWalletKey, (k, v) -> {
            final BigDecimal current = v == null ? ZERO : v;
            final BigDecimal next = current.add(delta);
            return next.compareTo(ZERO) < 0 ? ZERO : next;
        });

        reservationByCopyKey.put(copyKey, new Reservation(userWalletKey, newMargin));

        log.debug(LOG_MARGIN_RECONCILE,
                copyKey,
                userWalletKey,
                oldMargin.toPlainString(),
                newMargin.toPlainString(),
                delta.toPlainString());
    }

    private BigDecimal safeBigDecimal(String raw) {
        try {
            if (raw == null || raw.isBlank()) return null;
            return new BigDecimal(raw);
        } catch (Exception e) {
            return null;
        }
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
        private final BigDecimal entryPrice;     // <-- NUEVO (BigDecimal)
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

        private SymbolsCache(long expiresAtMs, Map<String, BinanceFuturesSymbolInfoClientDto> bySymbol) {
            this.expiresAtMs = expiresAtMs;
            this.bySymbol = bySymbol;
        }
    }

    private BigDecimal safePct(BigDecimal raw, BigDecimal max) {
        if (raw == null) return ZERO;
        if (raw.compareTo(ZERO) < 0) return ZERO;
        if (raw.compareTo(max) > 0) return max;
        return raw;
    }

    private List<MetricaWalletDto> normalizeCapitalShares(List<MetricaWalletDto> metrics) {
        if (metrics == null || metrics.isEmpty()) return metrics;

        double total = 0.0;
        for (MetricaWalletDto m : metrics) {
            if (m == null) continue;
            Double s = m.getCapitalShare();
            if (s != null && Double.isFinite(s) && s > 0) total += s;
        }

        if (total <= 0.0) return metrics;

        if (total > 1.0) {
            for (MetricaWalletDto m : metrics) {
                if (m == null) continue;
                Double s = m.getCapitalShare();
                if (s == null || !Double.isFinite(s) || s <= 0) {
                    m.setCapitalShare(0.0);
                } else {
                    m.setCapitalShare(s / total);
                }
            }
        } else {
            for (MetricaWalletDto m : metrics) {
                if (m == null) continue;
                Double s = m.getCapitalShare();
                if (s == null || !Double.isFinite(s) || s < 0) m.setCapitalShare(0.0);
            }
        }

        return metrics;
    }

    private void safelyExecuteCloseOperationStrict(String originId, UserDetailDto userDetail) {
        try {
            executeCloseOperationStrict(originId, userDetail);
        } catch (Exception ex) {
            log.error(LOG_CLOSE_ERROR, originId, userDetail.getUser().getId(), ex.toString(), ex);
        }
    }
}
