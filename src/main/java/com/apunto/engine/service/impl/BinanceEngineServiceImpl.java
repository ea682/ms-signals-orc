package com.apunto.engine.service.impl;

import com.apunto.engine.dto.CopyOperationDto;
import com.apunto.engine.dto.OperationDto;
import com.apunto.engine.dto.UserDetailDto;
import com.apunto.engine.dto.client.BinanceFuturesOrderClientResponse;
import com.apunto.engine.dto.client.BinanceFuturesSymbolFilterDto;
import com.apunto.engine.dto.client.BinanceFuturesSymbolInfoClientDto;
import com.apunto.engine.dto.client.MetricaWalletDto;
import com.apunto.engine.events.OperacionEvent;
import com.apunto.engine.service.BinanceEngineService;
import com.apunto.engine.service.CopyOperationService;
import com.apunto.engine.service.MetricWalletService;
import com.apunto.engine.service.ProcesBinanceService;
import com.apunto.engine.shared.enums.OrderType;
import com.apunto.engine.shared.enums.PositionSide;
import com.apunto.engine.shared.enums.Side;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
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
public class BinanceEngineServiceImpl implements BinanceEngineService {

    private static final double DEFAULT_BASE_CAPITAL = 1_000.0;
    private static final BigDecimal MIN_NOTIONAL_FALLBACK = new BigDecimal("7");
    private static final BigDecimal USER_MIN_NOTIONAL_USDT = new BigDecimal("7");
    private static final int DEFAULT_CALC_SCALE = 18;

    private static final long TTL_MS = 60_000L;

    private static final String FILTER_TYPE_LOT_SIZE = "LOT_SIZE";
    private static final String FILTER_TYPE_MARKET_LOT_SIZE = "MARKET_LOT_SIZE";
    private static final String FILTER_TYPE_MIN_NOTIONAL = "MIN_NOTIONAL";
    private static final String FILTER_TYPE_NOTIONAL = "NOTIONAL";

    private static final String TYPE_OPERATION_LONG = "LONG";

    private static final String ERR_EVENT_NULL = "El evento de operación no puede ser null";
    private static final String ERR_USERS_NULL = "La lista de usuarios no puede ser null";
    private static final String ERR_ENTRY_PRICE_INVALID = "entryPrice inválido para ";
    private static final String ERR_SYMBOL_NOT_FOUND = "No se encontró información de símbolo para: ";
    private static final String ERR_ORIG_QTY_NULL = "origQty null";
    private static final String ERR_AVG_PRICE_NULL = "avgPrice null";

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
    private static final String LOG_OPEN_INVALID_RESPONSE = "event=binance.open.invalid_response originId={} userId={} wallet={} symbol={}";
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

    private static final String LOG_QTY_ADJUSTED = "event=binance.qty_adjusted symbol={} qtyOriginal={} qtyFinal={} notionalFinal={} notionalMax={}";
    private static final String LOG_SYMBOLS_CACHE_REFRESH = "event=binance.symbols_cache.refresh size={} ttlMs={}";

    private static final String LOG_MARGIN_RELEASE = "event=margin.release copyKey={} userWalletKey={} margin={}";
    private static final String LOG_MARGIN_RECONCILE = "event=margin.reconcile copyKey={} userWalletKey={} oldMargin={} newMargin={} delta={}";

    private static final String LOG_COPY_CREATE_INVALID = "event=copyop.create.invalid_order idUser={} idWallet={} idOperation={}";

    private static final String LOG_CLOSE_SEND = "event=binance.close.send originId={} userId={} symbol={} qty={}";
    private static final String LOG_CLOSE_INVALID_RESPONSE = "event=binance.close.invalid_response originId={} userId={} symbol={}";
    private static final String LOG_CLOSE_OK = "event=binance.close.ok originId={} userId={} symbol={} qty={} orderId={}";

    private final ProcesBinanceService procesBinanceService;
    private final ThreadPoolTaskScheduler binanceTaskScheduler;
    private final MetricWalletService metricWalletService;
    private final CopyOperationService copyOperationService;

    @Value("${binance.dispatch.delay-ms:100}")
    private long delayBetweenMs;

    @Value("${engine.copy.margin-safety-buffer-pct:0.05}")
    private BigDecimal marginSafetyBufferPct;

    @Value("${binance.symbols.cache-ttl-ms:60000}")
    private long symbolsCacheTtlMs;

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
            final List<MetricaWalletDto> metrics = metricsByMaxWallet.computeIfAbsent(maxWallet, metricWalletService::getMetricWallets);
            final MetricaWalletDto walletMetric = getWalletMetricForOperation(event.getOperacion().getIdCuenta(), metrics);

            if (walletMetric == null) {
                log.warn(LOG_OPEN_NO_METRIC, originId, userId, event.getOperacion().getIdCuenta(), maxWallet);
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

        final String originId = event.getOperacion().getIdOperacion().toString();

        final Map<String, CopyOperationDto> copyByUser = copyOperationService.findOperationsByOrigin(originId)
                .stream()
                .collect(Collectors.toMap(
                        CopyOperationDto::getIdUser,
                        x -> x,
                        (a, b) -> a
                ));

        if (copyByUser.isEmpty()) {
            log.warn(LOG_CLOSE_NO_COPIES, originId, usersDetail.size());
            return;
        }

        final long baseTime = System.currentTimeMillis();
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
                    () -> safelyExecuteCloseOperation(copyOperation, userDetail, originId),
                    executionTime
            );

            scheduled++;
        }

        log.info(LOG_CLOSE_SCHEDULED, originId, usersDetail.size(), scheduled, delayBetweenMs);
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
        final String copyKey = copyKey(originId, userId);

        try {
            if (copyOperationService.existsByOriginAndUser(originId, userId)) {
                log.info(LOG_OPEN_SKIP_PERSISTED, originId, userId);
                return;
            }

            final PreparedOpen prepared = prepareOpen(event, userDetail, walletMetric);
            if (prepared == null) {
                return;
            }

            if (!reserveMargin(prepared.userWalletKey, copyKey, prepared.marginRequired, prepared.walletMarginBudget)) {
                log.warn(LOG_OPEN_SKIP_BUDGET,
                        originId,
                        userId,
                        walletMetric.getIdWallet(),
                        prepared.symbol,
                        prepared.marginRequired.toPlainString(),
                        prepared.walletMarginBudget.toPlainString(),
                        usedMarginByUserWallet.getOrDefault(prepared.userWalletKey, BigDecimal.ZERO).toPlainString());
                return;
            }

            log.info(LOG_OPEN_SEND,
                    originId,
                    userId,
                    walletMetric.getIdWallet(),
                    prepared.dto.getSymbol(),
                    prepared.dto.getQuantity(),
                    prepared.leverage,
                    prepared.notional.toPlainString(),
                    prepared.marginRequired.toPlainString(),
                    prepared.walletMarginBudget.toPlainString());

            final BinanceFuturesOrderClientResponse response = procesBinanceService.operationPosition(prepared.dto);

            if (!isValidOrderResponse(response)) {
                releaseReservation(copyKey);
                log.error(LOG_OPEN_INVALID_RESPONSE, originId, userId, walletMetric.getIdWallet(), prepared.symbol);
                return;
            }

            final BigDecimal actualNotional = safeNotional(response);
            final BigDecimal actualMargin = actualNotional
                    .divide(BigDecimal.valueOf(prepared.leverage), DEFAULT_CALC_SCALE, RoundingMode.HALF_UP);

            reconcileReservation(prepared.userWalletKey, copyKey, actualMargin);

            createNewOperation(
                    response,
                    walletMetric.getIdWallet(),
                    UUID.fromString(originId),
                    userId,
                    prepared.leverage
            );

            log.info(LOG_OPEN_OK,
                    originId,
                    userId,
                    walletMetric.getIdWallet(),
                    prepared.symbol,
                    response.getOrderId(),
                    actualNotional.toPlainString(),
                    actualMargin.toPlainString());

        } catch (Exception ex) {
            log.error(LOG_OPEN_ERROR, originId, userId, walletMetric.getIdWallet(), ex.toString(), ex);
        }
    }

    private PreparedOpen prepareOpen(OperacionEvent event, UserDetailDto userDetail, MetricaWalletDto walletMetric) {
        final String originId = event.getOperacion().getIdOperacion().toString();
        final String userId = userDetail.getUser().getId().toString();
        final String walletId = walletMetric.getIdWallet();

        final Double capitalShare = walletMetric.getCapitalShare();
        if (capitalShare == null || capitalShare <= 0) {
            log.error(LOG_PREP_INVALID_METRIC, originId, userId, walletId);
            return null;
        }

        final double capitalWallet = userDetail.getDetail().getCapital() * capitalShare;
        if (capitalWallet <= 0) {
            log.error(LOG_PREP_INVALID_BUDGET, originId, userId, walletId);
            return null;
        }

        final double baseCapital = Optional.ofNullable(walletMetric.getCapitalRequired())
                .filter(required -> required > 0)
                .orElse(DEFAULT_BASE_CAPITAL);

        final BigDecimal sizeOriginal = event.getOperacion().getSize();
        if (sizeOriginal == null || sizeOriginal.compareTo(BigDecimal.ZERO) <= 0) {
            log.error(LOG_PREP_INVALID_SIZE, originId, userId, walletId);
            return null;
        }

        BigDecimal fractionOfBaseUsed = sizeOriginal
                .divide(BigDecimal.valueOf(baseCapital), DEFAULT_CALC_SCALE, RoundingMode.HALF_UP);

        if (fractionOfBaseUsed.compareTo(BigDecimal.ONE) > 0) {
            fractionOfBaseUsed = BigDecimal.ONE;
        }

        if (fractionOfBaseUsed.compareTo(BigDecimal.ZERO) <= 0) {
            log.error(LOG_PREP_INVALID_FRACTION, originId, userId, walletId, fractionOfBaseUsed.toPlainString());
            return null;
        }

        final BigDecimal walletMarginBudget = BigDecimal.valueOf(capitalWallet);
        final BigDecimal marginThisTrade = walletMarginBudget.multiply(fractionOfBaseUsed);

        final BigDecimal entryPrice = event.getOperacion().getPrecioEntrada();
        if (entryPrice == null || entryPrice.compareTo(BigDecimal.ZERO) <= 0) {
            log.error(LOG_PREP_INVALID_ENTRY, originId, userId, walletId);
            return null;
        }

        final int leverage = normalizeLeverage(userDetail.getDetail().getLeverage());

        final String symbol = event.getOperacion().getParSymbol();
        if (symbol == null || symbol.trim().isEmpty()) {
            log.error(LOG_PREP_INVALID_SYMBOL, originId, userId, walletId);
            return null;
        }

        final BigDecimal maxNotional = marginThisTrade.multiply(BigDecimal.valueOf(leverage));

        BigDecimal desiredQty = maxNotional
                .divide(entryPrice, DEFAULT_CALC_SCALE, RoundingMode.DOWN);

        if (desiredQty.compareTo(BigDecimal.ZERO) <= 0) {
            log.info(LOG_PREP_SKIP_TOO_SMALL, originId, userId, walletId, symbol, maxNotional.toPlainString());
            return null;
        }

        final BigDecimal adjustedQty = adjustQuantityToBinanceRules(symbol, desiredQty, entryPrice, maxNotional);

        if (adjustedQty == null || adjustedQty.compareTo(BigDecimal.ZERO) <= 0) {
            log.info(LOG_PREP_SKIP_SYMBOL_RULES, originId, userId, walletId, symbol, maxNotional.toPlainString());
            return null;
        }

        final BigDecimal notionalFinal = adjustedQty.multiply(entryPrice);
        if (notionalFinal.compareTo(BigDecimal.ZERO) <= 0) {
            log.info(LOG_PREP_SKIP_NOTIONAL_ZERO, originId, userId, walletId, symbol);
            return null;
        }

        final BigDecimal marginRequired = notionalFinal
                .divide(BigDecimal.valueOf(leverage), DEFAULT_CALC_SCALE, RoundingMode.HALF_UP);

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

        final OperationDto dto = buildOpenPosition(event, adjustedQty, userDetail, leverage);

        return new PreparedOpen(dto, userWalletKey, walletMarginBudget, marginRequired, notionalFinal, leverage, symbol);
    }

    private OperationDto buildOpenPosition(OperacionEvent event, BigDecimal quantity, UserDetailDto userDetail, int leverage) {
        final boolean isLong = event.getOperacion().getTipoOperacion().equals(PositionSide.LONG);
        return OperationDto.builder()
                .symbol(event.getOperacion().getParSymbol())
                .side(isLong ? Side.BUY : Side.SELL)
                .type(OrderType.MARKET)
                .positionSide(event.getOperacion().getTipoOperacion())
                .quantity(quantity.toPlainString())
                .leverage(leverage)
                .reduceOnly(false)
                .apiKey(userDetail.getUserApiKey().getApiKey())
                .secret(userDetail.getUserApiKey().getApiSecret())
                .build();
    }

    private MetricaWalletDto getWalletMetricForOperation(String idWalletOperation, List<MetricaWalletDto> metrics) {
        if (metrics == null || metrics.isEmpty() || idWalletOperation == null) return null;
        final String w = idWalletOperation.trim();
        return metrics.stream()
                .filter(m -> m.getIdWallet() != null && m.getIdWallet().trim().equalsIgnoreCase(w))
                .findFirst()
                .orElse(null);
    }

    private BigDecimal adjustQuantityToBinanceRules(String parSymbol,
                                                    BigDecimal quantity,
                                                    BigDecimal entryPrice,
                                                    BigDecimal maxNotional) {

        if (quantity == null || quantity.compareTo(BigDecimal.ZERO) <= 0) {
            return null;
        }

        if (entryPrice == null || entryPrice.compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException(ERR_ENTRY_PRICE_INVALID + parSymbol);
        }

        if (maxNotional == null || maxNotional.compareTo(BigDecimal.ZERO) <= 0) {
            return null;
        }

        final BinanceFuturesSymbolInfoClientDto symbolDetail = getSymbolDetail(parSymbol);

        BigDecimal adjusted = quantity;
        BigDecimal stepSize = null;
        BigDecimal minQty = BigDecimal.ZERO;
        BigDecimal minNotionalFromExchange;
        BigDecimal effectiveMinNotional;
        final Integer quantityPrecision = symbolDetail.getQuantityPrecision();

        final BinanceFuturesSymbolFilterDto lotSizeFilter = symbolDetail.getFilters().stream()
                .filter(f -> FILTER_TYPE_LOT_SIZE.equals(f.getFilterType()) || FILTER_TYPE_MARKET_LOT_SIZE.equals(f.getFilterType()))
                .findFirst()
                .orElse(null);

        if (lotSizeFilter != null) {
            stepSize = new BigDecimal(lotSizeFilter.getStepSize());
            minQty = new BigDecimal(lotSizeFilter.getMinQty());

            if (minQty.compareTo(BigDecimal.ZERO) > 0) {
                final BigDecimal minNotionalByMinQty = minQty.multiply(entryPrice);
                if (minNotionalByMinQty.compareTo(maxNotional) > 0) {
                    return null;
                }
            }

            if (stepSize.compareTo(BigDecimal.ZERO) > 0) {
                adjusted = adjusted
                        .divide(stepSize, 0, RoundingMode.DOWN)
                        .multiply(stepSize);
            }

            if (adjusted.compareTo(minQty) < 0) {
                adjusted = minQty;
            }
        }

        final BinanceFuturesSymbolFilterDto minNotionalFilter = symbolDetail.getFilters().stream()
                .filter(f -> FILTER_TYPE_MIN_NOTIONAL.equals(f.getFilterType()) || FILTER_TYPE_NOTIONAL.equals(f.getFilterType()))
                .findFirst()
                .orElse(null);

        if (minNotionalFilter != null && minNotionalFilter.getNotional() != null) {
            minNotionalFromExchange = new BigDecimal(minNotionalFilter.getNotional());
        } else {
            minNotionalFromExchange = MIN_NOTIONAL_FALLBACK;
        }

        if (minNotionalFromExchange.compareTo(MIN_NOTIONAL_FALLBACK) < 0) {
            minNotionalFromExchange = MIN_NOTIONAL_FALLBACK;
        }

        effectiveMinNotional = minNotionalFromExchange
                .max(MIN_NOTIONAL_FALLBACK)
                .max(USER_MIN_NOTIONAL_USDT);

        if (effectiveMinNotional.compareTo(maxNotional) > 0) {
            return null;
        }

        final BigDecimal currentNotional = adjusted.multiply(entryPrice);

        if (currentNotional.compareTo(effectiveMinNotional) < 0) {
            int calcScale = DEFAULT_CALC_SCALE;
            if (quantityPrecision != null) {
                calcScale = Math.max(calcScale, quantityPrecision + 2);
            }
            if (stepSize != null) {
                final int stepScale = stepSize.stripTrailingZeros().scale();
                calcScale = Math.max(calcScale, stepScale + 2);
            }

            BigDecimal neededQty = effectiveMinNotional
                    .divide(entryPrice, calcScale, RoundingMode.UP);

            if (stepSize != null && stepSize.compareTo(BigDecimal.ZERO) > 0) {
                neededQty = neededQty
                        .divide(stepSize, 0, RoundingMode.UP)
                        .multiply(stepSize);
            }

            final BigDecimal neededNotional = neededQty.multiply(entryPrice);
            if (neededNotional.compareTo(maxNotional) > 0) {
                return null;
            }

            adjusted = neededQty;
        }

        if (quantityPrecision != null) {
            adjusted = adjusted.setScale(quantityPrecision, RoundingMode.DOWN);

            final BigDecimal notionalAfter = adjusted.multiply(entryPrice);
            if (notionalAfter.compareTo(effectiveMinNotional) < 0) {
                final BigDecimal increment = BigDecimal.ONE.movePointLeft(quantityPrecision);
                final BigDecimal candidate = adjusted.add(increment);

                BigDecimal candidateStep = candidate;
                if (stepSize != null && stepSize.compareTo(BigDecimal.ZERO) > 0) {
                    candidateStep = candidateStep
                            .divide(stepSize, 0, RoundingMode.UP)
                            .multiply(stepSize);
                }

                final BigDecimal candidateNotional = candidateStep.multiply(entryPrice);
                if (candidateNotional.compareTo(effectiveMinNotional) < 0 || candidateNotional.compareTo(maxNotional) > 0) {
                    return null;
                }

                adjusted = candidateStep.setScale(quantityPrecision, RoundingMode.DOWN);
            }
        }

        final BigDecimal finalNotional = adjusted.multiply(entryPrice);
        if (finalNotional.compareTo(effectiveMinNotional) < 0 || finalNotional.compareTo(maxNotional) > 0) {
            return null;
        }

        log.debug(LOG_QTY_ADJUSTED,
                parSymbol,
                quantity.toPlainString(),
                adjusted.toPlainString(),
                finalNotional.toPlainString(),
                maxNotional.toPlainString());

        return adjusted;
    }

    private BinanceFuturesSymbolInfoClientDto getSymbolDetail(String symbol) {
        final long now = System.currentTimeMillis();
        final String key = symbol.trim().toUpperCase();

        final SymbolsCache cache = symbolsCache;
        if (now < cache.expiresAtMs) {
            final BinanceFuturesSymbolInfoClientDto cached = cache.bySymbol.get(key);
            if (cached != null) {
                return cached;
            }
        }

        synchronized (symbolsCacheLock) {
            final SymbolsCache cache2 = symbolsCache;
            if (now >= cache2.expiresAtMs) {
                final List<BinanceFuturesSymbolInfoClientDto> symbols = procesBinanceService.getSymbols("S");
                final Map<String, BinanceFuturesSymbolInfoClientDto> map = new HashMap<>();
                if (symbols != null) {
                    for (BinanceFuturesSymbolInfoClientDto s : symbols) {
                        if (s != null && s.getSymbol() != null) {
                            map.put(s.getSymbol().trim().toUpperCase(), s);
                        }
                    }
                }
                symbolsCache = new SymbolsCache(now + Math.max(5_000L, symbolsCacheTtlMs), Collections.unmodifiableMap(map));
                log.debug(LOG_SYMBOLS_CACHE_REFRESH, map.size(), symbolsCacheTtlMs);
            }

            final BinanceFuturesSymbolInfoClientDto result = symbolsCache.bySymbol.get(key);
            if (result == null) {
                throw new IllegalArgumentException(ERR_SYMBOL_NOT_FOUND + symbol);
            }
            return result;
        }
    }

    private boolean reserveMargin(String userWalletKey, String copyKey, BigDecimal margin, BigDecimal budget) {
        if (margin == null || margin.compareTo(BigDecimal.ZERO) <= 0) {
            return false;
        }
        if (budget == null || budget.compareTo(BigDecimal.ZERO) <= 0) {
            return false;
        }

        final BigDecimal buffer = normalizeBufferPct(marginSafetyBufferPct);
        final BigDecimal available = budget.multiply(BigDecimal.ONE.subtract(buffer));

        boolean[] ok = {false};
        usedMarginByUserWallet.compute(userWalletKey, (k, current) -> {
            final BigDecimal used = current == null ? BigDecimal.ZERO : current;
            final BigDecimal next = used.add(margin);
            if (next.compareTo(available) <= 0) {
                ok[0] = true;
                return next;
            }
            return used;
        });

        if (ok[0]) {
            reservationByCopyKey.put(copyKey, new Reservation(userWalletKey, margin));
        }

        return ok[0];
    }

    private void releaseReservation(String copyKey) {
        final Reservation reservation = reservationByCopyKey.remove(copyKey);
        if (reservation == null) {
            return;
        }

        usedMarginByUserWallet.compute(reservation.userWalletKey, (k, current) -> {
            final BigDecimal used = current == null ? BigDecimal.ZERO : current;
            final BigDecimal next = used.subtract(reservation.margin);
            return next.compareTo(BigDecimal.ZERO) < 0 ? BigDecimal.ZERO : next;
        });

        log.debug(LOG_MARGIN_RELEASE, copyKey, reservation.userWalletKey, reservation.margin.toPlainString());
    }

    private void reconcileReservation(String userWalletKey, String copyKey, BigDecimal actualMargin) {
        if (actualMargin == null || actualMargin.compareTo(BigDecimal.ZERO) <= 0) {
            return;
        }

        final Reservation current = reservationByCopyKey.get(copyKey);
        if (current == null) {
            return;
        }

        final BigDecimal delta = actualMargin.subtract(current.margin);
        if (delta.compareTo(BigDecimal.ZERO) == 0) {
            return;
        }

        usedMarginByUserWallet.compute(userWalletKey, (k, used0) -> {
            final BigDecimal used = used0 == null ? BigDecimal.ZERO : used0;
            final BigDecimal next = used.add(delta);
            return next.compareTo(BigDecimal.ZERO) < 0 ? BigDecimal.ZERO : next;
        });

        reservationByCopyKey.put(copyKey, new Reservation(userWalletKey, actualMargin));

        log.debug(LOG_MARGIN_RECONCILE,
                copyKey,
                userWalletKey,
                current.margin.toPlainString(),
                actualMargin.toPlainString(),
                delta.toPlainString());
    }

    private void createNewOperation(BinanceFuturesOrderClientResponse order,
                                    String idWallet,
                                    UUID idOperation,
                                    String idUser,
                                    int leverage) {

        if (!isValidOrderResponse(order)) {
            log.error(LOG_COPY_CREATE_INVALID, idUser, idWallet, idOperation);
            return;
        }

        CopyOperationDto buildCopyOperation = buildCopyNewOperationDto(order, idWallet, idOperation, idUser, leverage);
        copyOperationService.newOperation(buildCopyOperation);
    }

    private void executeCloseOperation(CopyOperationDto copyOperation, UserDetailDto userDetail) {
        final String originId = copyOperation.getIdOrderOrigin();
        final String userId = userDetail.getUser().getId().toString();
        final String copyKey = copyKey(originId, userId);

        if (!copyOperation.isActive()) {
            log.info(LOG_CLOSE_SKIP_INACTIVE, originId, userId);
            return;
        }

        final OperationDto closeDto = buildClosePosition(copyOperation, userDetail);

        log.info(LOG_CLOSE_SEND, originId, userId, closeDto.getSymbol(), closeDto.getQuantity());

        final BinanceFuturesOrderClientResponse order = procesBinanceService.operationPosition(closeDto);

        if (!isValidOrderResponse(order)) {
            log.error(LOG_CLOSE_INVALID_RESPONSE, originId, userId, closeDto.getSymbol());
            return;
        }

        final CopyOperationDto buildCopyOperation = buildCopyCloseOperationDto(copyOperation, order);
        copyOperationService.closeOperation(buildCopyOperation);

        releaseReservation(copyKey);

        log.info(LOG_CLOSE_OK,
                originId,
                userId,
                copyOperation.getParsymbol(),
                copyOperation.getSizePar(),
                order.getOrderId());
    }

    private OperationDto buildClosePosition(CopyOperationDto copyOperation, UserDetailDto userDetail) {
        final boolean isLong = TYPE_OPERATION_LONG.equalsIgnoreCase(copyOperation.getTypeOperation());
        final int leverage = normalizeLeverage(userDetail.getDetail().getLeverage());

        return OperationDto.builder()
                .symbol(copyOperation.getParsymbol())
                .side(isLong ? Side.SELL : Side.BUY)
                .type(OrderType.MARKET)
                .positionSide(isLong ? PositionSide.LONG : PositionSide.SHORT)
                .quantity(copyOperation.getSizePar().toPlainString())
                .leverage(leverage)
                .reduceOnly(true)
                .apiKey(userDetail.getUserApiKey().getApiKey())
                .secret(userDetail.getUserApiKey().getApiSecret())
                .build();
    }

    private CopyOperationDto buildCopyNewOperationDto(BinanceFuturesOrderClientResponse order,
                                                      String idWallet,
                                                      UUID idOperation,
                                                      String idUser,
                                                      int leverage) {

        final BigDecimal qty = Objects.requireNonNull(order.getOrigQty(), ERR_ORIG_QTY_NULL);
        final BigDecimal avgPrice = Objects.requireNonNull(order.getAvgPrice(), ERR_AVG_PRICE_NULL);
        final BigDecimal countUsd = qty.multiply(avgPrice);

        return CopyOperationDto.builder()
                .idOrden(order.getOrderId().toString())
                .idOrderOrigin(idOperation.toString())
                .idUser(idUser)
                .idWalletOrigin(idWallet)
                .parsymbol(order.getSymbol())
                .typeOperation(order.getPositionSide())
                .leverage(BigDecimal.valueOf(leverage))
                .siseUsd(countUsd)
                .sizePar(qty)
                .priceEntry(avgPrice)
                .priceClose(null)
                .dateCreation(OffsetDateTime.ofInstant(
                        Instant.ofEpochMilli(order.getUpdateTime()),
                        ZoneOffset.UTC))
                .active(true)
                .build();
    }

    private CopyOperationDto buildCopyCloseOperationDto(CopyOperationDto operation, BinanceFuturesOrderClientResponse order) {
        final BigDecimal qty = Objects.requireNonNull(order.getOrigQty(), ERR_ORIG_QTY_NULL);
        final BigDecimal avgPrice = Objects.requireNonNull(order.getAvgPrice(), ERR_AVG_PRICE_NULL);
        final BigDecimal countUsd = qty.multiply(avgPrice);

        return CopyOperationDto.builder()
                .idOrden(order.getOrderId().toString())
                .idOrderOrigin(operation.getIdOrderOrigin())
                .idUser(operation.getIdUser())
                .idWalletOrigin(operation.getIdWalletOrigin())
                .parsymbol(order.getSymbol())
                .typeOperation(order.getPositionSide())
                .leverage(operation.getLeverage())
                .siseUsd(countUsd)
                .sizePar(qty)
                .priceEntry(operation.getPriceEntry())
                .priceClose(avgPrice)
                .dateCreation(operation.getDateCreation())
                .dateClose(OffsetDateTime.ofInstant(
                        Instant.ofEpochMilli(order.getUpdateTime()),
                        ZoneOffset.UTC))
                .active(false)
                .build();
    }

    private boolean isValidOrderResponse(BinanceFuturesOrderClientResponse response) {
        return response != null
                && response.getOrderId() != null
                && response.getSymbol() != null
                && response.getOrigQty() != null
                && response.getAvgPrice() != null
                && response.getAvgPrice().compareTo(BigDecimal.ZERO) > 0;
    }

    private BigDecimal safeNotional(BinanceFuturesOrderClientResponse response) {
        return response.getOrigQty().multiply(response.getAvgPrice());
    }

    private int normalizeLeverage(int requested) {
        return Math.max(1, requested);
    }

    private BigDecimal normalizeBufferPct(BigDecimal pct) {
        if (pct == null) return BigDecimal.ZERO;
        if (pct.compareTo(BigDecimal.ZERO) < 0) return BigDecimal.ZERO;
        if (pct.compareTo(new BigDecimal("0.30")) > 0) return new BigDecimal("0.30");
        return pct;
    }

    private String userWalletKey(String userId, String walletId) {
        return userId + ":" + walletId;
    }

    private String copyKey(String originId, String userId) {
        return originId + ":" + userId;
    }

    private static final class Reservation {
        private final String userWalletKey;
        private final BigDecimal margin;

        private Reservation(String userWalletKey, BigDecimal margin) {
            this.userWalletKey = userWalletKey;
            this.margin = margin;
        }
    }

    private static final class PreparedOpen {
        private final OperationDto dto;
        private final String userWalletKey;
        private final BigDecimal walletMarginBudget;
        private final BigDecimal marginRequired;
        private final BigDecimal notional;
        private final int leverage;
        private final String symbol;

        private PreparedOpen(OperationDto dto,
                             String userWalletKey,
                             BigDecimal walletMarginBudget,
                             BigDecimal marginRequired,
                             BigDecimal notional,
                             int leverage,
                             String symbol) {
            this.dto = dto;
            this.userWalletKey = userWalletKey;
            this.walletMarginBudget = walletMarginBudget;
            this.marginRequired = marginRequired;
            this.notional = notional;
            this.leverage = leverage;
            this.symbol = symbol;
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
}
