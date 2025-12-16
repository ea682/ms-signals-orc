package com.apunto.engine.service.impl;

import com.apunto.engine.dto.CloseOperationDto;
import com.apunto.engine.dto.CopyOperationDto;
import com.apunto.engine.dto.OperationDto;
import com.apunto.engine.dto.UserDetailDto;
import com.apunto.engine.dto.client.*;
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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
@RequiredArgsConstructor
public class BinanceEngineServiceImpl implements BinanceEngineService {

    private static final double DEFAULT_BASE_CAPITAL = 1_000.0;
    private static final int SCALE_DECIMALS = 6;
    private static final BigDecimal MIN_NOTIONAL_FALLBACK = BigDecimal.valueOf(7);
    private static final int DEFAULT_CALC_SCALE = 12;

    private final ProcesBinanceService procesBinanceService;
    private final ThreadPoolTaskScheduler binanceTaskScheduler;
    private final MetricWalletService metricWalletService;
    private final CopyOperationService copyOperationService;

    @Value("${binance.dispatch.delay-ms:100}")
    private long delayBetweenMs;

    @Value("${engine.total-capital-usdt:1000}")
    private double totalCapitalUsdt;

    private static final long TTL_MS = 60_000L;
    private final ConcurrentMap<String, Long> processedOperations = new ConcurrentHashMap<>();

    @Override
    public void openOperation(OperacionEvent event, List<UserDetailDto> usersDetail) {
        Objects.requireNonNull(event, "El evento de operación no puede ser null");
        Objects.requireNonNull(usersDetail, "La lista de usuarios no puede ser null");

        long startNs = System.nanoTime();

        String operationKey = String.valueOf(event.getOperacion().getIdOperacion());
        long now = System.currentTimeMillis();
        boolean[] isNew = {false};

        processedOperations.compute(operationKey, (key, previousTs) -> {
            if (previousTs == null || now - previousTs > TTL_MS) {
                isNew[0] = true;
                return now;
            }
            return previousTs;
        });

        if (!isNew[0]) {
            log.warn("operation.duplicate_kafka_event operationKey={} action=ignore", operationKey);
            return;
        }

        processedOperations.entrySet().removeIf(e -> now - e.getValue() > TTL_MS * 5);

        final long baseTime = System.currentTimeMillis();
        int index = 0;

        final Map<Integer, List<MetricaWalletDto>> metricsByMaxWallet = new HashMap<>();

        for (UserDetailDto userDetail : usersDetail) {
            Integer maxWallet = userDetail.getDetail().getMaxWallet();

            List<MetricaWalletDto> metrics = metricsByMaxWallet.computeIfAbsent(
                    maxWallet,
                    metricWalletService::getMetricWallets
            );

            final MetricaWalletDto walletMetric =
                    getWalletMetricForOperation(event.getOperacion().getIdCuenta(), metrics);

            if (walletMetric != null) {
                final long delay = index * delayBetweenMs;
                final Instant executionTime = Instant.ofEpochMilli(baseTime + delay);

                binanceTaskScheduler.schedule(
                        () -> executeNewOperation(event, userDetail, walletMetric),
                        executionTime
                );
            } else {
                log.warn("operation.no_metric_found idWallet={} userId={} maxWallet={}",
                        event.getOperacion().getIdCuenta(),
                        userDetail.getUser().getId(),
                        maxWallet);
            }

            index++;
        }

        long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs);
        log.info("operation.open_scheduled opId={} users={} uniqueMaxWallets={} delayBetweenMs={} durationMs={}",
                event.getOperacion().getIdOperacion(),
                usersDetail.size(),
                metricsByMaxWallet.size(),
                delayBetweenMs,
                durationMs);
    }

    @Override
    public void closeOperation(OperacionEvent event, List<UserDetailDto> usersDetail) {
        Objects.requireNonNull(event, "El evento de operación no puede ser null");
        Objects.requireNonNull(usersDetail, "La lista de usuarios no puede ser null");

        final long baseTime = System.currentTimeMillis();
        int index = 0;

        final String operationId = event.getOperacion().getIdOperacion().toString();
        final CopyOperationDto copyOperation = copyOperationService.findOperation(operationId);

        if (copyOperation == null) {
            log.warn("No se encontró la operación {}. No se programarán cierres.", operationId);
            return;
        }

        if (!copyOperation.isActive()) {
            log.info("La operación {} está inactiva. No se programarán cierres.", operationId);
            return;
        }

        for (UserDetailDto userDetail : usersDetail) {
            final long delay = index * delayBetweenMs;
            final Instant executionTime = Instant.ofEpochMilli(baseTime + delay);

            binanceTaskScheduler.schedule(
                    () -> safelyExecuteCloseOperation(copyOperation, userDetail, operationId),
                    executionTime
            );

            index++;
        }

        log.info(
                "Programadas {} operaciones de cierre hacia Binance con un delay de {}ms (operación {}).",
                usersDetail.size(), delayBetweenMs, operationId
        );
    }

    private void safelyExecuteCloseOperation(CopyOperationDto copyOperation,
                                             UserDetailDto userDetail,
                                             String operationId) {
        try {
            executeCloseOperation(copyOperation, userDetail);

        } catch (Exception ex) {
            log.error("Error ejecutando cierre de operación {} para user {}: {}",
                    operationId, userDetail.getUser().getId(), ex.getMessage(), ex);
        }
    }

    private void executeNewOperation(OperacionEvent event,
                                     UserDetailDto userDetail,
                                     MetricaWalletDto walletMetric) {
        try {
            final OperationDto dto = buildNewOperationDto(event, userDetail, walletMetric);

            log.debug("Enviando newOperation para userId={} symbol={} qty={} leverage={}",
                    userDetail.getUser().getId(),
                    dto.getSymbol(),
                    dto.getQuantity(),
                    userDetail.getDetail().getLeverage());

            final BinanceFuturesOrderClientResponse response =
                    procesBinanceService.operationPosition(dto);

            createNewOperation(
                    response,
                    walletMetric.getIdWallet(),
                    event.getOperacion().getIdOperacion(),
                    userDetail.getUser().getId().toString(),
                    userDetail.getDetail().getLeverage()
            );

            log.info("Ejecución exitosa de apertura en Binance para userId={} symbol={}",
                    userDetail.getUser().getId(), dto.getSymbol());
        } catch (Exception ex) {
            log.error("Error ejecutando newOperation para userId={}",
                    userDetail.getUser().getId(), ex);
        }
    }


    private OperationDto buildNewOperationDto(OperacionEvent event,
                                                 UserDetailDto userDetail,
                                                 MetricaWalletDto walletMetric) {
        Objects.requireNonNull(event, "event no puede ser null");
        Objects.requireNonNull(userDetail, "userDetail no puede ser null");
        Objects.requireNonNull(walletMetric, "walletMetric no puede ser null");

        final Double capitalShare = walletMetric.getCapitalShare();
        if (capitalShare == null || capitalShare <= 0) {
            throw new IllegalStateException(
                    "capitalShare inválido para wallet " + walletMetric.getIdWallet()
            );
        }

        final double capitalWallet = userDetail.getDetail().getCapital() * capitalShare;

        final double baseCapital = Optional.ofNullable(walletMetric.getCapitalRequired())
                .filter(required -> required > 0)
                .orElse(DEFAULT_BASE_CAPITAL);

        final BigDecimal sizeOriginal = event.getOperacion().getSize();
        if (sizeOriginal == null || sizeOriginal.compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException(
                    "Size inválido para operación: " + event.getOperacion().getIdOperacion()
            );
        }

        final BigDecimal baseCapitalBd = BigDecimal.valueOf(baseCapital);
        BigDecimal fractionOfBaseUsed = sizeOriginal
                .divide(baseCapitalBd, 10, RoundingMode.HALF_UP);

        if (fractionOfBaseUsed.compareTo(BigDecimal.ONE) > 0) {
            fractionOfBaseUsed = BigDecimal.ONE;
        }
        if (fractionOfBaseUsed.compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalStateException(
                    "fractionOfBaseUsed <= 0 para operación " + event.getOperacion().getIdOperacion()
            );
        }

        final BigDecimal capitalWalletBd = BigDecimal.valueOf(capitalWallet);
        final BigDecimal capitalThisTrade = capitalWalletBd.multiply(fractionOfBaseUsed);

        final BigDecimal entryPrice = event.getOperacion().getPrecioEntrada();
        if (entryPrice == null || entryPrice.compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException(
                    "Precio de entrada inválido para operación: " + event.getOperacion().getIdOperacion()
            );
        }

        BigDecimal quantity = capitalThisTrade
                .divide(entryPrice, SCALE_DECIMALS, RoundingMode.DOWN);

        if (quantity.compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalStateException(
                    "quantity calculada <= 0 para operación " + event.getOperacion().getIdOperacion()
            );
        }

        quantity = adjustQuantityToBinanceRules(
                event.getOperacion().getParSymbol(),
                quantity,
                entryPrice
        );

        log.debug(
                "Escalado operación wallet={} capitalShare={} capitalWallet={} baseCapital={} sizeOriginal={} fractionOfBaseUsed={} capitalThisTrade={} quantity={}",
                walletMetric.getIdWallet(),
                capitalShare,
                capitalWallet,
                baseCapital,
                sizeOriginal.toPlainString(),
                fractionOfBaseUsed.toPlainString(),
                capitalThisTrade.toPlainString(),
                quantity.toPlainString()
        );

        return buildBuyAndSellPosition(event, quantity, userDetail);
    }


    private OperationDto buildBuyAndSellPosition(OperacionEvent event, BigDecimal quantity, UserDetailDto userDetail) {

        if(event.getOperacion().getTipoOperacion().equals(PositionSide.LONG)){
            return OperationDto.builder()
                    .symbol(event.getOperacion().getParSymbol())
                    .side(Side.BUY)
                    .type(OrderType.MARKET)
                    .positionSide(event.getOperacion().getTipoOperacion())
                    .quantity(quantity.toPlainString())
                    .leverage(userDetail.getDetail().getLeverage())
                    .reduceOnly(false)
                    .apiKey(userDetail.getUserApiKey().getApiKey())
                    .secret(userDetail.getUserApiKey().getApiSecret())
                    .build();
        }else{
            return OperationDto.builder()
                    .symbol(event.getOperacion().getParSymbol())
                    .side(Side.SELL)
                    .type(OrderType.MARKET)
                    .positionSide(event.getOperacion().getTipoOperacion())
                    .quantity(quantity.toPlainString())
                    .leverage(userDetail.getDetail().getLeverage())
                    .reduceOnly(false)
                    .apiKey(userDetail.getUserApiKey().getApiKey())
                    .secret(userDetail.getUserApiKey().getApiSecret())
                    .build();
        }
    }

    private MetricaWalletDto getWalletMetricForOperation(String idWalletOperation,
                                                         List<MetricaWalletDto> metrics) {
        if (metrics == null || metrics.isEmpty()) {
            return null;
        }

        return metrics.stream()
                .filter(w -> Objects.equals(w.getIdWallet(), idWalletOperation))
                .findFirst()
                .orElse(null);
    }

    private static final BigDecimal USER_MIN_NOTIONAL_USDT = new BigDecimal("7");

    private BigDecimal adjustQuantityToBinanceRules(String parSymbol,
                                                    BigDecimal quantity,
                                                    BigDecimal entryPrice) {

        if (quantity == null || quantity.compareTo(BigDecimal.ZERO) <= 0) {
            return BigDecimal.ZERO;
        }

        if (entryPrice == null || entryPrice.compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException("entryPrice inválido para " + parSymbol);
        }

        final List<BinanceFuturesSymbolInfoClientDto> symbols =
                procesBinanceService.getSymbols("S");

        final BinanceFuturesSymbolInfoClientDto symbolDetail = symbols.stream()
                .filter(s -> parSymbol.equalsIgnoreCase(s.getSymbol()))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "No se encontró información de símbolo para: " + parSymbol
                ));

        BigDecimal adjusted = quantity;
        BigDecimal stepSize = null;
        BigDecimal minQty = BigDecimal.ZERO;
        BigDecimal minNotionalFromExchange;
        BigDecimal effectiveMinNotional;
        final Integer quantityPrecision = symbolDetail.getQuantityPrecision();

        final BinanceFuturesSymbolFilterDto lotSizeFilter = symbolDetail.getFilters().stream()
                .filter(f -> "LOT_SIZE".equals(f.getFilterType()))
                .findFirst()
                .orElse(null);

        if (lotSizeFilter != null) {
            stepSize = new BigDecimal(lotSizeFilter.getStepSize());
            minQty = new BigDecimal(lotSizeFilter.getMinQty());

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
                .filter(f -> "MIN_NOTIONAL".equals(f.getFilterType())
                        || "NOTIONAL".equals(f.getFilterType()))
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

        int calcScale = DEFAULT_CALC_SCALE;
        if (quantityPrecision != null) {
            calcScale = Math.max(calcScale, quantityPrecision + 2);
        }
        if (stepSize != null) {
            final int stepScale = stepSize.stripTrailingZeros().scale();
            calcScale = Math.max(calcScale, stepScale + 2);
        }

        if (effectiveMinNotional.compareTo(BigDecimal.ZERO) > 0) {
            final BigDecimal currentNotional = adjusted.multiply(entryPrice);

            if (currentNotional.compareTo(effectiveMinNotional) < 0) {
                BigDecimal neededQty = effectiveMinNotional
                        .divide(entryPrice, calcScale, RoundingMode.UP);

                if (stepSize != null && stepSize.compareTo(BigDecimal.ZERO) > 0) {
                    neededQty = neededQty
                            .divide(stepSize, 0, RoundingMode.UP)
                            .multiply(stepSize);
                }

                adjusted = neededQty;
            }
        }

        if (quantityPrecision != null) {
            adjusted = adjusted.setScale(quantityPrecision, RoundingMode.DOWN);

            if (effectiveMinNotional.compareTo(BigDecimal.ZERO) > 0) {
                final BigDecimal notionalAfter = adjusted.multiply(entryPrice);

                if (notionalAfter.compareTo(effectiveMinNotional) < 0) {
                    final BigDecimal increment =
                            BigDecimal.ONE.movePointLeft(quantityPrecision);
                    adjusted = adjusted.add(increment);

                    if (stepSize != null && stepSize.compareTo(BigDecimal.ZERO) > 0) {
                        adjusted = adjusted
                                .divide(stepSize, 0, RoundingMode.UP)
                                .multiply(stepSize);
                    }

                    adjusted = adjusted.setScale(quantityPrecision, RoundingMode.DOWN);
                }
            }
        }

        log.debug("[adjustQuantityToBinanceRules] symbol={} qtyOriginal={} qtyFinal={} notionalFinal={}",
                parSymbol,
                quantity.toPlainString(),
                adjusted.toPlainString(),
                adjusted.multiply(entryPrice).toPlainString()
        );

        return adjusted;
    }

    private void createNewOperation(BinanceFuturesOrderClientResponse order,
                                    String idWallet,
                                    UUID idOperation,
                                    String idUser,
                                    int leverage) {

        CopyOperationDto buildCopyOperation = buildCopyNewOperationDto(order, idWallet, idOperation, idUser, leverage);

        copyOperationService.newOperation(buildCopyOperation);
    }

    private void executeCloseOperation(CopyOperationDto copyOperation, UserDetailDto userDetail) {



        final BinanceFuturesOrderClientResponse order =
                procesBinanceService.operationPosition(buildClosePosition(copyOperation, userDetail));
        copyOperation.setIdUser(userDetail.getUser().getId().toString());


        CopyOperationDto buildCopyOperation = buildCopyCloseOperationDto(copyOperation, order);

        copyOperationService.closeOperation(buildCopyOperation);
        log.info("Cierre de operación enviado a Binance para userId={} symbol={} qty={} respuesta={}",
                userDetail.getUser().getId(),
                copyOperation.getParsymbol(),
                copyOperation.getSizePar(),
                order);
    }

    private OperationDto buildClosePosition(CopyOperationDto copyOperation, UserDetailDto userDetail) {

        if(copyOperation.getTypeOperation().equals("LONG")){
            return OperationDto.builder()
                    .symbol(copyOperation.getParsymbol())
                    .side(Side.SELL)
                    .type(OrderType.MARKET)
                    .positionSide(PositionSide.LONG)
                    .quantity(copyOperation.getSizePar().toPlainString())
                    .leverage(userDetail.getDetail().getLeverage())
                    .reduceOnly(true)
                    .apiKey(userDetail.getUserApiKey().getApiKey())
                    .secret(userDetail.getUserApiKey().getApiSecret())
                    .build();
        }else{
            return OperationDto.builder()
                    .symbol(copyOperation.getParsymbol())
                    .side(Side.BUY)
                    .type(OrderType.MARKET)
                    .positionSide(PositionSide.SHORT)
                    .quantity(copyOperation.getSizePar().toPlainString())
                    .leverage(userDetail.getDetail().getLeverage())
                    .reduceOnly(true)
                    .apiKey(userDetail.getUserApiKey().getApiKey())
                    .secret(userDetail.getUserApiKey().getApiSecret())
                    .build();
        }
    }

    private CopyOperationDto buildCopyNewOperationDto(BinanceFuturesOrderClientResponse order,
                                                   String idWallet,
                                                   UUID idOperation,
                                                   String idUser,
                                                      int leverage) {
        final BigDecimal countUsd = order.getOrigQty().multiply(order.getAvgPrice());

        final CopyOperationDto copyOperationDto = CopyOperationDto.builder()
                .idOrden(order.getOrderId().toString())
                .idOrderOrigin(idOperation.toString())
                .idUser(idUser)
                .idWalletOrigin(idWallet)
                .parsymbol(order.getSymbol())
                .typeOperation(order.getPositionSide())
                .leverage(new BigDecimal(leverage))
                .siseUsd(countUsd)
                .sizePar(order.getOrigQty())
                .priceEntry(order.getAvgPrice())
                .priceClose(null)
                .dateCreation(OffsetDateTime.ofInstant(
                                Instant.ofEpochMilli(order.getUpdateTime()),
                                ZoneOffset.UTC))
                .active(true)
                .build();

        return copyOperationDto;
    }

    private CopyOperationDto buildCopyCloseOperationDto(CopyOperationDto operation, BinanceFuturesOrderClientResponse order) {
        final BigDecimal countUsd = order.getOrigQty().multiply(order.getAvgPrice());

        final CopyOperationDto copyOperationDto = CopyOperationDto.builder()
                .idOrden(order.getOrderId().toString())
                .idOrderOrigin(operation.getIdOrderOrigin())
                .idUser(operation.getIdUser())
                .idWalletOrigin(operation.getIdWalletOrigin())
                .parsymbol(order.getSymbol())
                .typeOperation(order.getPositionSide())
                .leverage(operation.getLeverage())
                .siseUsd(countUsd)
                .sizePar(order.getOrigQty())
                .priceEntry(operation.getPriceEntry())
                .priceClose(order.getAvgPrice())
                .dateCreation(operation.getDateCreation())
                .dateClose(OffsetDateTime.ofInstant(
                        Instant.ofEpochMilli(order.getUpdateTime()),
                        ZoneOffset.UTC))
                .active(false)
                .build();

        return copyOperationDto;

    }
}
