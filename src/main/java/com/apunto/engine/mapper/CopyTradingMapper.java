package com.apunto.engine.mapper;

import com.apunto.engine.dto.client.BinanceFuturesOrderClientResponse;
import com.apunto.engine.dto.CopyOperationDto;
import com.apunto.engine.dto.OperationDto;
import com.apunto.engine.dto.UserDetailDto;
import com.apunto.engine.shared.enums.OrderType;
import com.apunto.engine.shared.enums.PositionSide;
import com.apunto.engine.shared.enums.Side;
import com.apunto.engine.shared.util.IdempotencyKeyUtil;
import org.mapstruct.AfterMapping;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.MappingTarget;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

@Mapper(componentModel = "spring")
public interface CopyTradingMapper {

    /**
     * Binance puede devolver origQty distinto a executedQty (ej. fills parciales).
     * Para persistencia y cierres, preferimos la cantidad realmente ejecutada.
     */
    default BigDecimal resolveFilledQty(BinanceFuturesOrderClientResponse order) {
        if (order == null) return null;

        // Preferimos executedQty, luego cumQty. origQty solo es seguro si Binance ya marco fill.
        BigDecimal q = order.getExecutedQty();
        if (q == null || q.compareTo(BigDecimal.ZERO) <= 0) {
            q = order.getCumQty();
        }
        if ((q == null || q.compareTo(BigDecimal.ZERO) <= 0) && hasFillStatus(order.getStatus())) {
            q = order.getOrigQty();
        }
        if (q == null || q.compareTo(BigDecimal.ZERO) <= 0) {
            return null;
        }
        return q;
    }

    default boolean hasFillStatus(String status) {
        if (status == null || status.isBlank()) {
            return false;
        }
        String normalized = status.trim().toUpperCase(java.util.Locale.ROOT);
        return "FILLED".equals(normalized)
                || "PARTIALLY_FILLED".equals(normalized)
                || "PARTIAL_FILLED".equals(normalized);
    }

    default BigDecimal resolveExecutionPrice(BinanceFuturesOrderClientResponse order) {
        if (order == null) return null;
        if (order.getAvgPrice() != null && order.getAvgPrice().compareTo(BigDecimal.ZERO) > 0) {
            return order.getAvgPrice();
        }
        if (order.getPrice() != null && order.getPrice().compareTo(BigDecimal.ZERO) > 0) {
            return order.getPrice();
        }
        BigDecimal q = resolveFilledQty(order);
        if (q != null && q.compareTo(BigDecimal.ZERO) > 0
                && order.getCumQuote() != null
                && order.getCumQuote().compareTo(BigDecimal.ZERO) > 0) {
            return order.getCumQuote().divide(q, 18, java.math.RoundingMode.HALF_UP);
        }
        return null;
    }

    default BigDecimal resolvePersistedEntryPrice(BinanceFuturesOrderClientResponse order) {
        BigDecimal execution = resolveExecutionPrice(order);
        if (execution != null && execution.compareTo(BigDecimal.ZERO) > 0) return execution;
        return order != null && order.getReferencePrice() != null && order.getReferencePrice().compareTo(BigDecimal.ZERO) > 0
                ? order.getReferencePrice() : null;
    }

    default OperationDto buildClosePosition(CopyOperationDto copyOperation, UserDetailDto userDetail) {

        if ("LONG".equals(copyOperation.getTypeOperation())) {
            return OperationDto.builder()
                    .symbol(copyOperation.getParsymbol())
                    .side(Side.SELL)
                    .type(OrderType.MARKET)
                    .positionSide(PositionSide.LONG)
                    .quantity(copyOperation.getSizePar().toPlainString())
                    .leverage(null)
                    .clientOrderId(IdempotencyKeyUtil.closeClientOrderId(
                            copyOperation.getIdOrderOrigin(),
                            userDetail.getUser().getId().toString(),
                            copyOperation.getIdWalletOrigin(),
                            copyOperation.getUserCopyAllocationId(),
                            copyOperation.getCopyStrategyCode()))
                    .originId(copyOperation.getIdOrderOrigin())
                    .userId(userDetail.getUser().getId().toString())
                    .walletId(copyOperation.getIdWalletOrigin())
                    .reduceOnly(true)
                    .configureAccountSettings(false)
                    .apiKey(userDetail.getUserApiKey().getApiKey())
                    .secret(userDetail.getUserApiKey().getApiSecret())
                    .build();
        } else {
            return OperationDto.builder()
                    .symbol(copyOperation.getParsymbol())
                    .side(Side.BUY)
                    .type(OrderType.MARKET)
                    .positionSide(PositionSide.SHORT)
                    .quantity(copyOperation.getSizePar().toPlainString())
                    .leverage(null)
                    .clientOrderId(IdempotencyKeyUtil.closeClientOrderId(
                            copyOperation.getIdOrderOrigin(),
                            userDetail.getUser().getId().toString(),
                            copyOperation.getIdWalletOrigin(),
                            copyOperation.getUserCopyAllocationId(),
                            copyOperation.getCopyStrategyCode()))
                    .originId(copyOperation.getIdOrderOrigin())
                    .userId(userDetail.getUser().getId().toString())
                    .walletId(copyOperation.getIdWalletOrigin())
                    .reduceOnly(true)
                    .configureAccountSettings(false)
                    .apiKey(userDetail.getUserApiKey().getApiKey())
                    .secret(userDetail.getUserApiKey().getApiSecret())
                    .build();
        }
    }

    @Mapping(target = "idOrden", expression = "java(order.getOrderId().toString())")
    @Mapping(target = "idOrderOrigin", expression = "java(idOperation.toString())")
    @Mapping(target = "idUser", source = "idUser")
    @Mapping(target = "idWalletOrigin", source = "idWallet")
    @Mapping(target = "parsymbol", source = "order.symbol")
    @Mapping(target = "typeOperation", source = "order.positionSide")
    @Mapping(target = "leverage", expression = "java(new java.math.BigDecimal(leverage))")
    @Mapping(target = "sizePar", expression = "java(resolveFilledQty(order))")
    @Mapping(target = "priceEntry", expression = "java(resolvePersistedEntryPrice(order))")
    @Mapping(target = "priceClose", ignore = true)
    @Mapping(target = "dateClose", ignore = true)
    @Mapping(target = "dateCreation", expression = "java(toUtcOffsetDateTimeOrNow(order.getUpdateTime()))")
    @Mapping(target = "dispatchIntentId", source = "order.dispatchIntentId")
    @Mapping(target = "sourceEventId", source = "order.sourceEventId")
    @Mapping(target = "clientOrderId", source = "order.clientOrderId")
    @Mapping(target = "priceStatus", source = "order.averagePriceStatus")
    @Mapping(target = "active", constant = "true")
    @Mapping(target = "siseUsd", ignore = true)
    CopyOperationDto buildCopyNewOperationDto(BinanceFuturesOrderClientResponse order,
                                              String idWallet,
                                              UUID idOperation,
                                              String idUser,
                                              int leverage);

    @AfterMapping
    default void fillSiseUsdNew(@MappingTarget CopyOperationDto target,
                                BinanceFuturesOrderClientResponse order,
                                String idWallet,
                                UUID idOperation,
                                String idUser,
                                int leverage) {
        BigDecimal q = resolveFilledQty(order);
        BigDecimal price = resolvePersistedEntryPrice(order);
        BigDecimal countUsd = (q == null ? BigDecimal.ZERO : q).multiply(price == null ? BigDecimal.ZERO : price);
        target.setSiseUsd(countUsd);
    }

    @Mapping(target = "idOrden", expression = "java(order.getOrderId().toString())")
    @Mapping(target = "idOrderOrigin", source = "operation.idOrderOrigin")
    @Mapping(target = "idUser", source = "operation.idUser")
    @Mapping(target = "idWalletOrigin", source = "operation.idWalletOrigin")
    @Mapping(target = "parsymbol", source = "order.symbol")
    @Mapping(target = "typeOperation", source = "order.positionSide")
    @Mapping(target = "leverage", source = "operation.leverage")
    @Mapping(target = "sizePar", expression = "java(resolveFilledQty(order))")
    @Mapping(target = "priceEntry", source = "operation.priceEntry")
    @Mapping(target = "priceClose", expression = "java(resolveExecutionPrice(order))")
    @Mapping(target = "dateCreation", source = "operation.dateCreation")
    @Mapping(target = "dateClose", expression = "java(toUtcOffsetDateTimeOrNow(order.getUpdateTime()))")
    @Mapping(target = "dispatchIntentId", source = "order.dispatchIntentId")
    @Mapping(target = "sourceEventId", source = "order.sourceEventId")
    @Mapping(target = "clientOrderId", source = "order.clientOrderId")
    @Mapping(target = "priceStatus", source = "order.averagePriceStatus")
    @Mapping(target = "economicCycleId", source = "operation.economicCycleId")
    @Mapping(target = "cycleSequence", source = "operation.cycleSequence")
    @Mapping(target = "economicDataStatus", expression = "java(order.getEconomicDataStatus() == null || order.getEconomicDataStatus().isBlank() ? operation.getEconomicDataStatus() : order.getEconomicDataStatus())")
    @Mapping(target = "active", constant = "false")
    @Mapping(target = "siseUsd", ignore = true)
    CopyOperationDto buildCopyCloseOperationDto(CopyOperationDto operation, BinanceFuturesOrderClientResponse order);

    @AfterMapping
    default void fillSiseUsdClose(@MappingTarget CopyOperationDto target,
                                  CopyOperationDto operation,
                                  BinanceFuturesOrderClientResponse order) {
        BigDecimal q = resolveFilledQty(order);
        BigDecimal price = resolveExecutionPrice(order);
        BigDecimal countUsd = (q == null ? BigDecimal.ZERO : q).multiply(price == null ? BigDecimal.ZERO : price);
        target.setSiseUsd(countUsd);
    }

    default OffsetDateTime toUtcOffsetDateTime(Long epochMillis) {
        if (epochMillis == null) return null;
        return OffsetDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), ZoneOffset.UTC);
    }

    default OffsetDateTime toUtcOffsetDateTimeOrNow(Long epochMillis) {
        return epochMillis == null ? OffsetDateTime.now(ZoneOffset.UTC) : toUtcOffsetDateTime(epochMillis);
    }
}
