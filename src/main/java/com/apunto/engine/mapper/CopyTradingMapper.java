package com.apunto.engine.mapper;

import com.apunto.engine.dto.client.BinanceFuturesOrderClientResponse;
import com.apunto.engine.dto.CopyOperationDto;
import com.apunto.engine.dto.OperationDto;
import com.apunto.engine.dto.UserDetailDto;
import com.apunto.engine.shared.enums.OrderType;
import com.apunto.engine.shared.enums.PositionSide;
import com.apunto.engine.shared.enums.Side;
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

    default OperationDto buildClosePosition(CopyOperationDto copyOperation, UserDetailDto userDetail) {
        if ("LONG".equals(copyOperation.getTypeOperation())) {
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
        } else {
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

    @Mapping(target = "idOrden", expression = "java(order.getOrderId().toString())")
    @Mapping(target = "idOrderOrigin", expression = "java(idOperation.toString())")
    @Mapping(target = "idUser", source = "idUser")
    @Mapping(target = "idWalletOrigin", source = "idWallet")
    @Mapping(target = "parsymbol", source = "order.symbol")
    @Mapping(target = "typeOperation", source = "order.positionSide")
    @Mapping(target = "leverage", expression = "java(new java.math.BigDecimal(leverage))")
    @Mapping(target = "sizePar", source = "order.origQty")
    @Mapping(target = "priceEntry", source = "order.avgPrice")
    @Mapping(target = "priceClose", constant = "null")
    @Mapping(target = "dateCreation", expression = "java(toUtcOffsetDateTime(order.getUpdateTime()))")
    @Mapping(target = "dateClose", constant = "null")
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
        BigDecimal countUsd = order.getOrigQty().multiply(order.getAvgPrice());
        target.setSiseUsd(countUsd);
    }

    @Mapping(target = "idOrden", expression = "java(order.getOrderId().toString())")
    @Mapping(target = "idOrderOrigin", source = "operation.idOrderOrigin")
    @Mapping(target = "idUser", source = "operation.idUser")
    @Mapping(target = "idWalletOrigin", source = "operation.idWalletOrigin")
    @Mapping(target = "parsymbol", source = "order.symbol")
    @Mapping(target = "typeOperation", source = "order.positionSide")
    @Mapping(target = "leverage", source = "operation.leverage")
    @Mapping(target = "sizePar", source = "order.origQty")
    @Mapping(target = "priceEntry", source = "operation.priceEntry")
    @Mapping(target = "priceClose", source = "order.avgPrice")
    @Mapping(target = "dateCreation", source = "operation.dateCreation")
    @Mapping(target = "dateClose", expression = "java(toUtcOffsetDateTime(order.getUpdateTime()))")
    @Mapping(target = "active", constant = "false")
    @Mapping(target = "siseUsd", ignore = true)
    CopyOperationDto buildCopyCloseOperationDto(CopyOperationDto operation, BinanceFuturesOrderClientResponse order);

    @AfterMapping
    default void fillSiseUsdClose(@MappingTarget CopyOperationDto target,
                                  CopyOperationDto operation,
                                  BinanceFuturesOrderClientResponse order) {
        BigDecimal countUsd = order.getOrigQty().multiply(order.getAvgPrice());
        target.setSiseUsd(countUsd);
    }

    default OffsetDateTime toUtcOffsetDateTime(Long epochMillis) {
        if (epochMillis == null) return null;
        return OffsetDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), ZoneOffset.UTC);
    }
}

