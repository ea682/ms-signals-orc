package com.apunto.engine.mapper;

import com.apunto.engine.dto.CopyOperationDto;
import com.apunto.engine.dto.OperationDto;
import com.apunto.engine.dto.UserDetailDto;
import com.apunto.engine.dto.client.BinanceFuturesOrderClientResponse;
import com.apunto.engine.entity.UserApiKeyEntity;
import com.apunto.engine.entity.UserEntity;
import com.apunto.engine.shared.enums.PositionSide;
import com.apunto.engine.shared.enums.Side;
import org.junit.jupiter.api.Test;
import org.mapstruct.factory.Mappers;

import java.math.BigDecimal;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CopyTradingMapperTest {

    private final CopyTradingMapper mapper = Mappers.getMapper(CopyTradingMapper.class);

    @Test
    void closeLongUsesHotPathWithoutAccountSettings() {
        OperationDto dto = mapper.buildClosePosition(copy("LONG"), userDetail());

        assertEquals(Side.SELL, dto.getSide());
        assertEquals(PositionSide.LONG, dto.getPositionSide());
        assertTrue(dto.isReduceOnly());
        assertFalse(dto.getConfigureAccountSettings());
    }

    @Test
    void closeShortUsesHotPathWithoutAccountSettings() {
        OperationDto dto = mapper.buildClosePosition(copy("SHORT"), userDetail());

        assertEquals(Side.BUY, dto.getSide());
        assertEquals(PositionSide.SHORT, dto.getPositionSide());
        assertTrue(dto.isReduceOnly());
        assertFalse(dto.getConfigureAccountSettings());
    }

    @Test
    void resolveFilledQtyDoesNotTreatNewOrigQtyAsExecuted() {
        BinanceFuturesOrderClientResponse order = new BinanceFuturesOrderClientResponse();
        order.setStatus("NEW");
        order.setOrigQty(new BigDecimal("0.25"));
        order.setExecutedQty(BigDecimal.ZERO);
        order.setCumQty(BigDecimal.ZERO);

        assertNull(mapper.resolveFilledQty(order));
    }

    @Test
    void resolveExecutionPriceCanUseCumQuoteWhenAvgPriceIsMissing() {
        BinanceFuturesOrderClientResponse order = new BinanceFuturesOrderClientResponse();
        order.setStatus("PARTIALLY_FILLED");
        order.setExecutedQty(new BigDecimal("0.5"));
        order.setCumQuote(new BigDecimal("50"));

        assertEquals(new BigDecimal("100.000000000000000000"), mapper.resolveExecutionPrice(order));
    }

    private CopyOperationDto copy(String typeOperation) {
        return CopyOperationDto.builder()
                .idOrderOrigin(UUID.randomUUID().toString())
                .idWalletOrigin("wallet-1")
                .parsymbol("BTCUSDT")
                .typeOperation(typeOperation)
                .sizePar(new BigDecimal("0.01"))
                .build();
    }

    private UserDetailDto userDetail() {
        UserEntity user = new UserEntity();
        user.setId(UUID.randomUUID());

        UserApiKeyEntity apiKey = new UserApiKeyEntity();
        apiKey.setApiKey("api-key");
        apiKey.setApiSecret("secret");

        return new UserDetailDto(user, null, apiKey);
    }
}
