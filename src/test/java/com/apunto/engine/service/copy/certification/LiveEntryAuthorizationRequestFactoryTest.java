package com.apunto.engine.service.copy.certification;

import com.apunto.engine.dto.OperationDto;
import com.apunto.engine.entity.UserCopyAllocationEntity;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LiveEntryAuthorizationRequestFactoryTest {

    private final LiveEntryAuthorizationRequestFactory factory =
            new LiveEntryAuthorizationRequestFactory(new LiveCertificationRuntimeProperties());

    @Test
    void createsTheExactRuntimeIdentityFromOrderAndAllocationMetadata() {
        LiveEntryAuthorizationContext context = factory.create(operation(), allocation());

        assertTrue(context.valid());
        assertEquals(0, new BigDecimal("250").compareTo(context.request().allocatedCapitalUsd()));
        assertEquals(0, new BigDecimal("5").compareTo(context.request().targetLeverage()));
        assertEquals("copy-strategy-v3", context.request().strategyVersion());
        assertEquals("USDC", context.request().quoteAsset());
    }

    @Test
    void missingAllocatedCapitalFailsClosedBeforeDatabaseLookup() {
        OperationDto operation = operation();
        operation.setTargetAllocatedCapitalUsd(null);

        LiveEntryAuthorizationContext context = factory.create(operation, allocation());

        assertFalse(context.valid());
        assertEquals("LIVE_CERTIFICATION_RUNTIME_CONTEXT_INCOMPLETE", context.reasonCode());
    }

    private OperationDto operation() {
        return OperationDto.builder()
                .userId(UUID.fromString("22222222-2222-2222-2222-222222222222").toString())
                .walletId("0xabc")
                .targetAllocatedCapitalUsd(new BigDecimal("250"))
                .targetLeverage(new BigDecimal("5"))
                .exchange("BINANCE")
                .quoteAsset("USDC")
                .build();
    }

    private UserCopyAllocationEntity allocation() {
        return UserCopyAllocationEntity.builder()
                .id(55L)
                .walletId("0xabc")
                .copyStrategyCode("MOVEMENT_ALL")
                .scopeType("ALL")
                .scopeValue("ALL")
                .build();
    }
}
