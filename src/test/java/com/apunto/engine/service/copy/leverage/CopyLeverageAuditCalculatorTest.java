package com.apunto.engine.service.copy.leverage;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CopyLeverageAuditCalculatorTest {

    @Test
    void openLongCapsSourceLeverageWhenPolicyCapIsLower() {
        CopyLeverageSnapshot snapshot = CopyLeverageAuditCalculator.evaluateOpen(CopyLeverageAuditInput.builder()
                .sourceLeverageX(bd("10"))
                .sourceNotionalUsd(bd("100"))
                .sourceMarginUsd(bd("10"))
                .leverageCapX(bd("5"))
                .build());

        assertEquals(bd("5"), snapshot.shadowAppliedLeverageX());
        assertTrue(snapshot.leverageWasCapped());
        assertEquals("LEVERAGE_CAPPED_BY_POLICY", snapshot.leverageCapReason());
        assertEquals(CopyLeverageStatus.LEVERAGE_CAPPED, snapshot.leverageStatus());
        assertEquals(bd("10"), snapshot.sourceEffectiveLeverageX());
    }

    @Test
    void openShortKeepsSourceLeverageBelowCap() {
        CopyLeverageSnapshot snapshot = CopyLeverageAuditCalculator.evaluateOpen(CopyLeverageAuditInput.builder()
                .sourceLeverageX(bd("3"))
                .shadowRequestedLeverageX(bd("3"))
                .shadowAppliedLeverageX(bd("3"))
                .leverageCapX(bd("5"))
                .build());

        assertEquals(bd("3"), snapshot.shadowAppliedLeverageX());
        assertFalse(snapshot.leverageWasCapped());
        assertEquals(CopyLeverageStatus.OK, snapshot.leverageStatus());
    }

    @Test
    void missingSourceLeverageIsExplicitAndCanStillProduceConservativeShadowCap() {
        CopyLeverageSnapshot snapshot = CopyLeverageAuditCalculator.evaluateOpen(CopyLeverageAuditInput.builder()
                .leverageCapX(bd("5"))
                .shadowNotionalUsd(bd("50"))
                .shadowRequiredMarginUsd(bd("10"))
                .build());

        assertEquals(CopyLeverageStatus.MISSING_SOURCE_LEVERAGE, snapshot.leverageStatus());
        assertEquals(bd("5"), snapshot.shadowAppliedLeverageX());
    }

    @Test
    void liveRequestedAndExchangeLeverageEqualIsOk() {
        CopyLeverageSnapshot snapshot = CopyLeverageAuditCalculator.evaluateOpen(CopyLeverageAuditInput.builder()
                .sourceLeverageX(bd("5"))
                .shadowAppliedLeverageX(bd("5"))
                .liveRequestedLeverageX(bd("5"))
                .liveExchangeLeverageX(bd("5"))
                .liveNotionalUsd(bd("50"))
                .liveRequiredMarginUsd(bd("10"))
                .requireLiveExchangeLeverage(true)
                .build());

        assertEquals(CopyLeverageStatus.OK, snapshot.leverageStatus());
        assertEquals(bd("5"), snapshot.liveEffectiveLeverageX());
    }

    @Test
    void liveLeverageMismatchIsExplicit() {
        CopyLeverageSnapshot snapshot = CopyLeverageAuditCalculator.evaluateOpen(CopyLeverageAuditInput.builder()
                .sourceLeverageX(bd("5"))
                .liveRequestedLeverageX(bd("5"))
                .liveExchangeLeverageX(bd("10"))
                .requireLiveExchangeLeverage(true)
                .build());

        assertEquals(CopyLeverageStatus.LEVERAGE_MISMATCH, snapshot.leverageStatus());
    }

    @Test
    void missingLiveExchangeLeverageIsExplicitWhenRequired() {
        CopyLeverageSnapshot snapshot = CopyLeverageAuditCalculator.evaluateOpen(CopyLeverageAuditInput.builder()
                .sourceLeverageX(bd("5"))
                .liveRequestedLeverageX(bd("5"))
                .requireLiveExchangeLeverage(true)
                .build());

        assertEquals(CopyLeverageStatus.MISSING_LIVE_EXCHANGE_LEVERAGE, snapshot.leverageStatus());
    }

    @Test
    void reduceAndCloseInheritLivePositionLeverage() {
        CopyLeverageSnapshot snapshot = CopyLeverageAuditCalculator.inheritedForReduction(
                bd("4"),
                bd("40"),
                bd("10"),
                "CROSSED",
                "live_position"
        );

        assertEquals(CopyLeverageStatus.OK, snapshot.leverageStatus());
        assertEquals(bd("4"), snapshot.liveExchangeLeverageX());
        assertEquals(bd("4"), snapshot.liveEffectiveLeverageX());
    }

    @Test
    void invalidLeverageIsNotAllowed() {
        CopyLeverageSnapshot snapshot = CopyLeverageAuditCalculator.evaluateOpen(CopyLeverageAuditInput.builder()
                .sourceLeverageX(BigDecimal.ZERO)
                .build());

        assertEquals(CopyLeverageStatus.INVALID_LEVERAGE, snapshot.leverageStatus());
    }

    @Test
    void logFieldsContainLeverageAuditKeys() {
        CopyLeverageSnapshot snapshot = CopyLeverageAuditCalculator.evaluateOpen(CopyLeverageAuditInput.builder()
                .sourceLeverageX(bd("5"))
                .shadowAppliedLeverageX(bd("5"))
                .liveRequestedLeverageX(bd("5"))
                .liveExchangeLeverageX(bd("5"))
                .liveNotionalUsd(bd("50"))
                .liveRequiredMarginUsd(bd("10"))
                .requireLiveExchangeLeverage(true)
                .build());

        String fields = snapshot.logFields();

        assertTrue(fields.contains("sourceLeverageX=5"));
        assertTrue(fields.contains("shadowAppliedLeverageX=5"));
        assertTrue(fields.contains("liveRequestedLeverageX=5"));
        assertTrue(fields.contains("liveExchangeLeverageX=5"));
        assertTrue(fields.contains("liveEffectiveLeverageX=5"));
        assertTrue(fields.contains("leverageStatus=OK"));
        assertFalse(fields.contains("\n"));
    }

    private static BigDecimal bd(String value) {
        return new BigDecimal(value).stripTrailingZeros();
    }
}
