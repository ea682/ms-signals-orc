package com.apunto.engine.service.copy.readiness;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ShadowLiveReadinessEvaluatorTest {

    private final ShadowLiveReadinessEvaluator evaluator =
            new ShadowLiveReadinessEvaluator(new ShadowLiveReadinessProperties());

    @Test
    void approvesWhenAllProductionGatesPass() {
        ShadowLiveReadinessDecision decision = evaluator.evaluate(baseReady().build());

        assertEquals(ShadowLiveReadinessStatus.APPROVED_FOR_LIVE, decision.status());
        assertTrue(decision.approvedForLive());
        assertEquals("SHADOW_READINESS_APPROVED", decision.primaryReasonCode());
        assertEquals("LIVE", decision.recommendedExecutionMode());
        assertEquals(new BigDecimal("100"), decision.capitalDecision().baseCapitalUSDT());
        assertEquals(new BigDecimal("100"), decision.capitalDecision().maxCapitalUSDT());
        assertEquals(false, decision.capitalDecision().canScale());
    }

    @Test
    void blocksWhenCopyGuardDoesNotAllowLive() {
        ShadowLiveReadinessDecision decision = evaluator.evaluate(baseReady()
                .copyGuardAllowsLive(false)
                .build());

        assertEquals(ShadowLiveReadinessStatus.BLOCKED, decision.status());
        assertTrue(decision.reasonCodes().contains("COPY_GUARD_BLOCKED"));
        assertTrue(decision.hardBlockers().contains("COPY_GUARD_BLOCKED"));
        assertEquals("SHADOW", decision.recommendedExecutionMode());
    }

    @Test
    void allowsMicroLiveWhenEvidenceIsHighButClosedPositionsAreLow() {
        ShadowLiveReadinessDecision decision = evaluator.evaluate(baseReady()
                .closedPositions(0L)
                .meaningfulMovements(120L)
                .evidenceScore(bd("75"))
                .riskClass("B")
                .build());

        assertEquals(ShadowLiveReadinessStatus.MICRO_LIVE_READY, decision.status());
        assertEquals("MICRO_LIVE", decision.recommendedExecutionMode());
        assertEquals("B", decision.riskClass());
        assertTrue(decision.softWarnings().contains("NEEDS_MORE_CLOSED_POSITIONS"));
        assertTrue(decision.reasonCodes().contains("NEEDS_MORE_CLOSED_POSITIONS"));
        assertTrue(decision.hardBlockers().isEmpty());
        assertEquals(new BigDecimal("100"), decision.capitalDecision().maxCapitalUSDT());
        assertEquals(false, decision.capitalDecision().canScale());
    }

    @Test
    void keepsSmallSlippageSampleAsMicroLiveWarningWhenNoHardBlockerExists() {
        ShadowLiveReadinessDecision decision = evaluator.evaluate(baseReady()
                .slippageSampleSize(3L)
                .meaningfulMovements(120L)
                .evidenceScore(bd("75"))
                .riskClass("B")
                .build());

        assertEquals(ShadowLiveReadinessStatus.MICRO_LIVE_READY, decision.status());
        assertEquals("MICRO_LIVE", decision.recommendedExecutionMode());
        assertTrue(decision.softWarnings().contains("NEEDS_MORE_SLIPPAGE_DATA"));
        assertTrue(decision.dataWarnings().contains("NEEDS_MORE_SLIPPAGE_DATA"));
        assertTrue(decision.hardBlockers().isEmpty());
        assertEquals(false, decision.capitalDecision().canScale());
    }

    @Test
    void allowsMicroLiveWithCompleteCyclesEvenWhenMovementCountIsMissing() {
        ShadowLiveReadinessDecision decision = evaluator.evaluate(baseReady()
                .closedPositions(0L)
                .completeCycles(3L)
                .meaningfulMovements(null)
                .evidenceScore(bd("10"))
                .riskClass("B")
                .build());

        assertEquals(ShadowLiveReadinessStatus.MICRO_LIVE_READY, decision.status());
        assertEquals("MICRO_LIVE", decision.recommendedExecutionMode());
        assertTrue(decision.softWarnings().contains("NEEDS_MORE_CLOSED_POSITIONS"));
    }

    @Test
    void allowsMicroLiveWithEvidenceScoreEvenWhenMovementCountIsLow() {
        ShadowLiveReadinessDecision decision = evaluator.evaluate(baseReady()
                .closedPositions(0L)
                .completeCycles(0L)
                .meaningfulMovements(1L)
                .evidenceScore(bd("75"))
                .riskClass("B")
                .build());

        assertEquals(ShadowLiveReadinessStatus.MICRO_LIVE_READY, decision.status());
        assertEquals("MICRO_LIVE", decision.recommendedExecutionMode());
        assertTrue(decision.softWarnings().contains("NEEDS_MORE_CLOSED_POSITIONS"));
    }

    @Test
    void needsMoreDataWhenHybridMicroEvidenceIsInsufficient() {
        ShadowLiveReadinessDecision decision = evaluator.evaluate(baseReady()
                .closedPositions(0L)
                .completeCycles(0L)
                .meaningfulMovements(1L)
                .evidenceScore(bd("10"))
                .riskClass("B")
                .build());

        assertEquals(ShadowLiveReadinessStatus.NEEDS_MORE_DATA, decision.status());
        assertEquals("SHADOW", decision.recommendedExecutionMode());
        assertTrue(decision.softWarnings().contains("NEEDS_MORE_CLOSED_POSITIONS"));
    }

    @Test
    void blocksWhenEndToEndLatencyIsTooHigh() {
        ShadowLiveReadinessDecision decision = evaluator.evaluate(baseReady()
                .shadowP95EndToEndMs(501L)
                .build());

        assertEquals(ShadowLiveReadinessStatus.BLOCKED, decision.status());
        assertTrue(decision.reasonCodes().contains("BLOCKED_LATENCY_TOO_HIGH"));
    }

    @Test
    void blocksWhenBinanceHttpLatencyIsTooHigh() {
        ShadowLiveReadinessDecision decision = evaluator.evaluate(baseReady()
                .binanceP95HttpMs(801L)
                .build());

        assertEquals(ShadowLiveReadinessStatus.BLOCKED, decision.status());
        assertTrue(decision.reasonCodes().contains("BLOCKED_BINANCE_LATENCY_TOO_HIGH"));
    }

    @Test
    void needsMoreDataWhenSlippageSampleIsTooSmall() {
        ShadowLiveReadinessDecision decision = evaluator.evaluate(baseReady()
                .slippageSampleSize(19L)
                .build());

        assertEquals(ShadowLiveReadinessStatus.NEEDS_MORE_DATA, decision.status());
        assertTrue(decision.reasonCodes().contains("NEEDS_MORE_SLIPPAGE_DATA"));
    }

    @Test
    void blocksWhenSlippageIsUnknown() {
        ShadowLiveReadinessDecision decision = evaluator.evaluate(baseReady()
                .slippageUnknown(true)
                .build());

        assertEquals(ShadowLiveReadinessStatus.BLOCKED, decision.status());
        assertTrue(decision.reasonCodes().contains("BLOCKED_SLIPPAGE_UNKNOWN"));
    }

    @Test
    void blocksWhenAdverseSlippageIsTooHigh() {
        ShadowLiveReadinessDecision decision = evaluator.evaluate(baseReady()
                .adverseSlippageBpsP95(bd("10.01"))
                .build());

        assertEquals(ShadowLiveReadinessStatus.BLOCKED, decision.status());
        assertTrue(decision.reasonCodes().contains("BLOCKED_SLIPPAGE_TOO_HIGH"));
    }

    @Test
    void blocksWhenPriceSourceIsUnreliable() {
        ShadowLiveReadinessDecision decision = evaluator.evaluate(baseReady()
                .priceSourceReliable(false)
                .build());

        assertEquals(ShadowLiveReadinessStatus.BLOCKED, decision.status());
        assertTrue(decision.reasonCodes().contains("PRICE_SOURCE_UNRELIABLE"));
    }

    @Test
    void blocksWhenSourceLeverageIsMissing() {
        ShadowLiveReadinessDecision decision = evaluator.evaluate(baseReady()
                .sourceLeverageX(null)
                .leverageStatus("MISSING_SOURCE_LEVERAGE")
                .build());

        assertEquals(ShadowLiveReadinessStatus.BLOCKED, decision.status());
        assertTrue(decision.reasonCodes().contains("BLOCKED_SOURCE_LEVERAGE_MISSING"));
    }

    @Test
    void blocksWhenLiveLeverageIsNotConfirmed() {
        ShadowLiveReadinessDecision decision = evaluator.evaluate(baseReady()
                .liveExchangeLeverageX(null)
                .leverageStatus("MISSING_LIVE_EXCHANGE_LEVERAGE")
                .build());

        assertEquals(ShadowLiveReadinessStatus.BLOCKED, decision.status());
        assertTrue(decision.reasonCodes().contains("BLOCKED_LIVE_LEVERAGE_NOT_CONFIRMED"));
    }

    @Test
    void blocksWhenLiveLeverageMismatchesRequestedLeverage() {
        ShadowLiveReadinessDecision decision = evaluator.evaluate(baseReady()
                .liveRequestedLeverageX(bd("5"))
                .liveExchangeLeverageX(bd("10"))
                .leverageMismatch(true)
                .build());

        assertEquals(ShadowLiveReadinessStatus.BLOCKED, decision.status());
        assertTrue(decision.reasonCodes().contains("BLOCKED_LEVERAGE_MISMATCH"));
        assertTrue(decision.reasonCodes().contains("BLOCKED_LEVERAGE_TOO_HIGH"));
    }

    @Test
    void blocksWhenEffectiveLeverageIsTooHigh() {
        ShadowLiveReadinessDecision decision = evaluator.evaluate(baseReady()
                .liveEffectiveLeverageX(bd("5.01"))
                .maxObservedEffectiveLeverageX(bd("5.01"))
                .build());

        assertEquals(ShadowLiveReadinessStatus.BLOCKED, decision.status());
        assertTrue(decision.reasonCodes().contains("BLOCKED_EFFECTIVE_LEVERAGE_TOO_HIGH"));
    }

    @Test
    void approvesWhenLeverageWasCappedButAppliedValuesRespectPolicy() {
        ShadowLiveReadinessDecision decision = evaluator.evaluate(baseReady()
                .sourceLeverageX(bd("5"))
                .liveRequestedLeverageX(bd("5"))
                .liveExchangeLeverageX(bd("5"))
                .liveEffectiveLeverageX(bd("5"))
                .leverageCapped(true)
                .leverageStatus("LEVERAGE_CAPPED")
                .build());

        assertEquals(ShadowLiveReadinessStatus.APPROVED_FOR_LIVE, decision.status());
    }

    private static ShadowLiveReadinessInput.ShadowLiveReadinessInputBuilder baseReady() {
        return ShadowLiveReadinessInput.builder()
                .shadowValidationPresent(true)
                .closedPositions(50L)
                .profitFactor(bd("1.30"))
                .expectancyUsdt(bd("0.01"))
                .netPnlUsdt(bd("1.00"))
                .top1Concentration(bd("0.35"))
                .stableHoursAfterDeploy(24L)
                .copyGuardAllowsLive(true)
                .accountingBugRecent(false)
                .unsupportedSymbols(false)
                .priceSourceReliable(true)
                .shadowP95EndToEndMs(500L)
                .liveMockP95EndToEndMs(1000L)
                .binanceP95HttpMs(800L)
                .adverseSlippageBpsP95(bd("10"))
                .maxAdverseSlippageUsdPerOrder(bd("1.00"))
                .slippageSampleSize(20L)
                .slippageUnknown(false)
                .sourceLeverageX(bd("5"))
                .liveRequestedLeverageX(bd("5"))
                .liveExchangeLeverageX(bd("5"))
                .liveEffectiveLeverageX(bd("5"))
                .maxObservedEffectiveLeverageX(bd("5"))
                .liveNotionalUsdPerOrder(bd("10"))
                .liveRequiredMarginUsdPerOrder(bd("10"))
                .leverageMismatch(false)
                .marginModeMismatch(false)
                .leverageCapped(false)
                .leverageStatus("OK");
    }

    private static BigDecimal bd(String value) {
        return new BigDecimal(value);
    }
}
