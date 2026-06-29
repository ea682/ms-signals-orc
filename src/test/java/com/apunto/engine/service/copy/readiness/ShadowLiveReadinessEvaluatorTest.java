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
    }

    @Test
    void blocksWhenCopyGuardDoesNotAllowLive() {
        ShadowLiveReadinessDecision decision = evaluator.evaluate(baseReady()
                .copyGuardAllowsLive(false)
                .build());

        assertEquals(ShadowLiveReadinessStatus.BLOCKED, decision.status());
        assertTrue(decision.reasonCodes().contains("COPY_GUARD_BLOCKED"));
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
