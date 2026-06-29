package com.apunto.engine.service.copy.readiness;

import lombok.Builder;

import java.math.BigDecimal;

@Builder
public record ShadowLiveReadinessInput(
        Boolean shadowValidationPresent,
        Long closedPositions,
        BigDecimal profitFactor,
        BigDecimal expectancyUsdt,
        BigDecimal netPnlUsdt,
        BigDecimal top1Concentration,
        Long stableHoursAfterDeploy,
        Boolean copyGuardAllowsLive,
        Boolean accountingBugRecent,
        Boolean unsupportedSymbols,
        Boolean priceSourceReliable,
        Long shadowP95EndToEndMs,
        Long liveMockP95EndToEndMs,
        Long binanceP95HttpMs,
        BigDecimal adverseSlippageBpsP95,
        BigDecimal maxAdverseSlippageUsdPerOrder,
        Long slippageSampleSize,
        Boolean slippageUnknown,
        BigDecimal sourceLeverageX,
        BigDecimal liveRequestedLeverageX,
        BigDecimal liveExchangeLeverageX,
        BigDecimal liveEffectiveLeverageX,
        BigDecimal maxObservedEffectiveLeverageX,
        BigDecimal liveNotionalUsdPerOrder,
        BigDecimal liveRequiredMarginUsdPerOrder,
        Boolean leverageMismatch,
        Boolean marginModeMismatch,
        Boolean leverageCapped,
        String leverageStatus
) {
}
