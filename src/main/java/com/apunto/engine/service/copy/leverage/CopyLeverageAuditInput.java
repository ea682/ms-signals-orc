package com.apunto.engine.service.copy.leverage;

import lombok.Builder;

import java.math.BigDecimal;

@Builder
public record CopyLeverageAuditInput(
        BigDecimal sourceLeverageX,
        BigDecimal sourceEffectiveLeverageX,
        BigDecimal shadowRequestedLeverageX,
        BigDecimal shadowAppliedLeverageX,
        BigDecimal liveRequestedLeverageX,
        BigDecimal liveExchangeLeverageX,
        BigDecimal liveEffectiveLeverageX,
        BigDecimal sourceNotionalUsd,
        BigDecimal sourceMarginUsd,
        BigDecimal shadowNotionalUsd,
        BigDecimal shadowRequiredMarginUsd,
        BigDecimal liveNotionalUsd,
        BigDecimal liveRequiredMarginUsd,
        String sourceMarginMode,
        String liveMarginMode,
        BigDecimal leverageCapX,
        String leverageSource,
        boolean requireLiveExchangeLeverage,
        boolean requireMarginModeMatch,
        boolean notionalMismatch
) {
}
