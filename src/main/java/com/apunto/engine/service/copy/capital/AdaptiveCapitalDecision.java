package com.apunto.engine.service.copy.capital;

import java.math.BigDecimal;
import java.util.List;

public record AdaptiveCapitalDecision(
        boolean allowAction,
        boolean closeCurrentSide,
        boolean openNewSide,
        boolean allowsMoney,
        BigDecimal capitalMultiplier,
        StrategyOperationalState operationalState,
        List<String> reasonCodes
) {
}

