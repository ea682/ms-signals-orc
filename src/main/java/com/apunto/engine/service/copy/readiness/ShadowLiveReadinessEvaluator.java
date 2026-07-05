package com.apunto.engine.service.copy.readiness;

import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

@Component
public class ShadowLiveReadinessEvaluator {

    private static final BigDecimal ZERO = BigDecimal.ZERO;

    private final ShadowLiveReadinessProperties properties;

    public ShadowLiveReadinessEvaluator(ShadowLiveReadinessProperties properties) {
        this.properties = properties;
    }

    public ShadowLiveReadinessDecision evaluate(ShadowLiveReadinessInput input) {
        ShadowLiveReadinessInput safe = input == null ? ShadowLiveReadinessInput.builder().build() : input;
        List<Reason> reasons = new ArrayList<>();

        if (!flag(safe.shadowValidationPresent(), false)) {
            reasons.add(new Reason(ShadowLiveReadinessStatus.NOT_READY, "SHADOW_VALIDATION_MISSING"));
        }
        if (safe.closedPositions() == null || safe.closedPositions() < properties.getMinClosedPositions()) {
            reasons.add(new Reason(ShadowLiveReadinessStatus.NEEDS_MORE_DATA, "NEEDS_MORE_CLOSED_POSITIONS"));
        }
        if (flag(safe.accountingBugRecent(), false)) {
            reasons.add(new Reason(ShadowLiveReadinessStatus.BLOCKED, "ACCOUNTING_BUG_RECENT"));
        }
        if (flag(safe.unsupportedSymbols(), false)) {
            reasons.add(new Reason(ShadowLiveReadinessStatus.BLOCKED, "SYMBOL_NOT_SUPPORTED"));
        }
        if (!flag(safe.copyGuardAllowsLive(), true)) {
            reasons.add(new Reason(ShadowLiveReadinessStatus.BLOCKED, "COPY_GUARD_BLOCKED"));
        }
        if (!flag(safe.priceSourceReliable(), true)) {
            reasons.add(new Reason(ShadowLiveReadinessStatus.BLOCKED, "PRICE_SOURCE_UNRELIABLE"));
        }
        if (properties.isRequirePositiveNetPnl() && !positive(safe.netPnlUsdt())) {
            reasons.add(new Reason(ShadowLiveReadinessStatus.BLOCKED, "NET_PNL_NOT_POSITIVE"));
        }
        if (below(safe.profitFactor(), properties.getMinProfitFactor())) {
            reasons.add(new Reason(ShadowLiveReadinessStatus.BLOCKED, "PROFIT_FACTOR_TOO_LOW"));
        }
        if (properties.isRequirePositiveExpectancy() && !positive(safe.expectancyUsdt())) {
            reasons.add(new Reason(ShadowLiveReadinessStatus.BLOCKED, "EXPECTANCY_NOT_POSITIVE"));
        }
        if (above(safe.top1Concentration(), properties.getMaxTop1Concentration())) {
            reasons.add(new Reason(ShadowLiveReadinessStatus.BLOCKED, "TOP1_CONCENTRATION_TOO_HIGH"));
        }
        if (safe.stableHoursAfterDeploy() == null || safe.stableHoursAfterDeploy() < properties.getMinStableHoursAfterDeploy()) {
            reasons.add(new Reason(ShadowLiveReadinessStatus.NEEDS_MORE_DATA, "NEEDS_MORE_STABLE_HOURS"));
        }

        addLatencyReasons(safe, reasons);
        addSlippageReasons(safe, reasons);
        addLeverageReasons(safe, reasons);

        if (reasons.isEmpty()) {
            return new ShadowLiveReadinessDecision(
                    ShadowLiveReadinessStatus.APPROVED_FOR_LIVE,
                    List.of("SHADOW_READINESS_APPROVED"),
                    "LIVE",
                    normalizeRiskClass(safe.riskClass()),
                    safe.evidenceScore(),
                    List.of(),
                    List.of(),
                    new ShadowLiveReadinessDecision.CapitalDecision(
                            "ALLOCATE",
                            properties.getMicroLiveBaseCapitalUSDT(),
                            properties.getMicroLiveMaxCapitalUSDT(),
                            BigDecimal.ONE,
                            false,
                            List.of("SHADOW_READINESS_APPROVED")
                    )
            );
        }

        List<String> hardBlockers = reasons.stream()
                .filter(this::isHardBlocker)
                .map(Reason::code)
                .distinct()
                .toList();
        List<String> softWarnings = reasons.stream()
                .filter(reason -> !isHardBlocker(reason))
                .map(Reason::code)
                .distinct()
                .toList();
        List<String> reasonCodes = reasons.stream().map(Reason::code).distinct().toList();

        if (hardBlockers.isEmpty() && microLiveEligible(safe)) {
            return new ShadowLiveReadinessDecision(
                    ShadowLiveReadinessStatus.MICRO_LIVE_READY,
                    reasonCodes,
                    "MICRO_LIVE",
                    normalizeRiskClass(safe.riskClass()),
                    safe.evidenceScore(),
                    hardBlockers,
                    softWarnings,
                    new ShadowLiveReadinessDecision.CapitalDecision(
                            "MICRO_ALLOCATE",
                            properties.getMicroLiveBaseCapitalUSDT(),
                            properties.getMicroLiveMaxCapitalUSDT(),
                            BigDecimal.ONE,
                            false,
                            softWarnings
                    )
            );
        }

        ShadowLiveReadinessStatus status = highestSeverity(reasons);
        return new ShadowLiveReadinessDecision(
                status,
                reasonCodes,
                "SHADOW",
                normalizeRiskClass(safe.riskClass()),
                safe.evidenceScore(),
                hardBlockers,
                softWarnings,
                new ShadowLiveReadinessDecision.CapitalDecision(
                        "NO_ALLOCATE",
                        BigDecimal.ZERO,
                        BigDecimal.ZERO,
                        BigDecimal.ZERO,
                        false,
                        reasonCodes
                )
        );
    }

    private boolean isHardBlocker(Reason reason) {
        return reason.status() == ShadowLiveReadinessStatus.BLOCKED
                || reason.status() == ShadowLiveReadinessStatus.NOT_READY;
    }

    private boolean microLiveEligible(ShadowLiveReadinessInput input) {
        final String riskClass = normalizeRiskClass(input.riskClass());
        if ("D".equals(riskClass)) return false;
        boolean completeCyclesOk = completeCycles(input) >= properties.getMinMicroLiveCompleteCycles();
        boolean meaningfulMovementsOk = meaningfulMovements(input) >= properties.getMinMicroLiveMeaningfulMovements();
        boolean evidenceOk = input.evidenceScore() != null
                && input.evidenceScore().compareTo(microThreshold(riskClass)) >= 0;
        return completeCyclesOk || meaningfulMovementsOk || evidenceOk;
    }

    private long completeCycles(ShadowLiveReadinessInput input) {
        if (input.completeCycles() != null) return Math.max(0, input.completeCycles());
        return 0;
    }

    private long meaningfulMovements(ShadowLiveReadinessInput input) {
        if (input.meaningfulMovements() != null) return Math.max(0, input.meaningfulMovements());
        return 0;
    }

    private BigDecimal microThreshold(String riskClass) {
        return switch (riskClass) {
            case "A" -> properties.getEvidenceMicroThresholdA();
            case "C" -> properties.getEvidenceMicroThresholdC();
            case "D" -> properties.getEvidenceMicroThresholdD();
            default -> properties.getEvidenceMicroThresholdB();
        };
    }

    private String normalizeRiskClass(String value) {
        final String normalized = normalize(value);
        if ("A".equals(normalized) || "B".equals(normalized) || "C".equals(normalized) || "D".equals(normalized)) {
            return normalized;
        }
        return "B";
    }

    private void addLatencyReasons(ShadowLiveReadinessInput input, List<Reason> reasons) {
        if (input.shadowP95EndToEndMs() == null || input.liveMockP95EndToEndMs() == null || input.binanceP95HttpMs() == null) {
            reasons.add(new Reason(ShadowLiveReadinessStatus.NEEDS_MORE_DATA, "NEEDS_MORE_LATENCY_DATA"));
            return;
        }
        if (input.shadowP95EndToEndMs() > properties.getMaxShadowP95EndToEndMs()
                || input.liveMockP95EndToEndMs() > properties.getMaxLiveMockP95EndToEndMs()) {
            reasons.add(new Reason(ShadowLiveReadinessStatus.BLOCKED, "BLOCKED_LATENCY_TOO_HIGH"));
        }
        if (input.binanceP95HttpMs() > properties.getMaxBinanceP95HttpMs()) {
            reasons.add(new Reason(ShadowLiveReadinessStatus.BLOCKED, "BLOCKED_BINANCE_LATENCY_TOO_HIGH"));
        }
    }

    private void addSlippageReasons(ShadowLiveReadinessInput input, List<Reason> reasons) {
        if (flag(input.slippageUnknown(), false)) {
            reasons.add(new Reason(ShadowLiveReadinessStatus.BLOCKED, "BLOCKED_SLIPPAGE_UNKNOWN"));
            return;
        }
        if (input.slippageSampleSize() == null || input.slippageSampleSize() < properties.getRequireSlippageSampleSize()) {
            reasons.add(new Reason(ShadowLiveReadinessStatus.NEEDS_MORE_DATA, "NEEDS_MORE_SLIPPAGE_DATA"));
            return;
        }
        if (input.adverseSlippageBpsP95() == null || input.maxAdverseSlippageUsdPerOrder() == null) {
            reasons.add(new Reason(ShadowLiveReadinessStatus.BLOCKED, "BLOCKED_SLIPPAGE_UNKNOWN"));
            return;
        }
        if (above(input.adverseSlippageBpsP95(), properties.getMaxAdverseSlippageBpsP95())
                || above(input.maxAdverseSlippageUsdPerOrder(), properties.getMaxAdverseSlippageUsdPerOrder())) {
            reasons.add(new Reason(ShadowLiveReadinessStatus.BLOCKED, "BLOCKED_SLIPPAGE_TOO_HIGH"));
        }
    }

    private void addLeverageReasons(ShadowLiveReadinessInput input, List<Reason> reasons) {
        final String leverageStatus = normalize(input.leverageStatus());
        if ("MISSING_SOURCE_LEVERAGE".equals(leverageStatus)) {
            reasons.add(new Reason(
                    properties.isRequireSourceLeverage() ? ShadowLiveReadinessStatus.BLOCKED : ShadowLiveReadinessStatus.NEEDS_MORE_DATA,
                    properties.isRequireSourceLeverage() ? "BLOCKED_SOURCE_LEVERAGE_MISSING" : "NEEDS_MORE_LEVERAGE_DATA"
            ));
        }
        if ("MISSING_LIVE_EXCHANGE_LEVERAGE".equals(leverageStatus)) {
            reasons.add(new Reason(
                    properties.isRequireLiveLeverageConfirmed() ? ShadowLiveReadinessStatus.BLOCKED : ShadowLiveReadinessStatus.NEEDS_MORE_DATA,
                    properties.isRequireLiveLeverageConfirmed() ? "BLOCKED_LIVE_LEVERAGE_NOT_CONFIRMED" : "NEEDS_MORE_LEVERAGE_DATA"
            ));
        }
        if ("LEVERAGE_MISMATCH".equals(leverageStatus) && properties.isBlockOnLeverageMismatch()) {
            reasons.add(new Reason(ShadowLiveReadinessStatus.BLOCKED, "BLOCKED_LEVERAGE_MISMATCH"));
        }
        if ("MARGIN_MODE_MISMATCH".equals(leverageStatus) && properties.isBlockOnMarginModeMismatch()) {
            reasons.add(new Reason(ShadowLiveReadinessStatus.BLOCKED, "BLOCKED_MARGIN_MODE_MISMATCH"));
        }
        if ("INVALID_LEVERAGE".equals(leverageStatus)) {
            reasons.add(new Reason(ShadowLiveReadinessStatus.BLOCKED, "BLOCKED_SOURCE_LEVERAGE_MISSING"));
        }

        if (!positive(input.sourceLeverageX())) {
            reasons.add(new Reason(
                    properties.isRequireSourceLeverage() ? ShadowLiveReadinessStatus.BLOCKED : ShadowLiveReadinessStatus.NEEDS_MORE_DATA,
                    properties.isRequireSourceLeverage() ? "BLOCKED_SOURCE_LEVERAGE_MISSING" : "NEEDS_MORE_LEVERAGE_DATA"
            ));
        }
        if (properties.isRequireLiveLeverageConfirmed() && !positive(input.liveExchangeLeverageX())) {
            reasons.add(new Reason(ShadowLiveReadinessStatus.BLOCKED, "BLOCKED_LIVE_LEVERAGE_NOT_CONFIRMED"));
        }
        if (properties.isBlockOnLeverageMismatch()
                && (flag(input.leverageMismatch(), false) || numericMismatch(input.liveRequestedLeverageX(), input.liveExchangeLeverageX()))) {
            reasons.add(new Reason(ShadowLiveReadinessStatus.BLOCKED, "BLOCKED_LEVERAGE_MISMATCH"));
        }
        if (properties.isBlockOnMarginModeMismatch() && flag(input.marginModeMismatch(), false)) {
            reasons.add(new Reason(ShadowLiveReadinessStatus.BLOCKED, "BLOCKED_MARGIN_MODE_MISMATCH"));
        }
        if (above(input.sourceLeverageX(), properties.getMaxLeverageX())
                || above(input.liveRequestedLeverageX(), properties.getMaxLeverageX())
                || above(input.liveExchangeLeverageX(), properties.getMaxLeverageX())) {
            reasons.add(new Reason(ShadowLiveReadinessStatus.BLOCKED, "BLOCKED_LEVERAGE_TOO_HIGH"));
        }

        BigDecimal effective = input.maxObservedEffectiveLeverageX() == null
                ? input.liveEffectiveLeverageX()
                : input.maxObservedEffectiveLeverageX();
        if (above(effective, properties.getMaxEffectiveLeverageX())) {
            reasons.add(new Reason(ShadowLiveReadinessStatus.BLOCKED, "BLOCKED_EFFECTIVE_LEVERAGE_TOO_HIGH"));
        }
        if (above(input.liveNotionalUsdPerOrder(), properties.getMaxLiveNotionalUsdPerOrder())) {
            reasons.add(new Reason(ShadowLiveReadinessStatus.BLOCKED, "BLOCKED_NOTIONAL_TOO_HIGH"));
        }
        if (above(input.liveRequiredMarginUsdPerOrder(), properties.getMaxLiveRequiredMarginUsdPerOrder())) {
            reasons.add(new Reason(ShadowLiveReadinessStatus.BLOCKED, "BLOCKED_REQUIRED_MARGIN_TOO_HIGH"));
        }
        if (input.liveNotionalUsdPerOrder() == null || input.liveRequiredMarginUsdPerOrder() == null || effective == null) {
            reasons.add(new Reason(ShadowLiveReadinessStatus.NEEDS_MORE_DATA, "NEEDS_MORE_LEVERAGE_DATA"));
        }
    }

    private ShadowLiveReadinessStatus highestSeverity(List<Reason> reasons) {
        if (reasons.stream().anyMatch(reason -> reason.status() == ShadowLiveReadinessStatus.BLOCKED)) {
            return ShadowLiveReadinessStatus.BLOCKED;
        }
        if (reasons.stream().anyMatch(reason -> reason.status() == ShadowLiveReadinessStatus.NOT_READY)) {
            return ShadowLiveReadinessStatus.NOT_READY;
        }
        return ShadowLiveReadinessStatus.NEEDS_MORE_DATA;
    }

    private static boolean flag(Boolean value, boolean defaultValue) {
        return value == null ? defaultValue : value;
    }

    private static boolean positive(BigDecimal value) {
        return value != null && value.compareTo(ZERO) > 0;
    }

    private static boolean below(BigDecimal value, BigDecimal threshold) {
        return value == null || threshold != null && value.compareTo(threshold) < 0;
    }

    private static boolean above(BigDecimal value, BigDecimal threshold) {
        return value != null && threshold != null && value.compareTo(threshold) > 0;
    }

    private static boolean numericMismatch(BigDecimal requested, BigDecimal exchange) {
        return positive(requested) && positive(exchange) && requested.compareTo(exchange) != 0;
    }

    private static String normalize(String value) {
        return value == null ? "" : value.trim().toUpperCase(Locale.ROOT).replace('-', '_');
    }

    private record Reason(ShadowLiveReadinessStatus status, String code) {
    }
}
