package com.apunto.engine.service.copy.readiness;

import com.apunto.engine.service.copy.promotion.LivePromotionProperties;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;

@Component
public class MicroLiveExecutionEvidencePolicy {

    private static final BigDecimal ZERO = BigDecimal.ZERO;
    private static final BigDecimal HUNDRED = new BigDecimal("100");

    private final LivePromotionProperties properties;
    private final MeterRegistry meterRegistry;

    public MicroLiveExecutionEvidencePolicy(LivePromotionProperties properties, MeterRegistry meterRegistry) {
        this.properties = properties;
        this.meterRegistry = meterRegistry;
    }

    public MicroLiveReadinessDecision evaluate(MicroLiveExecutionEvidence raw) {
        MicroLiveExecutionEvidence evidence = raw == null ? MicroLiveExecutionEvidence.builder().build() : raw;
        List<String> reasons = new ArrayList<>();

        long minDays = nonNegative(properties.getMinMicroDays());
        long minSubmitted = nonNegative(properties.getMinSubmittedOrders());
        long minAcknowledged = nonNegative(properties.getMinAcknowledgedOrders());
        long minFilled = nonNegative(properties.getMinFilledOrders());
        long minClosed = nonNegative(properties.getMinClosedOperations());

        if (evidence.observedDays() < minDays) reasons.add("MICRO_LIVE_NOT_READY_MIN_DAYS");
        if (evidence.submittedOrders() <= 0 && minSubmitted > 0) {
            reasons.add("MICRO_LIVE_NOT_READY_ZERO_SUBMITTED_ORDERS");
        } else if (evidence.submittedOrders() < minSubmitted) {
            reasons.add("MICRO_LIVE_NOT_READY_MIN_SUBMITTED_ORDERS");
        }
        if (evidence.acknowledgedOrders() < minAcknowledged) reasons.add("MICRO_LIVE_NOT_READY_MIN_ACKNOWLEDGED_ORDERS");
        if (evidence.filledOrders() < minFilled) reasons.add("MICRO_LIVE_NOT_READY_MIN_FILLED_ORDERS");
        if (evidence.closedOperations() < minClosed) reasons.add("MICRO_LIVE_NOT_READY_MIN_CLOSED_OPERATIONS");
        if (evidence.dispatchErrors() > nonNegative(properties.getMaxDispatchErrors())) reasons.add("MICRO_LIVE_NOT_READY_DISPATCH_ERRORS");
        BigDecimal errorRatePct = percentage(evidence.dispatchErrors(), evidence.submittedOrders());
        BigDecimal maxErrorRatePct = decimal(properties.getMaxErrorRatePct());
        if (maxErrorRatePct.compareTo(ZERO) >= 0 && errorRatePct.compareTo(maxErrorRatePct) > 0) {
            reasons.add("MICRO_LIVE_NOT_READY_ERROR_RATE");
        }
        if (evidence.reconciliationPending() > nonNegative(properties.getMaxReconciliationPending())) reasons.add("MICRO_LIVE_NOT_READY_RECONCILIATION_PENDING");
        if (evidence.duplicateCount() > nonNegative(properties.getMaxDuplicateCount())) reasons.add("MICRO_LIVE_NOT_READY_DUPLICATE_ORDERS");
        if (evidence.unresolvedAmbiguousTimeouts() > nonNegative(properties.getMaxUnresolvedAmbiguousTimeouts())) {
            reasons.add("MICRO_LIVE_NOT_READY_AMBIGUOUS_RECONCILIATION");
        }
        if (evidence.slippageSamples() < nonNegative(properties.getMinSlippageSamples())) reasons.add("MICRO_LIVE_NOT_READY_SLIPPAGE_SAMPLE");
        if (positive(properties.getMaxAdverseSlippageP95Bps())
                && decimal(evidence.adverseSlippageP95Bps()).compareTo(properties.getMaxAdverseSlippageP95Bps()) > 0) {
            reasons.add("MICRO_LIVE_NOT_READY_ADVERSE_SLIPPAGE");
        }
        if (positive(properties.getMaxDrawdownUsd())
                && decimal(evidence.maxDrawdownUsd()).compareTo(properties.getMaxDrawdownUsd()) > 0) {
            reasons.add("MICRO_LIVE_NOT_READY_DRAWDOWN");
        }
        if (properties.isRequirePositiveNetPnl()
                && decimal(evidence.realizedPnlUsd()).compareTo(decimal(properties.getMinNetPnlUsd())) < 0) {
            reasons.add("MICRO_LIVE_NOT_READY_NEGATIVE_PNL");
        }

        BigDecimal calendar = ratio(evidence.observedDays(), minDays);
        BigDecimal execution = average(
                ratio(evidence.submittedOrders(), minSubmitted),
                ratio(evidence.acknowledgedOrders(), minAcknowledged),
                ratio(evidence.filledOrders(), minFilled),
                ratio(evidence.closedOperations(), minClosed)
        );
        BigDecimal reconciliation = average(
                inverseRatio(evidence.dispatchErrors(), properties.getMaxDispatchErrors()),
                inverseRatio(evidence.reconciliationPending(), properties.getMaxReconciliationPending()),
                inverseRatio(evidence.duplicateCount(), properties.getMaxDuplicateCount()),
                inverseRatio(evidence.unresolvedAmbiguousTimeouts(), properties.getMaxUnresolvedAmbiguousTimeouts())
        );
        BigDecimal slippage = ratio(evidence.slippageSamples(), nonNegative(properties.getMinSlippageSamples()));
        BigDecimal finalPct = min(calendar, execution, reconciliation, slippage);
        if (!reasons.isEmpty() && finalPct.compareTo(HUNDRED) >= 0) finalPct = ZERO.setScale(2);

        for (String reason : reasons) {
            meterRegistry.counter("copy.readiness.block.total", "reason", metricTag(reason)).increment();
        }
        return new MicroLiveReadinessDecision(
                reasons.isEmpty(), reasons, scale(calendar), scale(execution), scale(reconciliation), scale(finalPct)
        );
    }

    private static BigDecimal ratio(long actual, long required) {
        if (required <= 0) return HUNDRED;
        if (actual <= 0) return ZERO;
        return BigDecimal.valueOf(actual).multiply(HUNDRED)
                .divide(BigDecimal.valueOf(required), 8, RoundingMode.HALF_UP)
                .min(HUNDRED);
    }

    private static BigDecimal inverseRatio(long actual, long maximum) {
        if (actual <= maximum) return HUNDRED;
        if (maximum <= 0) return ZERO;
        return BigDecimal.valueOf(maximum).multiply(HUNDRED)
                .divide(BigDecimal.valueOf(actual), 8, RoundingMode.HALF_UP)
                .min(HUNDRED);
    }

    private static BigDecimal percentage(long numerator, long denominator) {
        if (numerator <= 0) return ZERO;
        if (denominator <= 0) return HUNDRED;
        return BigDecimal.valueOf(numerator).multiply(HUNDRED)
                .divide(BigDecimal.valueOf(denominator), 8, RoundingMode.HALF_UP);
    }

    private static BigDecimal average(BigDecimal... values) {
        if (values == null || values.length == 0) return ZERO;
        BigDecimal sum = ZERO;
        for (BigDecimal value : values) sum = sum.add(decimal(value));
        return sum.divide(BigDecimal.valueOf(values.length), 8, RoundingMode.HALF_UP);
    }

    private static BigDecimal min(BigDecimal... values) {
        BigDecimal result = HUNDRED;
        if (values == null) return ZERO;
        for (BigDecimal value : values) result = result.min(decimal(value));
        return result;
    }

    private static BigDecimal scale(BigDecimal value) {
        return decimal(value).setScale(2, RoundingMode.HALF_UP);
    }

    private static BigDecimal decimal(BigDecimal value) {
        return value == null ? ZERO : value;
    }

    private static boolean positive(BigDecimal value) {
        return value != null && value.compareTo(ZERO) > 0;
    }

    private static long nonNegative(long value) {
        return Math.max(0L, value);
    }

    private static String metricTag(String value) {
        if (value == null || value.isBlank()) return "unknown";
        String normalized = value.trim().toLowerCase(java.util.Locale.ROOT).replace('-', '_');
        return normalized.length() > 64 ? normalized.substring(0, 64) : normalized;
    }
}
