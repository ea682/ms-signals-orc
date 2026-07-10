package com.apunto.engine.service.copy;

import com.apunto.engine.entity.UserCopyAllocationEntity;
import org.springframework.stereotype.Component;

import java.time.OffsetDateTime;
import java.util.Locale;

@Component
public class CopyRuntimeGuardPolicy {

    public Decision decide(UserCopyAllocationEntity allocation, CopyStrategyGuardDecision metricGuard) {
        if (allocation == null) {
            return Decision.block("ALLOCATION_MISSING", "allocation_missing");
        }
        String executionMode = UserCopyAllocationEntity.normalizeExecutionMode(allocation.getExecutionMode());
        if (!allocation.isActive() || allocation.getEndsAt() != null || allocation.getStatus() != UserCopyAllocationEntity.Status.ACTIVE) {
            return Decision.block("ALLOCATION_NOT_ACTIVE", "allocation_state");
        }
        if (allocation.getStatusCooldownUntil() != null && allocation.getStatusCooldownUntil().isAfter(OffsetDateTime.now())) {
            return Decision.block("ALLOCATION_COOLDOWN_ACTIVE", "allocation_state");
        }

        if (metricGuard == null) {
            return Decision.allow("RUNTIME_GUARD_NOT_AVAILABLE", "runtime_allocation");
        }
        if (isShadowOnlyGuard(metricGuard)) {
            if ("MICRO_LIVE".equals(executionMode) && isPromotedFromShadow(allocation)) {
                return Decision.allow("MICRO_LIVE_PROMOTED_ALLOCATION_SHOULD_NOT_BE_BLOCKED_BY_SHADOW_ONLY", "runtime_allocation");
            }
            if ("LIVE".equals(executionMode) && isPromotedFromRealFlow(allocation)) {
                return Decision.allow("LIVE_PROMOTED_ALLOCATION_SHOULD_NOT_BE_BLOCKED_BY_SHADOW_ONLY", "runtime_allocation");
            }
            return Decision.block(reasonCode(metricGuard, "SUMMARY_NOT_FINAL_LIVE_BLOCKED"), "metric_guard");
        }

        if (isRealRisk(metricGuard, executionMode)) {
            return Decision.block(reasonCode(metricGuard, "RUNTIME_GUARD_BLOCKED"), "metric_guard");
        }

        if (!metricGuard.allowed()) {
            return Decision.block(reasonCode(metricGuard, "RUNTIME_GUARD_BLOCKED"), "metric_guard");
        }
        return Decision.allow(reasonCode(metricGuard, "OK"), "metric_guard");
    }

    private static boolean isPromotedFromShadow(UserCopyAllocationEntity allocation) {
        return allocation != null
                && (allocation.getLinkedShadowAllocationId() != null || allocation.getPromotedFromShadowAt() != null);
    }

    private static boolean isPromotedFromRealFlow(UserCopyAllocationEntity allocation) {
        return isPromotedFromShadow(allocation);
    }

    private static boolean isShadowOnlyGuard(CopyStrategyGuardDecision guard) {
        String token = normalizedTokens(guard);
        return token.contains("SHADOW_ONLY")
                || token.contains("SUMMARY_NOT_FINAL_LIVE_BLOCKED")
                || (token.contains("METRIC_COPY_GUARD_PAUSE_OPEN") && token.contains("SHADOW_ONLY"));
    }

    private static boolean isRealRisk(CopyStrategyGuardDecision guard, String executionMode) {
        String token = normalizedTokens(guard);
        if (token.contains("DATA_RISK_CRITICAL")
                || token.contains("MANUAL_PAUSE")
                || token.contains("USER_DISABLED")
                || token.contains("CAPITAL_MISSING")
                || token.contains("SYMBOL_UNAVAILABLE")
                || token.contains("MICRO_LIVE_REQUIRED_REENTRY")
                || token.contains("MANUAL_REVIEW")
                || token.contains("DISABLED")
                || token.contains("BLOCKED")) {
            return true;
        }
        return "LIVE".equals(executionMode) && token.contains("SOURCE_EXPOSURE_DATA_MISSING");
    }

    private static String reasonCode(CopyStrategyGuardDecision guard, String fallback) {
        String reason = normalize(firstNonBlank(
                guard == null ? null : guard.reason(),
                guard == null ? null : guard.action(),
                guard == null ? null : guard.statusWhenBlocked(),
                fallback
        ));
        return reason == null ? fallback : reason;
    }

    private static String normalizedTokens(CopyStrategyGuardDecision guard) {
        return normalize(String.join("|",
                nullToEmpty(guard == null ? null : guard.reason()),
                nullToEmpty(guard == null ? null : guard.detail()),
                nullToEmpty(guard == null ? null : guard.statusWhenBlocked()),
                nullToEmpty(guard == null ? null : guard.action()),
                nullToEmpty(guard == null ? null : guard.targetExecutionMode())
        ));
    }

    private static String firstNonBlank(String... values) {
        if (values == null) return null;
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return null;
    }

    private static String nullToEmpty(String value) {
        return value == null ? "" : value;
    }

    private static String normalize(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        return value.trim().toUpperCase(Locale.ROOT).replace('-', '_');
    }

    public record Decision(boolean allowed, String reasonCode, String guardSource) {
        private static Decision allow(String reasonCode, String guardSource) {
            return new Decision(true, reasonCode, guardSource);
        }

        private static Decision block(String reasonCode, String guardSource) {
            return new Decision(false, reasonCode, guardSource);
        }
    }
}
