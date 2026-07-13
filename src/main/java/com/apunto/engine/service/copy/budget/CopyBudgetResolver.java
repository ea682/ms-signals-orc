package com.apunto.engine.service.copy.budget;

import com.apunto.engine.entity.UserCopyAllocationEntity;
import com.apunto.engine.shared.enums.FuturesCapitalAsset;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.UUID;

@Slf4j
@Component
public class CopyBudgetResolver {

    public static final String MICRO_LIVE_PROPORTIONAL_PORTFOLIO = "MICRO_LIVE_PROPORTIONAL_PORTFOLIO_V3";
    public static final String LIVE_PROPORTIONAL_PORTFOLIO = "LIVE_PROPORTIONAL_PORTFOLIO_V3";
    public static final String MICRO_LIVE_PROPORTIONAL_TARGET = "MICRO_LIVE_PROPORTIONAL_TARGET";
    public static final String LIVE_PROPORTIONAL_TARGET = "LIVE_PROPORTIONAL_TARGET";
    public static final String MICRO_LIVE_TOTAL_MARGIN_EXCEEDED = "MICRO_LIVE_TOTAL_MARGIN_EXCEEDED";
    public static final String MICRO_LIVE_INSUFFICIENT_AVAILABLE_BALANCE = "MICRO_LIVE_INSUFFICIENT_AVAILABLE_BALANCE";
    public static final String LIVE_ALLOCATED_CAPITAL_MISSING = "LIVE_ALLOCATED_CAPITAL_MISSING";
    public static final String BLOCKED_SOURCE_EQUITY_MISSING = "BLOCKED_SOURCE_EQUITY_MISSING";
    public static final String BLOCKED_SOURCE_EQUITY_INVALID = "BLOCKED_SOURCE_EQUITY_INVALID";
    public static final String BLOCKED_SOURCE_EQUITY_STALE = "BLOCKED_SOURCE_EQUITY_STALE";
    public static final String BLOCKED_SOURCE_SNAPSHOT_MISMATCH = "BLOCKED_SOURCE_SNAPSHOT_MISMATCH";
    public static final String BLOCKED_SOURCE_POSITION_NOTIONAL_MISSING = "BLOCKED_SOURCE_POSITION_NOTIONAL_MISSING";

    /** @deprecated Use the specific V3 equity/notional reasons. */
    @Deprecated(forRemoval = false)
    public static final String SOURCE_EXPOSURE_DATA_MISSING = BLOCKED_SOURCE_EQUITY_MISSING;

    private static final int SCALE = 12;
    private static final BigDecimal ZERO = BigDecimal.ZERO.setScale(SCALE, RoundingMode.DOWN);
    private static final BigDecimal MICRO_LIVE_TOTAL = new BigDecimal("100.000000000000");
    private static final BigDecimal MICRO_LIVE_LEVERAGE = new BigDecimal("5.000000000000");
    private static final BigDecimal DEFAULT_LEVERAGE = BigDecimal.ONE.setScale(SCALE, RoundingMode.DOWN);

    public CopyBudgetDecision resolve(CopyBudgetRequest request) {
        return resolveBudget(request);
    }

    public static CopyBudgetDecision resolveBudget(CopyBudgetRequest request) {
        CopyBudgetRequest safe = request == null ? CopyBudgetRequest.empty() : request;
        String executionMode = UserCopyAllocationEntity.normalizeExecutionMode(safe.executionMode());
        BigDecimal accountCapital = nonNegative(safe.accountCapitalUsd());
        BigDecimal allocationPct = percentage(safe.allocationPct());
        String capitalAsset = FuturesCapitalAsset.fromNullable(safe.capitalAsset()).name();

        CopyBudgetDecision decision = "MICRO_LIVE".equals(executionMode)
                ? resolveMicroLive(safe, accountCapital, allocationPct, capitalAsset)
                : resolveLive(safe, executionMode, accountCapital, allocationPct, capitalAsset);
        logResolved(safe, decision);
        return decision;
    }

    public static CopyBudgetDecision resolveBudget(String executionMode,
                                                   BigDecimal accountCapitalUsd,
                                                   BigDecimal allocationPct,
                                                   BigDecimal microLiveFixedBudgetUsd,
                                                   String capitalAsset) {
        return resolveBudget(CopyBudgetRequest.builder()
                .executionMode(executionMode)
                .accountCapitalUsd(accountCapitalUsd)
                .allocationPct(allocationPct)
                .microLiveFixedBudgetUsd(microLiveFixedBudgetUsd)
                .capitalAsset(capitalAsset)
                .build());
    }

    private static CopyBudgetDecision resolveMicroLive(CopyBudgetRequest request,
                                                       BigDecimal accountCapital,
                                                       BigDecimal allocationPct,
                                                       String capitalAsset) {
        BigDecimal openMargin = nonNegative(request.openMarginUsedUsd());
        BigDecimal remainingMargin = MICRO_LIVE_TOTAL.subtract(openMargin).max(BigDecimal.ZERO).setScale(SCALE, RoundingMode.DOWN);
        if (accountCapital.compareTo(MICRO_LIVE_TOTAL) < 0) {
            return decision(false, "MICRO_LIVE", MICRO_LIVE_PROPORTIONAL_PORTFOLIO,
                    MICRO_LIVE_TOTAL, accountCapital, allocationPct,
                    MICRO_LIVE_INSUFFICIENT_AVAILABLE_BALANCE, false, capitalAsset,
                    MICRO_LIVE_LEVERAGE, request, openMargin, remainingMargin, ZERO, ZERO, ZERO);
        }
        if (remainingMargin.compareTo(BigDecimal.ZERO) <= 0) {
            return decision(false, "MICRO_LIVE", MICRO_LIVE_PROPORTIONAL_PORTFOLIO,
                    MICRO_LIVE_TOTAL, accountCapital, allocationPct,
                    MICRO_LIVE_TOTAL_MARGIN_EXCEEDED, false, capitalAsset,
                    MICRO_LIVE_LEVERAGE, request, openMargin, remainingMargin, ZERO, ZERO, ZERO);
        }
        return proportionalDecision("MICRO_LIVE", MICRO_LIVE_PROPORTIONAL_PORTFOLIO,
                MICRO_LIVE_TOTAL, accountCapital, allocationPct, false, capitalAsset,
                MICRO_LIVE_LEVERAGE, request, openMargin, remainingMargin, MICRO_LIVE_PROPORTIONAL_TARGET);
    }

    private static CopyBudgetDecision resolveLive(CopyBudgetRequest request,
                                                  String executionMode,
                                                  BigDecimal accountCapital,
                                                  BigDecimal allocationPct,
                                                  String capitalAsset) {
        BigDecimal allocatedCapital = accountCapital.multiply(allocationPct).setScale(SCALE, RoundingMode.DOWN);
        BigDecimal leverage = positiveOrDefault(request.leverage(), DEFAULT_LEVERAGE);
        if (allocatedCapital.compareTo(BigDecimal.ZERO) <= 0) {
            return decision(false, executionMode, LIVE_PROPORTIONAL_PORTFOLIO,
                    allocatedCapital, accountCapital, allocationPct,
                    LIVE_ALLOCATED_CAPITAL_MISSING, true, capitalAsset,
                    leverage, request, ZERO, allocatedCapital, ZERO, ZERO, ZERO);
        }
        return proportionalDecision(executionMode, LIVE_PROPORTIONAL_PORTFOLIO,
                allocatedCapital, accountCapital, allocationPct, true, capitalAsset,
                leverage, request, ZERO, allocatedCapital, LIVE_PROPORTIONAL_TARGET);
    }

    private static CopyBudgetDecision proportionalDecision(String executionMode,
                                                           String budgetMode,
                                                           BigDecimal targetCapital,
                                                           BigDecimal accountCapital,
                                                           BigDecimal allocationPct,
                                                           boolean usesAllocationPct,
                                                           String capitalAsset,
                                                           BigDecimal leverage,
                                                           CopyBudgetRequest request,
                                                           BigDecimal openMargin,
                                                           BigDecimal remainingMargin,
                                                           String successReason) {
        BigDecimal equity = nullable(request.sourceAccountEquityUsd());
        if (equity == null) {
            return decision(false, executionMode, budgetMode, targetCapital, accountCapital, allocationPct,
                    BLOCKED_SOURCE_EQUITY_MISSING, usesAllocationPct, capitalAsset, leverage, request,
                    openMargin, remainingMargin, ZERO, ZERO, ZERO);
        }
        if (equity.compareTo(BigDecimal.ZERO) <= 0) {
            return decision(false, executionMode, budgetMode, targetCapital, accountCapital, allocationPct,
                    BLOCKED_SOURCE_EQUITY_INVALID, usesAllocationPct, capitalAsset, leverage, request,
                    openMargin, remainingMargin, ZERO, ZERO, ZERO);
        }
        BigDecimal sourceNotional = nullable(request.sourcePositionNotionalUsd());
        if (sourceNotional == null || sourceNotional.compareTo(BigDecimal.ZERO) <= 0) {
            return decision(false, executionMode, budgetMode, targetCapital, accountCapital, allocationPct,
                    BLOCKED_SOURCE_POSITION_NOTIONAL_MISSING, usesAllocationPct, capitalAsset, leverage, request,
                    openMargin, remainingMargin, ZERO, ZERO, ZERO);
        }

        BigDecimal exposure = sourceNotional.abs().divide(equity, SCALE, RoundingMode.DOWN);
        BigDecimal targetNotional = targetCapital.multiply(exposure).setScale(SCALE, RoundingMode.DOWN);
        BigDecimal targetMargin = targetNotional.divide(leverage, SCALE, RoundingMode.DOWN);
        return decision(targetNotional.compareTo(BigDecimal.ZERO) > 0, executionMode, budgetMode,
                targetCapital, accountCapital, allocationPct, successReason, usesAllocationPct,
                capitalAsset, leverage, request, openMargin, remainingMargin,
                targetMargin, targetNotional, exposure);
    }

    private static CopyBudgetDecision decision(boolean allowed,
                                               String executionMode,
                                               String budgetMode,
                                               BigDecimal budgetUsd,
                                               BigDecimal accountCapital,
                                               BigDecimal allocationPct,
                                               String reasonCode,
                                               boolean usesAllocationPct,
                                               String capitalAsset,
                                               BigDecimal leverage,
                                               CopyBudgetRequest request,
                                               BigDecimal openMargin,
                                               BigDecimal remainingMargin,
                                               BigDecimal copyMargin,
                                               BigDecimal copyNotional,
                                               BigDecimal sourceExposure) {
        return new CopyBudgetDecision(
                allowed,
                executionMode,
                budgetMode,
                scale(budgetUsd),
                scale(accountCapital),
                scale(allocationPct),
                reasonCode,
                usesAllocationPct,
                capitalAsset,
                scale(copyMargin),
                scale(copyNotional),
                scale(sourceExposure),
                scale(request.sourceAccountEquityUsd()),
                scale(request.sourcePositionMarginUsd()),
                scale(request.sourcePositionNotionalUsd()),
                scale(budgetUsd),
                ZERO,
                scale(openMargin),
                scale(remainingMargin),
                null,
                request.openPositionsCount(),
                scale(leverage)
        );
    }

    private static void logResolved(CopyBudgetRequest request, CopyBudgetDecision decision) {
        log.info(
                "event=copy.budget.resolved policyVersion=proportional-portfolio-v3 userId={} detailUserId={} walletId={} allocationId={} executionMode={} strategyCode={} budgetMode={} allocatedCapitalUsd={} copyMarginUsd={} copyNotionalUsd={} sourceAccountEquityUsd={} sourcePositionNotionalUsd={} sourceExposureRatio={} leverage={} openMarginUsd={} remainingMarginUsd={} reasonCode={} decision={}",
                request.userId(), request.detailUserId(), safe(request.walletId()), request.userCopyAllocationId(),
                decision.executionMode(), safe(request.copyStrategyCode()), decision.budgetMode(),
                decision.budgetUsd(), decision.copyMarginUsd(), decision.copyNotionalUsd(),
                decision.sourceAccountEquityUsd(), decision.sourcePositionNotionalUsd(),
                decision.sourceExposurePct(), decision.leverage(), decision.openMarginUsedUsd(),
                decision.remainingMarginUsd(), decision.reasonCode(), decision.allowed() ? "ALLOW" : "BLOCK"
        );
    }

    private static BigDecimal nullable(BigDecimal value) {
        return value == null ? null : value.setScale(SCALE, RoundingMode.DOWN);
    }

    private static BigDecimal nonNegative(BigDecimal value) {
        if (value == null || value.compareTo(BigDecimal.ZERO) <= 0) {
            return ZERO;
        }
        return value.setScale(SCALE, RoundingMode.DOWN);
    }

    private static BigDecimal percentage(BigDecimal value) {
        if (value == null || value.compareTo(BigDecimal.ZERO) <= 0) {
            return ZERO;
        }
        return value.min(BigDecimal.ONE).setScale(SCALE, RoundingMode.DOWN);
    }

    private static BigDecimal positiveOrDefault(BigDecimal value, BigDecimal fallback) {
        return value == null || value.compareTo(BigDecimal.ZERO) <= 0
                ? fallback
                : value.setScale(SCALE, RoundingMode.DOWN);
    }

    private static BigDecimal scale(BigDecimal value) {
        return value == null ? ZERO : value.setScale(SCALE, RoundingMode.DOWN);
    }

    private static String safe(String value) {
        return value == null ? "" : value;
    }

    public record CopyBudgetDecision(
            boolean allowed,
            String executionMode,
            String budgetMode,
            BigDecimal budgetUsd,
            BigDecimal accountCapitalUsd,
            BigDecimal allocationPct,
            String reasonCode,
            boolean usesAllocationPct,
            String capitalAsset,
            BigDecimal copyMarginUsd,
            BigDecimal copyNotionalUsd,
            BigDecimal sourceExposurePct,
            BigDecimal sourceAccountEquityUsd,
            BigDecimal sourcePositionMarginUsd,
            BigDecimal sourcePositionNotionalUsd,
            BigDecimal totalCapitalUsd,
            BigDecimal maxMarginPerOperationUsd,
            BigDecimal openMarginUsedUsd,
            BigDecimal remainingMarginUsd,
            Integer maxConcurrentPositions,
            Integer openPositionsCount,
            BigDecimal leverage
    ) {}

    public record CopyBudgetRequest(
            UUID userId,
            UUID detailUserId,
            String walletId,
            Long userCopyAllocationId,
            String executionMode,
            String copyStrategyCode,
            BigDecimal accountCapitalUsd,
            BigDecimal allocationPct,
            BigDecimal microLiveFixedBudgetUsd,
            String capitalAsset,
            BigDecimal maxMarginPerOperationUsd,
            BigDecimal openMarginUsedUsd,
            Integer maxConcurrentPositions,
            Integer openPositionsCount,
            BigDecimal leverage,
            BigDecimal sourceAccountEquityUsd,
            BigDecimal sourcePositionMarginUsd,
            BigDecimal sourcePositionNotionalUsd,
            BigDecimal sourceExposurePct,
            Boolean requireSourceExposure,
            BigDecimal maxSourceExposurePct
    ) {
        private static CopyBudgetRequest empty() {
            return builder().build();
        }

        public static Builder builder() {
            return new Builder();
        }

        public static final class Builder {
            private UUID userId;
            private UUID detailUserId;
            private String walletId;
            private Long userCopyAllocationId;
            private String executionMode;
            private String copyStrategyCode;
            private BigDecimal accountCapitalUsd;
            private BigDecimal allocationPct;
            private BigDecimal microLiveFixedBudgetUsd;
            private String capitalAsset;
            private BigDecimal maxMarginPerOperationUsd;
            private BigDecimal openMarginUsedUsd;
            private Integer maxConcurrentPositions;
            private Integer openPositionsCount;
            private BigDecimal leverage;
            private BigDecimal sourceAccountEquityUsd;
            private BigDecimal sourcePositionMarginUsd;
            private BigDecimal sourcePositionNotionalUsd;
            private BigDecimal sourceExposurePct;
            private Boolean requireSourceExposure;
            private BigDecimal maxSourceExposurePct;

            private Builder() {}

            public Builder userId(UUID value) { this.userId = value; return this; }
            public Builder detailUserId(UUID value) { this.detailUserId = value; return this; }
            public Builder walletId(String value) { this.walletId = value; return this; }
            public Builder userCopyAllocationId(Long value) { this.userCopyAllocationId = value; return this; }
            public Builder executionMode(String value) { this.executionMode = value; return this; }
            public Builder copyStrategyCode(String value) { this.copyStrategyCode = value; return this; }
            public Builder accountCapitalUsd(BigDecimal value) { this.accountCapitalUsd = value; return this; }
            public Builder allocationPct(BigDecimal value) { this.allocationPct = value; return this; }
            public Builder microLiveFixedBudgetUsd(BigDecimal value) { this.microLiveFixedBudgetUsd = value; return this; }
            public Builder capitalAsset(String value) { this.capitalAsset = value; return this; }
            public Builder maxMarginPerOperationUsd(BigDecimal value) { this.maxMarginPerOperationUsd = value; return this; }
            public Builder openMarginUsedUsd(BigDecimal value) { this.openMarginUsedUsd = value; return this; }
            public Builder maxConcurrentPositions(Integer value) { this.maxConcurrentPositions = value; return this; }
            public Builder openPositionsCount(Integer value) { this.openPositionsCount = value; return this; }
            public Builder leverage(BigDecimal value) { this.leverage = value; return this; }
            public Builder sourceAccountEquityUsd(BigDecimal value) { this.sourceAccountEquityUsd = value; return this; }
            public Builder sourcePositionMarginUsd(BigDecimal value) { this.sourcePositionMarginUsd = value; return this; }
            public Builder sourcePositionNotionalUsd(BigDecimal value) { this.sourcePositionNotionalUsd = value; return this; }
            public Builder sourceExposurePct(BigDecimal value) { this.sourceExposurePct = value; return this; }
            public Builder requireSourceExposure(Boolean value) { this.requireSourceExposure = value; return this; }
            public Builder maxSourceExposurePct(BigDecimal value) { this.maxSourceExposurePct = value; return this; }

            public CopyBudgetRequest build() {
                return new CopyBudgetRequest(
                        userId, detailUserId, walletId, userCopyAllocationId, executionMode, copyStrategyCode,
                        accountCapitalUsd, allocationPct, microLiveFixedBudgetUsd, capitalAsset,
                        maxMarginPerOperationUsd, openMarginUsedUsd, maxConcurrentPositions, openPositionsCount,
                        leverage, sourceAccountEquityUsd, sourcePositionMarginUsd, sourcePositionNotionalUsd,
                        sourceExposurePct, requireSourceExposure, maxSourceExposurePct
                );
            }
        }
    }
}
