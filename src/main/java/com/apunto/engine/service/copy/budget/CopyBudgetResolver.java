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

    public static final String FIXED_USD = "FIXED_USD";
    public static final String WEIGHTED_PERCENTAGE = "WEIGHTED_PERCENTAGE";
    public static final String MICRO_LIVE_FIXED_BUDGET_USD = "MICRO_LIVE_FIXED_BUDGET_USD";
    public static final String MICRO_LIVE_FIXED_PER_OPERATION = "MICRO_LIVE_FIXED_PER_OPERATION";
    public static final String MICRO_LIVE_FIXED_PER_OPERATION_USD = "MICRO_LIVE_FIXED_PER_OPERATION_USD";
    public static final String MICRO_LIVE_TOTAL_MARGIN_EXCEEDED = "MICRO_LIVE_TOTAL_MARGIN_EXCEEDED";
    public static final String MICRO_LIVE_MAX_CONCURRENT_POSITIONS_EXCEEDED = "MICRO_LIVE_MAX_CONCURRENT_POSITIONS_EXCEEDED";
    public static final String MICRO_LIVE_INSUFFICIENT_AVAILABLE_BALANCE = "MICRO_LIVE_INSUFFICIENT_AVAILABLE_BALANCE";
    public static final String LIVE_WEIGHTED_ALLOCATION_PCT = "LIVE_WEIGHTED_ALLOCATION_PCT";
    public static final String LIVE_SOURCE_EXPOSURE_PERCENT = "LIVE_SOURCE_EXPOSURE_PERCENT";
    public static final String LIVE_SOURCE_EXPOSURE_PERCENT_OF_ALLOCATED_CAPITAL = "LIVE_SOURCE_EXPOSURE_PERCENT_OF_ALLOCATED_CAPITAL";
    public static final String SOURCE_EXPOSURE_DATA_MISSING = "SOURCE_EXPOSURE_DATA_MISSING";
    public static final String LIVE_ALLOCATED_CAPITAL_MISSING = "LIVE_ALLOCATED_CAPITAL_MISSING";

    private static final BigDecimal ZERO = BigDecimal.ZERO.setScale(12, RoundingMode.HALF_UP);
    private static final BigDecimal ONE = BigDecimal.ONE;
    private static final BigDecimal DEFAULT_MICRO_TOTAL = new BigDecimal("100.000000000000");
    private static final BigDecimal DEFAULT_MICRO_PER_OPERATION = new BigDecimal("20.000000000000");
    private static final BigDecimal DEFAULT_LEVERAGE = BigDecimal.ONE.setScale(12, RoundingMode.HALF_UP);
    private static final int DEFAULT_MAX_CONCURRENT = 5;
    private static final int SCALE = 12;

    public CopyBudgetDecision resolve(CopyBudgetRequest request) {
        return resolveBudget(request);
    }

    public static CopyBudgetDecision resolveBudget(CopyBudgetRequest request) {
        CopyBudgetRequest safeRequest = request == null ? CopyBudgetRequest.empty() : request;
        String executionMode = UserCopyAllocationEntity.normalizeExecutionMode(safeRequest.executionMode());
        BigDecimal accountCapital = positive(safeRequest.accountCapitalUsd());
        BigDecimal allocationPct = pct(safeRequest.allocationPct());
        BigDecimal configuredTargetCapital = positive(safeRequest.microLiveFixedBudgetUsd());
        BigDecimal targetCapital = configuredTargetCapital.compareTo(BigDecimal.ZERO) > 0
                ? configuredTargetCapital
                : DEFAULT_MICRO_TOTAL;
        String capitalAsset = FuturesCapitalAsset.fromNullable(safeRequest.capitalAsset()).name();

        CopyBudgetDecision decision;
        if ("MICRO_LIVE".equals(executionMode)) {
            decision = resolveMicroLive(safeRequest, executionMode, accountCapital, allocationPct, targetCapital, capitalAsset);
        } else {
            decision = resolveLive(safeRequest, executionMode, accountCapital, allocationPct, capitalAsset);
        }
        logResolved(safeRequest, decision);
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
                                                       String executionMode,
                                                       BigDecimal accountCapital,
                                                       BigDecimal allocationPct,
                                                       BigDecimal targetCapital,
                                                       String capitalAsset) {
        if (accountCapital.compareTo(targetCapital) < 0) {
            return decision(false, executionMode, MICRO_LIVE_FIXED_PER_OPERATION, targetCapital, accountCapital, allocationPct,
                    MICRO_LIVE_INSUFFICIENT_AVAILABLE_BALANCE, false, capitalAsset)
                    .withMicroFields(ZERO, ZERO, targetCapital, perOperation(request), openMargin(request), ZERO,
                            maxConcurrent(request), openPositions(request), leverage(request));
        }

        BigDecimal totalCapital = targetCapital.setScale(SCALE, RoundingMode.HALF_UP);
        BigDecimal perOperation = perOperation(request);
        BigDecimal openMarginUsed = openMargin(request);
        BigDecimal remaining = totalCapital.subtract(openMarginUsed).max(BigDecimal.ZERO).setScale(SCALE, RoundingMode.HALF_UP);
        int maxConcurrent = maxConcurrent(request);
        int openPositions = openPositions(request);
        BigDecimal leverage = leverage(request);

        if (remaining.compareTo(BigDecimal.ZERO) <= 0) {
            return decision(false, executionMode, MICRO_LIVE_FIXED_PER_OPERATION, totalCapital, accountCapital, allocationPct,
                    MICRO_LIVE_TOTAL_MARGIN_EXCEEDED, false, capitalAsset)
                    .withMicroFields(ZERO, ZERO, totalCapital, perOperation, openMarginUsed, remaining,
                            maxConcurrent, openPositions, leverage);
        }
        if (openPositions >= maxConcurrent) {
            return decision(false, executionMode, MICRO_LIVE_FIXED_PER_OPERATION, totalCapital, accountCapital, allocationPct,
                    MICRO_LIVE_MAX_CONCURRENT_POSITIONS_EXCEEDED, false, capitalAsset)
                    .withMicroFields(ZERO, ZERO, totalCapital, perOperation, openMarginUsed, remaining,
                            maxConcurrent, openPositions, leverage);
        }

        BigDecimal copyMargin = remaining.min(perOperation).setScale(SCALE, RoundingMode.HALF_UP);
        BigDecimal copyNotional = copyMargin.multiply(leverage).setScale(SCALE, RoundingMode.HALF_UP);
        return decision(true, executionMode, MICRO_LIVE_FIXED_PER_OPERATION, totalCapital, accountCapital, allocationPct,
                MICRO_LIVE_FIXED_PER_OPERATION_USD, false, capitalAsset)
                .withMicroFields(copyMargin, copyNotional, totalCapital, perOperation, openMarginUsed, remaining,
                        maxConcurrent, openPositions, leverage);
    }

    private static CopyBudgetDecision resolveLive(CopyBudgetRequest request,
                                                  String executionMode,
                                                  BigDecimal accountCapital,
                                                  BigDecimal allocationPct,
                                                  String capitalAsset) {
        BigDecimal allocatedCapital = accountCapital.multiply(allocationPct).setScale(SCALE, RoundingMode.HALF_UP);
        if (allocatedCapital.compareTo(BigDecimal.ZERO) <= 0) {
            return decision(false, executionMode, LIVE_SOURCE_EXPOSURE_PERCENT, allocatedCapital, accountCapital, allocationPct,
                    LIVE_ALLOCATED_CAPITAL_MISSING, true, capitalAsset);
        }

        BigDecimal sourceExposure = sourceExposurePct(request);
        if (sourceExposure.compareTo(BigDecimal.ZERO) <= 0 && Boolean.TRUE.equals(request.requireSourceExposure())) {
            return decision(false, executionMode, LIVE_SOURCE_EXPOSURE_PERCENT, allocatedCapital, accountCapital, allocationPct,
                    SOURCE_EXPOSURE_DATA_MISSING, true, capitalAsset)
                    .withLiveFields(ZERO, ZERO, sourceExposure, positive(request.sourceAccountEquityUsd()),
                            positive(request.sourcePositionMarginUsd()), leverage(request));
        }
        if (sourceExposure.compareTo(BigDecimal.ZERO) <= 0) {
            sourceExposure = allocationPct;
        }

        BigDecimal maxSourceExposure = positive(request.maxSourceExposurePct());
        if (maxSourceExposure.compareTo(BigDecimal.ZERO) > 0 && sourceExposure.compareTo(maxSourceExposure) > 0) {
            sourceExposure = maxSourceExposure;
        }

        BigDecimal copyMargin = allocatedCapital.multiply(sourceExposure).setScale(SCALE, RoundingMode.HALF_UP);
        BigDecimal copyNotional = copyMargin.multiply(leverage(request)).setScale(SCALE, RoundingMode.HALF_UP);
        return decision(copyMargin.compareTo(BigDecimal.ZERO) > 0, executionMode, LIVE_SOURCE_EXPOSURE_PERCENT,
                allocatedCapital, accountCapital, allocationPct,
                copyMargin.compareTo(BigDecimal.ZERO) > 0
                        ? LIVE_SOURCE_EXPOSURE_PERCENT_OF_ALLOCATED_CAPITAL
                        : SOURCE_EXPOSURE_DATA_MISSING,
                true, capitalAsset)
                .withLiveFields(copyMargin, copyNotional, sourceExposure, positive(request.sourceAccountEquityUsd()),
                        positive(request.sourcePositionMarginUsd()), leverage(request));
    }

    private static BigDecimal sourceExposurePct(CopyBudgetRequest request) {
        BigDecimal explicit = pct(request.sourceExposurePct());
        if (explicit.compareTo(BigDecimal.ZERO) > 0) {
            return explicit;
        }
        BigDecimal equity = positive(request.sourceAccountEquityUsd());
        BigDecimal margin = positive(request.sourcePositionMarginUsd());
        if (equity.compareTo(BigDecimal.ZERO) <= 0 || margin.compareTo(BigDecimal.ZERO) <= 0) {
            return ZERO;
        }
        return margin.divide(equity, SCALE, RoundingMode.HALF_UP).min(ONE).setScale(SCALE, RoundingMode.HALF_UP);
    }

    private static CopyBudgetDecision decision(boolean allowed,
                                               String executionMode,
                                               String budgetMode,
                                               BigDecimal budgetUsd,
                                               BigDecimal accountCapital,
                                               BigDecimal allocationPct,
                                               String reasonCode,
                                               boolean usesAllocationPct,
                                               String capitalAsset) {
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
                ZERO,
                ZERO,
                ZERO,
                ZERO,
                ZERO,
                ZERO,
                ZERO,
                ZERO,
                ZERO,
                null,
                null,
                DEFAULT_LEVERAGE
        );
    }

    private static void logResolved(CopyBudgetRequest request, CopyBudgetDecision decision) {
        log.info(
                "event=copy.budget.resolved userId={} detailUserId={} walletId={} userCopyAllocationId={} executionMode={} copyStrategyCode={} budgetMode={} allocatedCapitalUsd={} totalCapitalUsd={} maxMarginPerOperationUsd={} openMarginUsedUsd={} remainingMarginUsd={} copyMarginUsd={} copyNotionalUsd={} sourceAccountEquityUsd={} sourcePositionMarginUsd={} sourceExposurePct={} leverage={} reasonCode={} decision={} elapsedMs=0",
                request.userId(),
                request.detailUserId(),
                safe(request.walletId()),
                request.userCopyAllocationId(),
                decision.executionMode(),
                safe(request.copyStrategyCode()),
                decision.budgetMode(),
                decision.budgetUsd().toPlainString(),
                decision.totalCapitalUsd().toPlainString(),
                decision.maxMarginPerOperationUsd().toPlainString(),
                decision.openMarginUsedUsd().toPlainString(),
                decision.remainingMarginUsd().toPlainString(),
                decision.copyMarginUsd().toPlainString(),
                decision.copyNotionalUsd().toPlainString(),
                decision.sourceAccountEquityUsd().toPlainString(),
                decision.sourcePositionMarginUsd().toPlainString(),
                decision.sourceExposurePct().toPlainString(),
                decision.leverage().toPlainString(),
                decision.reasonCode(),
                decision.allowed() ? "ALLOW" : "REJECT"
        );
    }

    private static BigDecimal perOperation(CopyBudgetRequest request) {
        BigDecimal value = positive(request.maxMarginPerOperationUsd());
        return value.compareTo(BigDecimal.ZERO) > 0 ? value : DEFAULT_MICRO_PER_OPERATION;
    }

    private static BigDecimal openMargin(CopyBudgetRequest request) {
        return positive(request.openMarginUsedUsd());
    }

    private static int maxConcurrent(CopyBudgetRequest request) {
        Integer value = request.maxConcurrentPositions();
        return value == null || value <= 0 ? DEFAULT_MAX_CONCURRENT : value;
    }

    private static int openPositions(CopyBudgetRequest request) {
        Integer value = request.openPositionsCount();
        return value == null || value < 0 ? 0 : value;
    }

    private static BigDecimal leverage(CopyBudgetRequest request) {
        BigDecimal value = positive(request.leverage());
        return value.compareTo(BigDecimal.ZERO) > 0 ? value : DEFAULT_LEVERAGE;
    }

    private static BigDecimal positive(BigDecimal value) {
        if (value == null || value.compareTo(BigDecimal.ZERO) <= 0) {
            return ZERO;
        }
        return value.setScale(SCALE, RoundingMode.HALF_UP);
    }

    private static BigDecimal pct(BigDecimal value) {
        if (value == null || value.compareTo(BigDecimal.ZERO) <= 0) {
            return ZERO;
        }
        BigDecimal normalized = value.min(ONE);
        return normalized.setScale(SCALE, RoundingMode.HALF_UP);
    }

    private static BigDecimal scale(BigDecimal value) {
        return value == null ? ZERO : value.setScale(SCALE, RoundingMode.HALF_UP);
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
            BigDecimal totalCapitalUsd,
            BigDecimal maxMarginPerOperationUsd,
            BigDecimal openMarginUsedUsd,
            BigDecimal remainingMarginUsd,
            Integer maxConcurrentPositions,
            Integer openPositionsCount,
            BigDecimal leverage
    ) {
        CopyBudgetDecision withMicroFields(BigDecimal copyMarginUsd,
                                           BigDecimal copyNotionalUsd,
                                           BigDecimal totalCapitalUsd,
                                           BigDecimal maxMarginPerOperationUsd,
                                           BigDecimal openMarginUsedUsd,
                                           BigDecimal remainingMarginUsd,
                                           Integer maxConcurrentPositions,
                                           Integer openPositionsCount,
                                           BigDecimal leverage) {
            return new CopyBudgetDecision(
                    allowed, executionMode, budgetMode, budgetUsd, accountCapitalUsd, allocationPct, reasonCode,
                    usesAllocationPct, capitalAsset, scale(copyMarginUsd), scale(copyNotionalUsd), sourceExposurePct,
                    sourceAccountEquityUsd, sourcePositionMarginUsd, scale(totalCapitalUsd), scale(maxMarginPerOperationUsd),
                    scale(openMarginUsedUsd), scale(remainingMarginUsd), maxConcurrentPositions, openPositionsCount, scale(leverage)
            );
        }

        CopyBudgetDecision withLiveFields(BigDecimal copyMarginUsd,
                                          BigDecimal copyNotionalUsd,
                                          BigDecimal sourceExposurePct,
                                          BigDecimal sourceAccountEquityUsd,
                                          BigDecimal sourcePositionMarginUsd,
                                          BigDecimal leverage) {
            return new CopyBudgetDecision(
                    allowed, executionMode, budgetMode, budgetUsd, accountCapitalUsd, allocationPct, reasonCode,
                    usesAllocationPct, capitalAsset, scale(copyMarginUsd), scale(copyNotionalUsd), scale(sourceExposurePct),
                    scale(sourceAccountEquityUsd), scale(sourcePositionMarginUsd), totalCapitalUsd, maxMarginPerOperationUsd,
                    openMarginUsedUsd, remainingMarginUsd, maxConcurrentPositions, openPositionsCount, scale(leverage)
            );
        }
    }

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
            private BigDecimal sourceExposurePct;
            private Boolean requireSourceExposure;
            private BigDecimal maxSourceExposurePct;

            private Builder() {
            }

            public Builder userId(UUID userId) {
                this.userId = userId;
                return this;
            }

            public Builder detailUserId(UUID detailUserId) {
                this.detailUserId = detailUserId;
                return this;
            }

            public Builder walletId(String walletId) {
                this.walletId = walletId;
                return this;
            }

            public Builder userCopyAllocationId(Long userCopyAllocationId) {
                this.userCopyAllocationId = userCopyAllocationId;
                return this;
            }

            public Builder executionMode(String executionMode) {
                this.executionMode = executionMode;
                return this;
            }

            public Builder copyStrategyCode(String copyStrategyCode) {
                this.copyStrategyCode = copyStrategyCode;
                return this;
            }

            public Builder accountCapitalUsd(BigDecimal accountCapitalUsd) {
                this.accountCapitalUsd = accountCapitalUsd;
                return this;
            }

            public Builder allocationPct(BigDecimal allocationPct) {
                this.allocationPct = allocationPct;
                return this;
            }

            public Builder microLiveFixedBudgetUsd(BigDecimal microLiveFixedBudgetUsd) {
                this.microLiveFixedBudgetUsd = microLiveFixedBudgetUsd;
                return this;
            }

            public Builder capitalAsset(String capitalAsset) {
                this.capitalAsset = capitalAsset;
                return this;
            }

            public Builder maxMarginPerOperationUsd(BigDecimal maxMarginPerOperationUsd) {
                this.maxMarginPerOperationUsd = maxMarginPerOperationUsd;
                return this;
            }

            public Builder openMarginUsedUsd(BigDecimal openMarginUsedUsd) {
                this.openMarginUsedUsd = openMarginUsedUsd;
                return this;
            }

            public Builder maxConcurrentPositions(Integer maxConcurrentPositions) {
                this.maxConcurrentPositions = maxConcurrentPositions;
                return this;
            }

            public Builder openPositionsCount(Integer openPositionsCount) {
                this.openPositionsCount = openPositionsCount;
                return this;
            }

            public Builder leverage(BigDecimal leverage) {
                this.leverage = leverage;
                return this;
            }

            public Builder sourceAccountEquityUsd(BigDecimal sourceAccountEquityUsd) {
                this.sourceAccountEquityUsd = sourceAccountEquityUsd;
                return this;
            }

            public Builder sourcePositionMarginUsd(BigDecimal sourcePositionMarginUsd) {
                this.sourcePositionMarginUsd = sourcePositionMarginUsd;
                return this;
            }

            public Builder sourceExposurePct(BigDecimal sourceExposurePct) {
                this.sourceExposurePct = sourceExposurePct;
                return this;
            }

            public Builder requireSourceExposure(Boolean requireSourceExposure) {
                this.requireSourceExposure = requireSourceExposure;
                return this;
            }

            public Builder maxSourceExposurePct(BigDecimal maxSourceExposurePct) {
                this.maxSourceExposurePct = maxSourceExposurePct;
                return this;
            }

            public CopyBudgetRequest build() {
                return new CopyBudgetRequest(
                        userId,
                        detailUserId,
                        walletId,
                        userCopyAllocationId,
                        executionMode,
                        copyStrategyCode,
                        accountCapitalUsd,
                        allocationPct,
                        microLiveFixedBudgetUsd,
                        capitalAsset,
                        maxMarginPerOperationUsd,
                        openMarginUsedUsd,
                        maxConcurrentPositions,
                        openPositionsCount,
                        leverage,
                        sourceAccountEquityUsd,
                        sourcePositionMarginUsd,
                        sourceExposurePct,
                        requireSourceExposure,
                        maxSourceExposurePct
                );
            }
        }
    }
}
