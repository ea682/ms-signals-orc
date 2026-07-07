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
    public static final String INSUFFICIENT_BALANCE_FOR_MICRO_LIVE = "INSUFFICIENT_BALANCE_FOR_MICRO_LIVE";
    public static final String LIVE_WEIGHTED_ALLOCATION_PCT = "LIVE_WEIGHTED_ALLOCATION_PCT";
    private static final BigDecimal ZERO = BigDecimal.ZERO.setScale(12, RoundingMode.HALF_UP);
    private static final BigDecimal ONE = BigDecimal.ONE;
    private static final int SCALE = 12;

    public CopyBudgetDecision resolve(CopyBudgetRequest request) {
        return resolveBudget(request);
    }

    public static CopyBudgetDecision resolveBudget(CopyBudgetRequest request) {
        CopyBudgetRequest safeRequest = request == null ? CopyBudgetRequest.empty() : request;
        String executionMode = UserCopyAllocationEntity.normalizeExecutionMode(safeRequest.executionMode());
        BigDecimal accountCapital = positive(safeRequest.accountCapitalUsd());
        BigDecimal allocationPct = pct(safeRequest.allocationPct());
        BigDecimal microBudget = positive(safeRequest.microLiveFixedBudgetUsd());
        String capitalAsset = FuturesCapitalAsset.fromNullable(safeRequest.capitalAsset()).name();

        CopyBudgetDecision decision;
        if ("MICRO_LIVE".equals(executionMode)) {
            decision = resolveMicroLive(safeRequest, executionMode, accountCapital, allocationPct, microBudget, capitalAsset);
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
                                                       BigDecimal microBudget,
                                                       String capitalAsset) {
        if (accountCapital.compareTo(BigDecimal.ZERO) <= 0) {
            return new CopyBudgetDecision(
                    false,
                    executionMode,
                    FIXED_USD,
                    ZERO,
                    accountCapital,
                    allocationPct,
                    INSUFFICIENT_BALANCE_FOR_MICRO_LIVE,
                    false,
                    capitalAsset
            );
        }
        BigDecimal fixedBudget = microBudget.compareTo(BigDecimal.ZERO) > 0 ? microBudget : new BigDecimal("100.000000000000");
        BigDecimal budget = accountCapital.min(fixedBudget).setScale(SCALE, RoundingMode.HALF_UP);
        return new CopyBudgetDecision(
                true,
                executionMode,
                FIXED_USD,
                budget,
                accountCapital,
                allocationPct,
                MICRO_LIVE_FIXED_BUDGET_USD,
                false,
                capitalAsset
        );
    }

    private static CopyBudgetDecision resolveLive(CopyBudgetRequest request,
                                                  String executionMode,
                                                  BigDecimal accountCapital,
                                                  BigDecimal allocationPct,
                                                  String capitalAsset) {
        BigDecimal budget = accountCapital.multiply(allocationPct).setScale(SCALE, RoundingMode.HALF_UP);
        return new CopyBudgetDecision(
                budget.compareTo(BigDecimal.ZERO) > 0,
                executionMode,
                WEIGHTED_PERCENTAGE,
                budget,
                accountCapital,
                allocationPct,
                LIVE_WEIGHTED_ALLOCATION_PCT,
                true,
                capitalAsset
        );
    }

    private static void logResolved(CopyBudgetRequest request, CopyBudgetDecision decision) {
        log.info(
                "event=copy.budget.resolved userId={} detailUserId={} walletId={} userCopyAllocationId={} executionMode={} copyStrategyCode={} budgetMode={} budgetUsd={} accountCapitalUsd={} allocationPct={} reasonCode={} decision={}",
                request.userId(),
                request.detailUserId(),
                safe(request.walletId()),
                request.userCopyAllocationId(),
                decision.executionMode(),
                safe(request.copyStrategyCode()),
                decision.budgetMode(),
                decision.budgetUsd().toPlainString(),
                decision.accountCapitalUsd().toPlainString(),
                decision.allocationPct().toPlainString(),
                decision.reasonCode(),
                decision.allowed() ? "ALLOW" : "REJECT"
        );
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
            String capitalAsset
    ) {
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
            String capitalAsset
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
                        capitalAsset
                );
            }
        }
    }
}
