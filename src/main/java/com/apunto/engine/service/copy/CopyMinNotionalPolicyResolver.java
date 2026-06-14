package com.apunto.engine.service.copy;

import com.apunto.engine.dto.UserDetailDto;
import com.apunto.engine.dto.client.MetricaWalletDto;
import com.apunto.engine.entity.DetailUserEntity;
import com.apunto.engine.entity.UserCopyAllocationEntity;
import com.apunto.engine.service.UserCopyAllocationService;
import com.apunto.engine.shared.enums.CopyMinNotionalMode;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;

@Component
@RequiredArgsConstructor
public class CopyMinNotionalPolicyResolver {

    private final UserCopyAllocationService userCopyAllocationService;
    private final CopyStrategyRuntimeRouter copyStrategyRuntimeRouter;

    public CopyMinNotionalPolicy resolve(UserDetailDto userDetail,
                                         String walletId,
                                         MetricaWalletDto walletMetric) {
        if (userDetail == null || userDetail.getUser() == null || userDetail.getDetail() == null) {
            return CopyMinNotionalPolicy.skip();
        }

        final UUID userId = userDetail.getUser().getId();
        if (userId == null) {
            return CopyMinNotionalPolicy.skip();
        }

        final DetailUserEntity detail = userDetail.getDetail();
        final String strategyCode = copyStrategyRuntimeRouter.strategyCodeOf(walletMetric);
        final Optional<UserCopyAllocationEntity> allocationOpt = userCopyAllocationService.findActiveAllocation(userId, walletId, strategyCode)
                .or(() -> userCopyAllocationService.findActiveAllocation(userId, walletId));
        final UserCopyAllocationEntity allocation = allocationOpt.orElse(null);

        final CopyMinNotionalMode detailMode = defaultMode(detail.getCopyMinNotionalMode(), CopyMinNotionalMode.SKIP);
        final CopyMinNotionalMode allocationMode = allocation == null
                ? CopyMinNotionalMode.INHERIT
                : defaultMode(allocation.getCopyMinNotionalMode(), CopyMinNotionalMode.INHERIT);
        final CopyMinNotionalMode effectiveMode = allocationMode.inherits() ? detailMode : allocationMode;

        final Integer score = resolveScore(walletMetric, allocation);
        final BigDecimal historyDays = resolveHistoryDays(walletMetric);
        final Integer operationsCount = resolveOperationsCount(walletMetric);

        return new CopyMinNotionalPolicy(
                effectiveMode,
                allocation == null ? null : allocation.getId(),
                score,
                firstPositive(allocation == null ? null : allocation.getCopyMinNotionalMinScore(), detail.getCopyMinNotionalMinScore()),
                historyDays,
                firstPositive(allocation == null ? null : allocation.getCopyMinNotionalMinHistoryDays(), detail.getCopyMinNotionalMinHistoryDays()),
                operationsCount,
                firstPositive(allocation == null ? null : allocation.getCopyMinNotionalMinOperations(), detail.getCopyMinNotionalMinOperations()),
                firstPositive(allocation == null ? null : allocation.getCopyMinNotionalMaxUsdt(), detail.getCopyMinNotionalMaxUsdt())
        );
    }

    private static CopyMinNotionalMode defaultMode(CopyMinNotionalMode value, CopyMinNotionalMode fallback) {
        return value == null ? fallback : value;
    }

    private static Integer resolveScore(MetricaWalletDto walletMetric, UserCopyAllocationEntity allocation) {
        if (walletMetric != null && walletMetric.getScoring() != null) {
            if (walletMetric.getScoring().getDecisionMetricConservative() != null) {
                return walletMetric.getScoring().getDecisionMetricConservative();
            }
            if (walletMetric.getScoring().getDecisionMetric() != null) {
                return walletMetric.getScoring().getDecisionMetric();
            }
        }
        return allocation == null ? null : allocation.getScore();
    }

    private static BigDecimal resolveHistoryDays(MetricaWalletDto walletMetric) {
        if (walletMetric == null || walletMetric.getWallet() == null) {
            return null;
        }
        final Double historyDays = walletMetric.getWallet().getHistoryDays();
        if (historyDays == null || !Double.isFinite(historyDays) || historyDays < 0.0) {
            return null;
        }
        return BigDecimal.valueOf(historyDays);
    }

    private static Integer resolveOperationsCount(MetricaWalletDto walletMetric) {
        if (walletMetric == null || walletMetric.getWallet() == null) {
            return null;
        }
        return walletMetric.getWallet().getCountOperation();
    }

    private static Integer firstPositive(Integer first, Integer second) {
        if (first != null && first > 0) {
            return first;
        }
        if (second != null && second > 0) {
            return second;
        }
        return null;
    }

    private static BigDecimal firstPositive(BigDecimal first, BigDecimal second) {
        if (first != null && first.compareTo(BigDecimal.ZERO) > 0) {
            return first;
        }
        if (second != null && second.compareTo(BigDecimal.ZERO) > 0) {
            return second;
        }
        return null;
    }
}
