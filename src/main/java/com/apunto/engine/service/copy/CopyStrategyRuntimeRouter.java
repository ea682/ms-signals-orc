package com.apunto.engine.service.copy;

import com.apunto.engine.dto.client.MetricaWalletDto;
import com.apunto.engine.entity.UserCopyAllocationEntity;
import com.apunto.engine.hyperliquid.model.HyperliquidDeltaType;
import com.apunto.engine.shared.enums.PositionSide;
import com.apunto.engine.jobs.model.CopyJobAction;
import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

@Component
public class CopyStrategyRuntimeRouter {

    public static final String DEFAULT_STRATEGY_CODE = "MOVEMENT_ALL";
    public static final String LONG_ONLY = "LONG_ONLY";
    public static final String SHORT_ONLY = "SHORT_ONLY";
    public static final String MOVEMENT_ALL = "MOVEMENT_ALL";
    public static final String PURE_OPEN_CLOSE = "PURE_OPEN_CLOSE";
    public static final String FIRST_OPEN_FINAL_CLOSE = "FIRST_OPEN_FINAL_CLOSE";
    public static final String FLIP_ONLY = "FLIP_ONLY";
    public static final String RECENT_7D = "RECENT_7D";
    public static final String RECENT_14D = "RECENT_14D";
    public static final String RECENT_30D = "RECENT_30D";

    private static final Set<String> DIRECT_COPY_STRATEGIES = Set.of(
            MOVEMENT_ALL,
            LONG_ONLY,
            SHORT_ONLY,
            PURE_OPEN_CLOSE,
            FIRST_OPEN_FINAL_CLOSE,
            FLIP_ONLY,
            RECENT_7D,
            RECENT_14D,
            RECENT_30D
    );

    private static final Set<String> ALL_FLOW_STRATEGIES = Set.of(
            MOVEMENT_ALL,
            FIRST_OPEN_FINAL_CLOSE,
            RECENT_7D,
            RECENT_14D,
            RECENT_30D
    );

    public String strategyCodeOf(MetricaWalletDto metric) {
        if (metric == null) {
            return DEFAULT_STRATEGY_CODE;
        }

        if (metric.getStrategy() != null) {
            String fromCode = normalizeStrategyCode(metric.getStrategy().getStrategyCode());
            if (fromCode != null) return fromCode;

            String fromSlug = fromSlug(metric.getStrategy().getStrategySlug());
            if (fromSlug != null) return fromSlug;
        }

        String platform = metric.getWallet() == null ? null : metric.getWallet().getPlatform();
        String fromPlatform = fromPlatform(platform);
        return fromPlatform == null ? DEFAULT_STRATEGY_CODE : fromPlatform;
    }

    public String strategyCodeOf(UserCopyAllocationEntity allocation) {
        if (allocation == null) {
            return DEFAULT_STRATEGY_CODE;
        }
        String code = normalizeStrategyCode(allocation.getCopyStrategyCode());
        return code == null ? DEFAULT_STRATEGY_CODE : code;
    }

    public String allocationKey(String walletId, String strategyCode) {
        String wallet = normalizeWalletId(walletId);
        String strategy = normalizeStrategyCode(strategyCode);
        if (wallet == null) return null;
        return wallet + "|" + (strategy == null ? DEFAULT_STRATEGY_CODE : strategy);
    }

    public boolean isCopyableJoyasCandidate(MetricaWalletDto metric) {
        String code = strategyCodeOf(metric);
        if (!DIRECT_COPY_STRATEGIES.contains(code)) {
            return false;
        }

        MetricaWalletDto.StrategyDto strategy = metric == null ? null : metric.getStrategy();
        MetricaWalletDto.CopyabilityDto copyability = strategy == null ? null : strategy.getCopyability();
        if (copyability == null) {
            return true;
        }

        String type = normalize(copyability.getType());
        Boolean canOpenPosition = copyability.getCanOpenPosition();
        Boolean supportedByJoyas = copyability.getSupportedByJoyas();

        return !"DIAGNOSTIC_ONLY".equals(type)
                && !Boolean.FALSE.equals(canOpenPosition)
                && !Boolean.FALSE.equals(supportedByJoyas);
    }

    public boolean allocationAppliesToEvent(
            UserCopyAllocationEntity allocation,
            CopyJobAction action,
            HyperliquidDeltaType deltaType,
            String side
    ) {
        return strategyAppliesToEvent(strategyCodeOf(allocation), action, deltaType, side);
    }

    public boolean metricAppliesToEvent(
            MetricaWalletDto metric,
            CopyJobAction action,
            HyperliquidDeltaType deltaType,
            String side
    ) {
        return strategyAppliesToEvent(strategyCodeOf(metric), action, deltaType, side);
    }

    public boolean strategyCodeAppliesToEvent(
            String strategyCode,
            CopyJobAction action,
            HyperliquidDeltaType deltaType,
            String side
    ) {
        return strategyAppliesToEvent(strategyCode, action, deltaType, side);
    }

    public boolean metricAllowsTargetLeg(
            MetricaWalletDto metric,
            PositionSide side,
            HyperliquidDeltaType triggerDeltaType,
            String legOriginId,
            String triggerOriginId
    ) {
        String strategy = strategyCodeOf(metric);
        String sideCode = side == null ? null : normalize(side.name());

        if (LONG_ONLY.equals(strategy)) {
            return "LONG".equals(sideCode);
        }
        if (SHORT_ONLY.equals(strategy)) {
            return "SHORT".equals(sideCode);
        }
        if (PURE_OPEN_CLOSE.equals(strategy)) {
            return triggerDeltaType == HyperliquidDeltaType.OPEN && sameOrigin(legOriginId, triggerOriginId);
        }
        if (FLIP_ONLY.equals(strategy)) {
            return triggerDeltaType == HyperliquidDeltaType.FLIP && sameOrigin(legOriginId, triggerOriginId);
        }
        return true;
    }

    public Comparator<MetricaWalletDto> metricPreferenceComparator(
            CopyJobAction action,
            HyperliquidDeltaType deltaType,
            String side
    ) {
        return Comparator
                .comparingDouble((MetricaWalletDto metric) -> metricAppliesToEvent(metric, action, deltaType, side) ? 1.0 : 0.0)
                .thenComparingDouble(this::strategySpecificity)
                .thenComparingDouble(this::strategyScore)
                .thenComparingDouble(this::decisionScore)
                .thenComparingDouble(MetricaWalletDto::getCapitalShare)
                .reversed();
    }

    public String strategySummary(MetricaWalletDto metric) {
        if (metric == null) {
            return DEFAULT_STRATEGY_CODE;
        }
        MetricaWalletDto.StrategyDto strategy = metric.getStrategy();
        String label = strategy == null ? null : strategy.getStrategyLabel();
        String code = strategyCodeOf(metric);
        return label == null || label.isBlank() ? code : code + ":" + label;
    }

    public static String normalizeWalletId(String raw) {
        if (raw == null) return null;
        String value = raw.trim().toLowerCase(Locale.ROOT);
        return value.isEmpty() ? null : value;
    }

    public static String normalizeStrategyCode(String raw) {
        String normalized = normalize(raw);
        if (normalized == null) return null;
        return normalized.replace('-', '_');
    }

    private boolean strategyAppliesToEvent(
            String strategy,
            CopyJobAction action,
            HyperliquidDeltaType deltaType,
            String side
    ) {
        String strategyCode = normalizeStrategyCode(strategy);
        if (strategyCode == null) strategyCode = DEFAULT_STRATEGY_CODE;

        if (action == CopyJobAction.CLOSE) {
            return true;
        }

        HyperliquidDeltaType effectiveDelta = deltaType == null ? HyperliquidDeltaType.UNKNOWN : deltaType;
        if (effectiveDelta == HyperliquidDeltaType.UPDATE
                || effectiveDelta == HyperliquidDeltaType.NO_CHANGE
                || effectiveDelta == HyperliquidDeltaType.UNKNOWN) {
            return false;
        }

        String sideCode = normalize(side);
        if (LONG_ONLY.equals(strategyCode)) {
            return "LONG".equals(sideCode);
        }
        if (SHORT_ONLY.equals(strategyCode)) {
            return "SHORT".equals(sideCode);
        }
        if (PURE_OPEN_CLOSE.equals(strategyCode)) {
            return effectiveDelta == HyperliquidDeltaType.OPEN || effectiveDelta == HyperliquidDeltaType.CLOSE;
        }
        if (FLIP_ONLY.equals(strategyCode)) {
            return effectiveDelta == HyperliquidDeltaType.FLIP;
        }
        return ALL_FLOW_STRATEGIES.contains(strategyCode);
    }

    private double strategySpecificity(MetricaWalletDto metric) {
        String code = strategyCodeOf(metric);
        return switch (code) {
            case PURE_OPEN_CLOSE -> 90.0;
            case FLIP_ONLY -> 80.0;
            case LONG_ONLY, SHORT_ONLY -> 70.0;
            case FIRST_OPEN_FINAL_CLOSE -> 60.0;
            case MOVEMENT_ALL -> 50.0;
            case RECENT_7D -> 40.0;
            case RECENT_14D -> 35.0;
            case RECENT_30D -> 30.0;
            default -> 0.0;
        };
    }

    private double strategyScore(MetricaWalletDto metric) {
        if (metric == null || metric.getStrategy() == null || metric.getStrategy().getScore() == null) {
            return 0.0;
        }
        return metric.getStrategy().getScore();
    }

    private double decisionScore(MetricaWalletDto metric) {
        if (metric == null || metric.getScoring() == null) {
            return 0.0;
        }
        Integer score = metric.getScoring().getDecisionMetricConservative();
        return score == null ? 0.0 : score.doubleValue();
    }

    private static boolean sameOrigin(String a, String b) {
        if (a == null || b == null) return false;
        return Objects.equals(a.trim(), b.trim());
    }

    private static String fromPlatform(String raw) {
        String platform = normalizeLower(raw);
        if (platform == null) return null;
        int idx = platform.lastIndexOf(':');
        if (idx >= 0 && idx < platform.length() - 1) {
            return fromSlug(platform.substring(idx + 1));
        }
        return fromSlug(platform);
    }

    private static String fromSlug(String raw) {
        String slug = normalizeLower(raw);
        if (slug == null) return null;
        return switch (slug.replace('_', '-')) {
            case "long" -> LONG_ONLY;
            case "short" -> SHORT_ONLY;
            case "movement", "movement-all" -> MOVEMENT_ALL;
            case "pure-open-close", "open-close-clean", "untouched-open-close" -> PURE_OPEN_CLOSE;
            case "first-open-final-close" -> FIRST_OPEN_FINAL_CLOSE;
            case "flip", "flip-only" -> FLIP_ONLY;
            case "recent-7d" -> RECENT_7D;
            case "recent-14d" -> RECENT_14D;
            case "recent-30d" -> RECENT_30D;
            default -> normalizeStrategyCode(slug);
        };
    }

    private static String normalize(String raw) {
        if (raw == null) return null;
        String value = raw.trim().toUpperCase(Locale.ROOT);
        return value.isEmpty() ? null : value;
    }

    private static String normalizeLower(String raw) {
        if (raw == null) return null;
        String value = raw.trim().toLowerCase(Locale.ROOT);
        return value.isEmpty() ? null : value;
    }
}
