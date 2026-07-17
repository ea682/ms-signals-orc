package com.apunto.engine.service.copy;

import com.apunto.engine.shared.metric.MetricStrategyIdentity;
import com.apunto.engine.dto.client.MetricaWalletDto;
import com.apunto.engine.entity.UserCopyAllocationEntity;
import com.apunto.engine.hyperliquid.model.HyperliquidDeltaType;
import com.apunto.engine.jobs.model.CopyJobAction;
import com.apunto.engine.shared.enums.PositionSide;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@Component
public class CopyStrategyRuntimeRouter {

    public static final String DEFAULT_STRATEGY_CODE = "MOVEMENT_ALL";
    public static final String LONG_ONLY = "LONG_ONLY";
    public static final String SHORT_ONLY = "SHORT_ONLY";
    public static final String MOVEMENT_ALL = "MOVEMENT_ALL";
    public static final String PURE_OPEN_CLOSE = "PURE_OPEN_CLOSE";
    public static final String FIRST_OPEN_FINAL_CLOSE = "FIRST_OPEN_FINAL_CLOSE";
    public static final String FLIP_ONLY = "FLIP_ONLY";
    public static final String SYMBOL_SPECIALIST = "SYMBOL_SPECIALIST";
    public static final String HIGH_LIQUIDITY_ONLY = "HIGH_LIQUIDITY_ONLY";
    public static final String MAJORS_ONLY = "MAJORS_ONLY";
    public static final String HIGH_QUALITY_SYMBOLS_ONLY = "HIGH_QUALITY_SYMBOLS_ONLY";
    public static final String LOW_LEVERAGE_ONLY = "LOW_LEVERAGE_ONLY";
    public static final String TOP_SYMBOLS_ONLY = "TOP_SYMBOLS_ONLY";
    public static final String SWING_ONLY = "SWING_ONLY";
    public static final String RECENT_7D = "RECENT_7D";
    public static final String RECENT_14D = "RECENT_14D";
    public static final String RECENT_30D = "RECENT_30D";
    public static final String ROBUST_EX_TOP_1 = "ROBUST_EX_TOP_1";
    public static final String ROBUST_EX_TOP_5 = "ROBUST_EX_TOP_5";
    public static final String PARTIAL_REDUCE = "PARTIAL_REDUCE";
    public static final String FINAL_CLOSE_ONLY = "FINAL_CLOSE_ONLY";

    private static final Set<String> CORE_COPY_PROFILES = Set.of(
            MOVEMENT_ALL,
            LONG_ONLY,
            SHORT_ONLY
    );

    private static final Set<String> ADVANCED_COPY_PROFILES = Set.of(
            FIRST_OPEN_FINAL_CLOSE,
            PURE_OPEN_CLOSE,
            FLIP_ONLY,
            SYMBOL_SPECIALIST,
            HIGH_LIQUIDITY_ONLY,
            MAJORS_ONLY,
            HIGH_QUALITY_SYMBOLS_ONLY,
            LOW_LEVERAGE_ONLY,
            TOP_SYMBOLS_ONLY,
            SWING_ONLY
    );

    private static final Set<String> SCORING_WINDOWS = Set.of(
            RECENT_7D,
            RECENT_14D,
            RECENT_30D
    );

    private static final Set<String> ROBUSTNESS_CHECKS = Set.of(
            ROBUST_EX_TOP_1,
            ROBUST_EX_TOP_5
    );

    private static final Set<String> DIAGNOSTIC_ONLY = Set.of(
            PARTIAL_REDUCE,
            FINAL_CLOSE_ONLY,
            "REDUCE_ONLY",
            "CLOSE_ONLY"
    );

    private static final Set<String> ALL_FLOW_COPY_PROFILES = Set.of(
            MOVEMENT_ALL,
            FIRST_OPEN_FINAL_CLOSE,
            SYMBOL_SPECIALIST,
            HIGH_LIQUIDITY_ONLY,
            MAJORS_ONLY,
            HIGH_QUALITY_SYMBOLS_ONLY,
            LOW_LEVERAGE_ONLY,
            TOP_SYMBOLS_ONLY,
            SWING_ONLY
    );

    @Value("${copy-profile.movement-all-enabled:true}")
    private boolean movementAllEnabled = true;

    @Value("${copy-profile.long-only-enabled:true}")
    private boolean longOnlyEnabled = true;

    @Value("${copy-profile.short-only-enabled:true}")
    private boolean shortOnlyEnabled = true;

    @Value("${copy-profile.first-open-final-close-enabled:true}")
    private boolean firstOpenFinalCloseEnabled = true;

    @Value("${copy-profile.pure-open-close-enabled:true}")
    private boolean pureOpenCloseEnabled = true;

    @Value("${copy-profile.flip-only-enabled:true}")
    private boolean flipOnlyEnabled = true;

    @Value("${copy-profile.symbol-specialist-enabled:true}")
    private boolean symbolSpecialistEnabled = true;

    @Value("${copy-profile.high-liquidity-only-enabled:true}")
    private boolean highLiquidityOnlyEnabled = true;

    @Value("${copy-profile.majors-only-enabled:true}")
    private boolean majorsOnlyEnabled = true;

    @Value("${copy-profile.high-quality-symbols-only-enabled:true}")
    private boolean highQualitySymbolsOnlyEnabled = true;

    @Value("${copy-profile.low-leverage-only-enabled:true}")
    private boolean lowLeverageOnlyEnabled = true;

    @Value("${copy-profile.top-symbols-only-enabled:true}")
    private boolean topSymbolsOnlyEnabled = true;

    @Value("${copy-profile.swing-only-enabled:true}")
    private boolean swingOnlyEnabled = true;

    @Value("${copy-profile.recent-windows-as-live-enabled:false}")
    private boolean recentWindowsAsLiveEnabled = false;

    @Value("${copy-shadow.enable-scoring-windows:false}")
    private boolean shadowEnableScoringWindows = false;

    @Value("${copy-profile.majors-symbols:BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,XRPUSDT,DOGEUSDT,LINKUSDT,AVAXUSDT}")
    private String majorsSymbols = "BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,XRPUSDT,DOGEUSDT,LINKUSDT,AVAXUSDT";

    @Value("${copy-profile.blocked-symbols:FARTCOINUSDT,LITUSDT}")
    private String blockedSymbols = "FARTCOINUSDT,LITUSDT";

    @Value("${copy-profile.allowed-symbols:}")
    private String allowedSymbols = "";

    public enum CopyProfileCategory {
        CORE_COPY_PROFILE,
        ADVANCED_COPY_PROFILE,
        SCORING_WINDOW,
        ROBUSTNESS_CHECK,
        DIAGNOSTIC_ONLY,
        UNKNOWN
    }

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

    public String allocationKey(String walletId, String strategyCode, String scopeType, String scopeValue) {
        return profileKey(walletId, strategyCode, scopeType, scopeValue);
    }

    public String allocationKey(UserCopyAllocationEntity allocation) {
        if (allocation == null) {
            return null;
        }
        return allocationKey(
                allocation.getWalletId(),
                strategyCodeOf(allocation),
                allocation.getScopeType(),
                allocation.getScopeValue()
        );
    }

    public String profileKey(String walletId, String strategyCode, String scopeType, String scopeValue) {
        String wallet = normalizeWalletId(walletId);
        if (wallet == null) return null;
        return MetricStrategyIdentity.canonicalKey(wallet, strategyCode, scopeType, scopeValue);
    }

    public boolean isCopyableJoyasCandidate(MetricaWalletDto metric) {
        return isLiveEligibleJoyasCandidate(metric);
    }

    public boolean isShadowEligibleJoyasCandidate(MetricaWalletDto metric) {
        return profileEligible(metric, true);
    }

    public boolean isLiveEligibleJoyasCandidate(MetricaWalletDto metric) {
        return profileEligible(metric, false);
    }

    public CopyProfileCategory profileCategory(String strategyCode) {
        String code = normalizeStrategyCode(strategyCode);
        if (code == null) return CopyProfileCategory.UNKNOWN;
        if (CORE_COPY_PROFILES.contains(code)) return CopyProfileCategory.CORE_COPY_PROFILE;
        if (ADVANCED_COPY_PROFILES.contains(code)) return CopyProfileCategory.ADVANCED_COPY_PROFILE;
        if (SCORING_WINDOWS.contains(code)) return CopyProfileCategory.SCORING_WINDOW;
        if (ROBUSTNESS_CHECKS.contains(code)) return CopyProfileCategory.ROBUSTNESS_CHECK;
        if (DIAGNOSTIC_ONLY.contains(code)) return CopyProfileCategory.DIAGNOSTIC_ONLY;
        return CopyProfileCategory.UNKNOWN;
    }

    public boolean isScoringWindow(String strategyCode) {
        return profileCategory(strategyCode) == CopyProfileCategory.SCORING_WINDOW;
    }

    public boolean allocationAppliesToEvent(
            UserCopyAllocationEntity allocation,
            CopyJobAction action,
            HyperliquidDeltaType deltaType,
            String side
    ) {
        return allocationAppliesToEvent(allocation, action, deltaType, side, null);
    }

    public boolean allocationAppliesToEvent(
            UserCopyAllocationEntity allocation,
            CopyJobAction action,
            HyperliquidDeltaType deltaType,
            String side,
            String symbol
    ) {
        return strategyAppliesToEvent(strategyCodeOf(allocation), action, deltaType, side, symbol, allocation == null ? null : allocation.getScopeValue());
    }

    public boolean metricAppliesToEvent(
            MetricaWalletDto metric,
            CopyJobAction action,
            HyperliquidDeltaType deltaType,
            String side
    ) {
        return strategyAppliesToEvent(strategyCodeOf(metric), action, deltaType, side, null, scopeValue(metric));
    }

    public boolean strategyCodeAppliesToEvent(
            String strategyCode,
            CopyJobAction action,
            HyperliquidDeltaType deltaType,
            String side
    ) {
        return strategyAppliesToEvent(strategyCode, action, deltaType, side, null, null);
    }

    public boolean strategyCodeAppliesToEvent(
            String strategyCode,
            String scopeValue,
            CopyJobAction action,
            HyperliquidDeltaType deltaType,
            String side,
            String symbol
    ) {
        return strategyAppliesToEvent(strategyCode, action, deltaType, side, symbol, scopeValue);
    }

    public boolean metricAllowsTargetLeg(
            MetricaWalletDto metric,
            PositionSide side,
            HyperliquidDeltaType triggerDeltaType,
            String legOriginId,
            String triggerOriginId
    ) {
        return metricAllowsTargetLeg(metric, side, triggerDeltaType, legOriginId, triggerOriginId, null);
    }

    public boolean metricAllowsTargetLeg(
            MetricaWalletDto metric,
            PositionSide side,
            HyperliquidDeltaType triggerDeltaType,
            String legOriginId,
            String triggerOriginId,
            String symbol
    ) {
        String strategy = strategyCodeOf(metric);
        String sideCode = side == null ? null : normalize(side.name());

        if (!symbolAllowedForStrategy(strategy, symbol, scopeValue(metric))) {
            return false;
        }
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

    private boolean profileEligible(MetricaWalletDto metric, boolean shadow) {
        String code = strategyCodeOf(metric);
        CopyProfileCategory category = profileCategory(code);
        if (category == CopyProfileCategory.SCORING_WINDOW) {
            return (shadow ? shadowEnableScoringWindows : recentWindowsAsLiveEnabled) && metricCopyabilityAllows(metric);
        }
        if (category == CopyProfileCategory.ROBUSTNESS_CHECK
                || category == CopyProfileCategory.DIAGNOSTIC_ONLY
                || category == CopyProfileCategory.UNKNOWN) {
            return false;
        }
        return profileEnabled(code) && metricCopyabilityAllows(metric);
    }

    private boolean metricCopyabilityAllows(MetricaWalletDto metric) {
        MetricaWalletDto.StrategyDto strategy = metric == null ? null : metric.getStrategy();
        MetricaWalletDto.CopyabilityDto copyability = strategy == null ? null : strategy.getCopyability();
        if (copyability == null) {
            return true;
        }

        String type = normalize(copyability.getType());
        Boolean canOpenPosition = copyability.getCanOpenPosition();
        Boolean supportedByJoyas = copyability.getSupportedByJoyas();

        return !"DIAGNOSTIC_ONLY".equals(type)
                && !"VALIDATION_ONLY".equals(type)
                && !"ROBUSTNESS_CHECK".equals(type)
                && !"SCORING_WINDOW".equals(type)
                && !Boolean.FALSE.equals(canOpenPosition)
                && !Boolean.FALSE.equals(supportedByJoyas);
    }

    private boolean strategyAppliesToEvent(
            String strategy,
            CopyJobAction action,
            HyperliquidDeltaType deltaType,
            String side,
            String symbol,
            String scopeValue
    ) {
        String strategyCode = normalizeStrategyCode(strategy);
        if (strategyCode == null) strategyCode = DEFAULT_STRATEGY_CODE;

        if (!profileEnabled(strategyCode)) {
            return false;
        }
        CopyProfileCategory category = profileCategory(strategyCode);
        if (category == CopyProfileCategory.SCORING_WINDOW
                || category == CopyProfileCategory.ROBUSTNESS_CHECK
                || category == CopyProfileCategory.DIAGNOSTIC_ONLY
                || category == CopyProfileCategory.UNKNOWN) {
            return false;
        }

        HyperliquidDeltaType effectiveDelta = deltaType == null ? HyperliquidDeltaType.UNKNOWN : deltaType;
        if (effectiveDelta == HyperliquidDeltaType.UPDATE
                || effectiveDelta == HyperliquidDeltaType.NO_CHANGE
                || effectiveDelta == HyperliquidDeltaType.UNKNOWN) {
            return false;
        }

        if (!symbolAllowedForStrategy(strategyCode, symbol, scopeValue)) {
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
        return ALL_FLOW_COPY_PROFILES.contains(strategyCode);
    }

    private boolean symbolAllowedForStrategy(String strategyCode, String symbol, String scopeValue) {
        String code = normalizeStrategyCode(strategyCode);
        if (code == null) {
            return true;
        }
        if (SYMBOL_SPECIALIST.equals(code)) {
            String scope = normalizeSymbol(scopeValue);
            return scope == null || "ALL".equals(scope) || symbolMatches(symbol, Set.of(scope));
        }
        if (MAJORS_ONLY.equals(code)) {
            return symbolMatches(symbol, parseCsv(majorsSymbols));
        }
        if (HIGH_QUALITY_SYMBOLS_ONLY.equals(code)) {
            Set<String> allowed = parseCsv(allowedSymbols);
            if (!allowed.isEmpty() && !symbolMatches(symbol, allowed)) {
                return false;
            }
            return !symbolMatches(symbol, parseCsv(blockedSymbols));
        }
        return true;
    }

    private boolean profileEnabled(String code) {
        String strategyCode = normalizeStrategyCode(code);
        if (strategyCode == null) return false;
        return switch (strategyCode) {
            case MOVEMENT_ALL -> movementAllEnabled;
            case LONG_ONLY -> longOnlyEnabled;
            case SHORT_ONLY -> shortOnlyEnabled;
            case FIRST_OPEN_FINAL_CLOSE -> firstOpenFinalCloseEnabled;
            case PURE_OPEN_CLOSE -> pureOpenCloseEnabled;
            case FLIP_ONLY -> flipOnlyEnabled;
            case SYMBOL_SPECIALIST -> symbolSpecialistEnabled;
            case HIGH_LIQUIDITY_ONLY -> highLiquidityOnlyEnabled;
            case MAJORS_ONLY -> majorsOnlyEnabled;
            case HIGH_QUALITY_SYMBOLS_ONLY -> highQualitySymbolsOnlyEnabled;
            case LOW_LEVERAGE_ONLY -> lowLeverageOnlyEnabled;
            case TOP_SYMBOLS_ONLY -> topSymbolsOnlyEnabled;
            case SWING_ONLY -> swingOnlyEnabled;
            case RECENT_7D, RECENT_14D, RECENT_30D -> recentWindowsAsLiveEnabled;
            default -> false;
        };
    }

    private double strategySpecificity(MetricaWalletDto metric) {
        String code = strategyCodeOf(metric);
        return switch (code) {
            case SYMBOL_SPECIALIST -> 100.0;
            case TOP_SYMBOLS_ONLY -> 96.0;
            case HIGH_QUALITY_SYMBOLS_ONLY -> 94.0;
            case MAJORS_ONLY -> 92.0;
            case LOW_LEVERAGE_ONLY -> 91.0;
            case HIGH_LIQUIDITY_ONLY -> 90.0;
            case SWING_ONLY -> 88.0;
            case PURE_OPEN_CLOSE -> 86.0;
            case FLIP_ONLY -> 84.0;
            case LONG_ONLY, SHORT_ONLY -> 80.0;
            case FIRST_OPEN_FINAL_CLOSE -> 70.0;
            case MOVEMENT_ALL -> 50.0;
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

    private static boolean symbolMatches(String rawSymbol, Set<String> configuredSymbols) {
        String symbol = normalizeSymbol(rawSymbol);
        if (symbol == null || configuredSymbols == null || configuredSymbols.isEmpty()) {
            return false;
        }
        String base = stripQuote(symbol);
        return configuredSymbols.contains(symbol) || configuredSymbols.contains(base);
    }

    private static Set<String> parseCsv(String raw) {
        if (raw == null || raw.isBlank()) {
            return Set.of();
        }
        return Arrays.stream(raw.split(","))
                .map(CopyStrategyRuntimeRouter::normalizeSymbol)
                .filter(Objects::nonNull)
                .collect(Collectors.toUnmodifiableSet());
    }

    private static String scopeValue(MetricaWalletDto metric) {
        if (metric == null) return null;
        if (metric.getWallet() != null && metric.getWallet().getCountOperationBreakdown() != null) {
            String value = metric.getWallet().getCountOperationBreakdown().getScopeValue();
            if (value != null && !value.isBlank()) return value;
        }
        if (metric.getRealJewel() != null && metric.getRealJewel().getScopeValue() != null) {
            return metric.getRealJewel().getScopeValue();
        }
        return null;
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
            case "symbol", "symbol-specialist" -> SYMBOL_SPECIALIST;
            case "high-liquidity-only" -> HIGH_LIQUIDITY_ONLY;
            case "majors-only" -> MAJORS_ONLY;
            case "high-quality-symbols-only" -> HIGH_QUALITY_SYMBOLS_ONLY;
            case "low-leverage-only" -> LOW_LEVERAGE_ONLY;
            case "top-symbols-only" -> TOP_SYMBOLS_ONLY;
            case "swing-only" -> SWING_ONLY;
            case "recent-7d" -> RECENT_7D;
            case "recent-14d" -> RECENT_14D;
            case "recent-30d" -> RECENT_30D;
            case "robust-ex-top-1" -> ROBUST_EX_TOP_1;
            case "robust-ex-top-5" -> ROBUST_EX_TOP_5;
            case "partial-reduce", "reduce-only", "reduce" -> PARTIAL_REDUCE;
            case "final-close-only", "close-only", "final-close" -> FINAL_CLOSE_ONLY;
            default -> normalizeStrategyCode(slug);
        };
    }

    private static String normalize(String raw) {
        if (raw == null) return null;
        String value = raw.trim().toUpperCase(Locale.ROOT);
        return value.isEmpty() ? null : value;
    }


    private static String normalizeScopeType(String raw, String strategyCode) {
        String value = normalize(raw);
        if (value != null && !"STRATEGY".equals(value) && !"DEFAULT".equals(value)) {
            return value;
        }
        if (LONG_ONLY.equals(strategyCode) || SHORT_ONLY.equals(strategyCode)) return "DIRECTION";
        if (SYMBOL_SPECIALIST.equals(strategyCode)) return "SYMBOL";
        return "ALL";
    }

    private static String normalizeScopeValue(String raw, String strategyCode) {
        String value = normalize(raw);
        if (value != null && !"DEFAULT".equals(value) && !"STRATEGY".equals(value)) return value;
        if (LONG_ONLY.equals(strategyCode)) return "LONG";
        if (SHORT_ONLY.equals(strategyCode)) return "SHORT";
        return "ALL";
    }

    private static String normalizeLower(String raw) {
        if (raw == null) return null;
        String value = raw.trim().toLowerCase(Locale.ROOT);
        return value.isEmpty() ? null : value;
    }

    private static String normalizeSymbol(String raw) {
        if (raw == null) return null;
        String value = raw.trim().toUpperCase(Locale.ROOT).replace("-", "").replace("_", "");
        return value.isEmpty() ? null : value;
    }

    private static String stripQuote(String symbol) {
        String value = normalizeSymbol(symbol);
        if (value == null) return null;
        for (String quote : ListHolder.QUOTES) {
            if (value.endsWith(quote) && value.length() > quote.length()) {
                return value.substring(0, value.length() - quote.length());
            }
        }
        return value;
    }

    private static final class ListHolder {
        private static final String[] QUOTES = {"USDT", "USDC", "USD", "BTC", "ETH"};
    }
}
