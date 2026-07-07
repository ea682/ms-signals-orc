package com.apunto.engine.service.copy.promotion;

import java.util.Locale;
import java.util.Map;
import java.util.Set;

public final class UserCopyAllocationCopyModeResolver {

    public static final String COPY_ALL_METRIC_MOVEMENTS = "copy_all_metric_movements";
    public static final String COPY_ONLY_SHORT_EVENTS = "copy_only_short_events";
    public static final String COPY_ONLY_LONG_EVENTS = "copy_only_long_events";
    public static final String COPY_OPEN_AND_FULL_CLOSE_ONLY = "copy_open_and_full_close_only";
    public static final String COPY_FIRST_OPEN_FINAL_CLOSE = "copy_first_open_final_close";
    public static final String COPY_STRATEGY_FILTERED_EVENTS = "copy_strategy_filtered_events";
    public static final String COPY_ONLY_FLIP_EVENTS = "copy_only_flip_events";

    public static final String COPY_MODE_RESOLVED = "COPY_MODE_RESOLVED";
    public static final String COPY_MODE_MAPPING_FALLBACK = "COPY_MODE_MAPPING_FALLBACK";
    public static final String COPY_MODE_CONSTRAINT_SAFE = "COPY_MODE_CONSTRAINT_SAFE";
    public static final String INVALID_COPY_MODE_MAPPING = "INVALID_COPY_MODE_MAPPING";

    private static final Set<String> ALLOWED_COPY_MODES = Set.of(
            COPY_ALL_METRIC_MOVEMENTS,
            COPY_ONLY_SHORT_EVENTS,
            COPY_ONLY_LONG_EVENTS,
            COPY_OPEN_AND_FULL_CLOSE_ONLY,
            COPY_FIRST_OPEN_FINAL_CLOSE,
            COPY_STRATEGY_FILTERED_EVENTS,
            COPY_ONLY_FLIP_EVENTS
    );

    private static final Map<String, String> STRATEGY_COPY_MODES = Map.ofEntries(
            Map.entry("MOVEMENT_ALL", COPY_ALL_METRIC_MOVEMENTS),
            Map.entry("SHORT_ONLY", COPY_ONLY_SHORT_EVENTS),
            Map.entry("LONG_ONLY", COPY_ONLY_LONG_EVENTS),
            Map.entry("OPEN_CLOSE_ONLY", COPY_OPEN_AND_FULL_CLOSE_ONLY),
            Map.entry("OPEN_AND_FULL_CLOSE_ONLY", COPY_OPEN_AND_FULL_CLOSE_ONLY),
            Map.entry("PURE_OPEN_CLOSE", COPY_OPEN_AND_FULL_CLOSE_ONLY),
            Map.entry("FIRST_OPEN_FINAL_CLOSE", COPY_FIRST_OPEN_FINAL_CLOSE),
            Map.entry("FLIP_ONLY", COPY_ONLY_FLIP_EVENTS),
            Map.entry("SYMBOL_SPECIALIST", COPY_STRATEGY_FILTERED_EVENTS),
            Map.entry("LOW_LEVERAGE_ONLY", COPY_STRATEGY_FILTERED_EVENTS),
            Map.entry("TOP_SYMBOLS_ONLY", COPY_STRATEGY_FILTERED_EVENTS),
            Map.entry("MAJORS_ONLY", COPY_STRATEGY_FILTERED_EVENTS),
            Map.entry("HIGH_LIQUIDITY_ONLY", COPY_STRATEGY_FILTERED_EVENTS),
            Map.entry("HIGH_QUALITY_SYMBOLS_ONLY", COPY_STRATEGY_FILTERED_EVENTS),
            Map.entry("SWING_ONLY", COPY_STRATEGY_FILTERED_EVENTS)
    );

    private static final Map<String, String> LEGACY_SOURCE_COPY_MODES = Map.ofEntries(
            Map.entry("copy_movement_all_events", COPY_ALL_METRIC_MOVEMENTS),
            Map.entry("copy_all_movements", COPY_ALL_METRIC_MOVEMENTS),
            Map.entry("copy_all_events", COPY_ALL_METRIC_MOVEMENTS),
            Map.entry("copy_short_events", COPY_ONLY_SHORT_EVENTS),
            Map.entry("copy_long_events", COPY_ONLY_LONG_EVENTS),
            Map.entry("copy_open_close_events", COPY_OPEN_AND_FULL_CLOSE_ONLY),
            Map.entry("copy_first_open_final_close_events", COPY_FIRST_OPEN_FINAL_CLOSE),
            Map.entry("copy_flip_events", COPY_ONLY_FLIP_EVENTS)
    );

    private UserCopyAllocationCopyModeResolver() {
    }

    public static CopyModeResolution resolve(String strategyCode, String sourceCopyMode) {
        String strategy = normalizeStrategy(strategyCode);
        String source = normalizeCopyMode(sourceCopyMode);

        String fromStrategy = strategy == null ? null : STRATEGY_COPY_MODES.get(strategy);
        if (fromStrategy != null) {
            String reason = COPY_STRATEGY_FILTERED_EVENTS.equals(fromStrategy)
                    ? COPY_MODE_MAPPING_FALLBACK
                    : COPY_MODE_RESOLVED;
            return CopyModeResolution.valid(fromStrategy, reason, strategy, source);
        }

        if (isAllowedCopyMode(source)) {
            return CopyModeResolution.valid(source, COPY_MODE_MAPPING_FALLBACK, strategy, source);
        }

        String legacy = source == null ? null : LEGACY_SOURCE_COPY_MODES.get(source);
        if (legacy != null) {
            return CopyModeResolution.valid(legacy, COPY_MODE_MAPPING_FALLBACK, strategy, source);
        }

        return CopyModeResolution.invalid(strategy, source);
    }

    public static boolean isAllowedCopyMode(String copyMode) {
        String normalized = normalizeCopyMode(copyMode);
        return normalized != null && ALLOWED_COPY_MODES.contains(normalized);
    }

    private static String normalizeStrategy(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        return value.trim().toUpperCase(Locale.ROOT).replace('-', '_');
    }

    private static String normalizeCopyMode(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        return value.trim().toLowerCase(Locale.ROOT).replace('-', '_');
    }

    public record CopyModeResolution(
            boolean valid,
            String copyMode,
            String reasonCode,
            String constraintReasonCode,
            String normalizedStrategyCode,
            String normalizedSourceCopyMode
    ) {
        private static CopyModeResolution valid(String copyMode, String reasonCode, String strategyCode, String sourceCopyMode) {
            return new CopyModeResolution(true, copyMode, reasonCode, COPY_MODE_CONSTRAINT_SAFE, strategyCode, sourceCopyMode);
        }

        private static CopyModeResolution invalid(String strategyCode, String sourceCopyMode) {
            return new CopyModeResolution(false, null, INVALID_COPY_MODE_MAPPING, null, strategyCode, sourceCopyMode);
        }
    }
}
