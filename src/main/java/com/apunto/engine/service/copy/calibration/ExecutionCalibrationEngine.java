package com.apunto.engine.service.copy.calibration;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

public class ExecutionCalibrationEngine {

    private static final int SCALE = 12;
    private final int mediumSampleSize;
    private final int highSampleSize;
    private final Duration maximumAge;

    public ExecutionCalibrationEngine(int mediumSampleSize, int highSampleSize, Duration maximumAge) {
        if (mediumSampleSize <= 0 || highSampleSize < mediumSampleSize
                || maximumAge == null || maximumAge.isNegative() || maximumAge.isZero()) {
            throw new IllegalArgumentException("INVALID_CALIBRATION_POLICY");
        }
        this.mediumSampleSize = mediumSampleSize;
        this.highSampleSize = highSampleSize;
        this.maximumAge = maximumAge;
    }

    public CalibrationEstimate estimate(CalibrationSegment requested,
                                        List<CalibrationObservation> observations,
                                        OffsetDateTime now) {
        Objects.requireNonNull(requested, "requested");
        Objects.requireNonNull(now, "now");
        List<CalibrationObservation> valid = observations == null ? List.of() : observations.stream()
                .filter(Objects::nonNull)
                .filter(observation -> observation.segment() != null && observation.value() != null
                        && observation.observedAt() != null && !observation.observedAt().isAfter(now))
                .toList();
        List<CalibrationObservation> fresh = valid.stream()
                .filter(observation -> !observation.observedAt().isBefore(now.minus(maximumAge)))
                .toList();
        if (fresh.isEmpty()) {
            return unavailable(valid.isEmpty() ? "CALIBRATION_EVIDENCE_MISSING" : "CALIBRATION_STALE");
        }

        Match match = firstMatch(requested, fresh);
        if (match.values().isEmpty()) return unavailable("CALIBRATION_EVIDENCE_MISSING");
        List<WeightedValue> sorted = match.values().stream()
                .map(observation -> new WeightedValue(observation.value(), observation.sampleCount()))
                .sorted(Comparator.comparing(WeightedValue::value))
                .toList();
        BigDecimal p10 = percentile(sorted, new BigDecimal("0.10"));
        BigDecimal p90 = percentile(sorted, new BigDecimal("0.90"));
        List<WeightedValue> winsorized = sorted.stream()
                .map(value -> new WeightedValue(value.value().max(p10).min(p90), value.weight()))
                .toList();
        BigDecimal mean = average(winsorized);
        BigDecimal stddev = standardDeviation(winsorized, mean);
        int count = sorted.stream().mapToInt(WeightedValue::weight).sum();
        int effectiveCount = sorted.size();
        List<String> reasons = new ArrayList<>();
        if (!"EXACT".equals(match.level())) reasons.add("CALIBRATION_FALLBACK_" + match.level());
        if (sorted.stream().allMatch(value -> value.value().signum() == 0)) reasons.add("ZERO_REAL");
        if (effectiveCount < mediumSampleSize) reasons.add("CALIBRATION_SMALL_SAMPLE");
        return new CalibrationEstimate(
                true,
                percentile(sorted, new BigDecimal("0.50")),
                percentile(sorted, new BigDecimal("0.75")),
                p90,
                mean,
                stddev,
                count,
                effectiveCount,
                match.values().stream().map(CalibrationObservation::observedAt).min(Comparator.naturalOrder()).orElse(null),
                match.values().stream().map(CalibrationObservation::observedAt).max(Comparator.naturalOrder()).orElse(null),
                confidence(effectiveCount, match.level()),
                match.level(),
                match.values().stream().map(CalibrationObservation::modelVersion)
                        .filter(Objects::nonNull).sorted().reduce((left, right) -> right).orElse("UNKNOWN"),
                List.copyOf(reasons));
    }

    private Match firstMatch(CalibrationSegment requested, List<CalibrationObservation> values) {
        List<Level> levels = List.of(
                new Level("EXACT", segment -> segment.equals(requested)),
                new Level("SYMBOL_SIDE_ACTION", segment -> segment.symbol().equals(requested.symbol())
                        && segment.side().equals(requested.side()) && segment.action().equals(requested.action())),
                new Level("SYMBOL_ACTION", segment -> segment.symbol().equals(requested.symbol())
                        && segment.action().equals(requested.action())),
                new Level("ACTION_NOTIONAL", segment -> segment.action().equals(requested.action())
                        && segment.notionalBand().equals(requested.notionalBand())),
                new Level("ACTION", segment -> segment.action().equals(requested.action())),
                new Level("GLOBAL", segment -> true));
        for (Level level : levels) {
            List<CalibrationObservation> matched = values.stream()
                    .filter(observation -> level.matcher().test(observation.segment()))
                    .toList();
            if (!matched.isEmpty()) return new Match(level.name(), matched);
        }
        return new Match("NONE", List.of());
    }

    private CalibrationEstimate unavailable(String reason) {
        return new CalibrationEstimate(false, null, null, null, null, null, 0, 0,
                null, null, CalibrationConfidence.LOW, "NONE", "UNKNOWN", List.of(reason));
    }

    private CalibrationConfidence confidence(int count, String fallbackLevel) {
        CalibrationConfidence raw = count >= highSampleSize
                ? CalibrationConfidence.HIGH
                : count >= mediumSampleSize ? CalibrationConfidence.MEDIUM : CalibrationConfidence.LOW;
        if ("EXACT".equals(fallbackLevel)) return raw;
        return raw == CalibrationConfidence.HIGH ? CalibrationConfidence.MEDIUM : CalibrationConfidence.LOW;
    }

    private BigDecimal percentile(List<WeightedValue> values, BigDecimal percentile) {
        if (values.size() == 1) return values.getFirst().value().stripTrailingZeros();
        if (values.stream().allMatch(value -> value.weight() == 1)) {
            return unweightedPercentile(values.stream().map(WeightedValue::value).toList(), percentile);
        }
        int totalWeight = values.stream().mapToInt(WeightedValue::weight).sum();
        BigDecimal threshold = percentile.multiply(BigDecimal.valueOf(totalWeight));
        int cumulative = 0;
        for (WeightedValue value : values) {
            cumulative += value.weight();
            if (BigDecimal.valueOf(cumulative).compareTo(threshold) >= 0) {
                return value.value().stripTrailingZeros();
            }
        }
        return values.getLast().value().stripTrailingZeros();
    }

    private BigDecimal unweightedPercentile(List<BigDecimal> values, BigDecimal percentile) {
        BigDecimal index = percentile.multiply(BigDecimal.valueOf(values.size() - 1L));
        int lower = index.setScale(0, RoundingMode.FLOOR).intValueExact();
        int upper = index.setScale(0, RoundingMode.CEILING).intValueExact();
        if (lower == upper) return values.get(lower).stripTrailingZeros();
        BigDecimal fraction = index.subtract(BigDecimal.valueOf(lower));
        return values.get(lower).add(values.get(upper).subtract(values.get(lower)).multiply(fraction))
                .setScale(SCALE, RoundingMode.HALF_UP).stripTrailingZeros();
    }

    private BigDecimal average(List<WeightedValue> values) {
        BigDecimal weightedSum = values.stream()
                .map(value -> value.value().multiply(BigDecimal.valueOf(value.weight())))
                .reduce(BigDecimal.ZERO, BigDecimal::add);
        int totalWeight = values.stream().mapToInt(WeightedValue::weight).sum();
        return weightedSum.divide(BigDecimal.valueOf(totalWeight), SCALE, RoundingMode.HALF_UP)
                .stripTrailingZeros();
    }

    private BigDecimal standardDeviation(List<WeightedValue> values, BigDecimal mean) {
        int totalWeight = values.stream().mapToInt(WeightedValue::weight).sum();
        if (totalWeight < 2) return BigDecimal.ZERO;
        BigDecimal variance = values.stream()
                .map(value -> value.value().subtract(mean).pow(2)
                        .multiply(BigDecimal.valueOf(value.weight())))
                .reduce(BigDecimal.ZERO, BigDecimal::add)
                .divide(BigDecimal.valueOf(totalWeight - 1L), SCALE, RoundingMode.HALF_UP);
        return BigDecimal.valueOf(Math.sqrt(variance.doubleValue()))
                .setScale(SCALE, RoundingMode.HALF_UP).stripTrailingZeros();
    }

    private record Level(String name, Predicate<CalibrationSegment> matcher) {}
    private record Match(String level, List<CalibrationObservation> values) {}
    private record WeightedValue(BigDecimal value, int weight) {}
}
