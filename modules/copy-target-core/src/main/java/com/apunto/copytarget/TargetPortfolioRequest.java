package com.apunto.copytarget;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

public final class TargetPortfolioRequest {
    private final Instant calculatedAt;
    private final BigDecimal sourceAccountEquityUsd;
    private final Instant equityObservedAt;
    private final String equitySource;
    private final Duration maximumEquityAge;
    private final long sourceSnapshotVersion;
    private final List<SourcePosition> sourcePositions;
    private final BigDecimal targetAllocatedCapitalUsd;
    private final BigDecimal targetLeverage;
    private final BigDecimal availableMarginUsd;
    private final BigDecimal usedMarginUsd;
    private final BigDecimal reservedMarginUsd;
    private final List<ExistingTargetPosition> existingPositions;
    private final List<ExistingTargetPosition> managedExistingPositions;
    private final List<ExistingTargetPosition> portfolioExistingPositions;
    private final TargetPositionSnapshotStatus targetPositionSnapshotStatus;
    private final List<BinanceSymbolFilter> filters;
    private final String quoteAsset;
    private final Integer userMaxConcurrentPositions;
    private final CalculationVersions versions;

    private TargetPortfolioRequest(Builder builder) {
        this.calculatedAt = Objects.requireNonNull(builder.calculatedAt, "calculatedAt");
        this.sourceAccountEquityUsd = DecimalSupport.nullable(builder.sourceAccountEquityUsd);
        this.equityObservedAt = builder.equityObservedAt;
        this.equitySource = builder.equitySource == null ? null : builder.equitySource.trim();
        this.maximumEquityAge = builder.maximumEquityAge == null
                ? Duration.ofSeconds(30)
                : builder.maximumEquityAge;
        if (maximumEquityAge.isNegative()) {
            throw new IllegalArgumentException("maximumEquityAge must not be negative");
        }
        this.sourceSnapshotVersion = builder.sourceSnapshotVersion;
        this.sourcePositions = List.copyOf(builder.sourcePositions == null ? List.of() : builder.sourcePositions);
        this.targetAllocatedCapitalUsd = DecimalSupport.nonNegativeOrZero(builder.targetAllocatedCapitalUsd);
        this.targetLeverage = DecimalSupport.nonNegativeOrZero(builder.targetLeverage);
        this.availableMarginUsd = builder.availableMarginUsd == null
                ? this.targetAllocatedCapitalUsd
                : DecimalSupport.nonNegativeOrZero(builder.availableMarginUsd);
        this.usedMarginUsd = DecimalSupport.nonNegativeOrZero(builder.usedMarginUsd);
        this.reservedMarginUsd = DecimalSupport.nonNegativeOrZero(builder.reservedMarginUsd);
        this.existingPositions = List.copyOf(builder.existingPositions == null ? List.of() : builder.existingPositions);
        this.managedExistingPositions = List.copyOf(builder.managedExistingPositions == null
                ? this.existingPositions
                : builder.managedExistingPositions);
        this.portfolioExistingPositions = List.copyOf(builder.portfolioExistingPositions == null
                ? this.managedExistingPositions
                : builder.portfolioExistingPositions);
        this.targetPositionSnapshotStatus = builder.targetPositionSnapshotStatus == null
                ? TargetPositionSnapshotStatus.AUTHORITATIVE
                : builder.targetPositionSnapshotStatus;
        this.filters = List.copyOf(builder.filters == null ? List.of() : builder.filters);
        this.quoteAsset = required(builder.quoteAsset, "quoteAsset").toUpperCase();
        if (builder.userMaxConcurrentPositions != null && builder.userMaxConcurrentPositions <= 0) {
            throw new IllegalArgumentException("userMaxConcurrentPositions must be null or positive");
        }
        this.userMaxConcurrentPositions = builder.userMaxConcurrentPositions;
        this.versions = Objects.requireNonNull(builder.versions, "versions");
    }

    public static Builder builder() {
        return new Builder();
    }

    public Instant calculatedAt() { return calculatedAt; }
    public BigDecimal sourceAccountEquityUsd() { return sourceAccountEquityUsd; }
    public Instant equityObservedAt() { return equityObservedAt; }
    public String equitySource() { return equitySource; }
    public Duration maximumEquityAge() { return maximumEquityAge; }
    public long sourceSnapshotVersion() { return sourceSnapshotVersion; }
    public List<SourcePosition> sourcePositions() { return sourcePositions; }
    public BigDecimal targetAllocatedCapitalUsd() { return targetAllocatedCapitalUsd; }
    public BigDecimal targetLeverage() { return targetLeverage; }
    public BigDecimal availableMarginUsd() { return availableMarginUsd; }
    public BigDecimal usedMarginUsd() { return usedMarginUsd; }
    public BigDecimal reservedMarginUsd() { return reservedMarginUsd; }
    public List<ExistingTargetPosition> existingPositions() { return existingPositions; }
    public List<ExistingTargetPosition> managedExistingPositions() { return managedExistingPositions; }
    public List<ExistingTargetPosition> portfolioExistingPositions() { return portfolioExistingPositions; }
    public TargetPositionSnapshotStatus targetPositionSnapshotStatus() { return targetPositionSnapshotStatus; }
    public List<BinanceSymbolFilter> filters() { return filters; }
    public String quoteAsset() { return quoteAsset; }
    public Integer userMaxConcurrentPositions() { return userMaxConcurrentPositions; }
    public CalculationVersions versions() { return versions; }

    private static String required(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return value.trim();
    }

    public static final class Builder {
        private Instant calculatedAt;
        private BigDecimal sourceAccountEquityUsd;
        private Instant equityObservedAt;
        private String equitySource;
        private Duration maximumEquityAge;
        private long sourceSnapshotVersion;
        private List<SourcePosition> sourcePositions;
        private BigDecimal targetAllocatedCapitalUsd;
        private BigDecimal targetLeverage;
        private BigDecimal availableMarginUsd;
        private BigDecimal usedMarginUsd;
        private BigDecimal reservedMarginUsd;
        private List<ExistingTargetPosition> existingPositions;
        private List<ExistingTargetPosition> managedExistingPositions;
        private List<ExistingTargetPosition> portfolioExistingPositions;
        private TargetPositionSnapshotStatus targetPositionSnapshotStatus;
        private List<BinanceSymbolFilter> filters;
        private String quoteAsset;
        private Integer userMaxConcurrentPositions;
        private CalculationVersions versions;

        private Builder() {}

        public Builder calculatedAt(Instant value) { this.calculatedAt = value; return this; }
        public Builder sourceAccountEquityUsd(BigDecimal value) { this.sourceAccountEquityUsd = value; return this; }
        public Builder equityObservedAt(Instant value) { this.equityObservedAt = value; return this; }
        public Builder equitySource(String value) { this.equitySource = value; return this; }
        public Builder maximumEquityAge(Duration value) { this.maximumEquityAge = value; return this; }
        public Builder sourceSnapshotVersion(long value) { this.sourceSnapshotVersion = value; return this; }
        public Builder sourcePositions(List<SourcePosition> value) { this.sourcePositions = value; return this; }
        public Builder targetAllocatedCapitalUsd(BigDecimal value) { this.targetAllocatedCapitalUsd = value; return this; }
        public Builder targetLeverage(BigDecimal value) { this.targetLeverage = value; return this; }
        public Builder availableMarginUsd(BigDecimal value) { this.availableMarginUsd = value; return this; }
        public Builder usedMarginUsd(BigDecimal value) { this.usedMarginUsd = value; return this; }
        public Builder reservedMarginUsd(BigDecimal value) { this.reservedMarginUsd = value; return this; }
        public Builder existingPositions(List<ExistingTargetPosition> value) { this.existingPositions = value; return this; }
        public Builder managedExistingPositions(List<ExistingTargetPosition> value) { this.managedExistingPositions = value; return this; }
        public Builder portfolioExistingPositions(List<ExistingTargetPosition> value) { this.portfolioExistingPositions = value; return this; }
        public Builder targetPositionSnapshotStatus(TargetPositionSnapshotStatus value) { this.targetPositionSnapshotStatus = value; return this; }
        public Builder filters(List<BinanceSymbolFilter> value) { this.filters = value; return this; }
        public Builder quoteAsset(String value) { this.quoteAsset = value; return this; }
        public Builder userMaxConcurrentPositions(Integer value) { this.userMaxConcurrentPositions = value; return this; }
        public Builder versions(CalculationVersions value) { this.versions = value; return this; }

        public TargetPortfolioRequest build() { return new TargetPortfolioRequest(this); }
    }
}
