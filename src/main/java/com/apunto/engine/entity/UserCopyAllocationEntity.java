package com.apunto.engine.entity;

import com.apunto.engine.entity.converter.UserCopyAllocationStatusConverter;
import com.apunto.engine.shared.enums.CopyMinNotionalMode;
import jakarta.persistence.Column;
import jakarta.persistence.Convert;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.OffsetDateTime;
import java.util.UUID;

@Entity
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(
        name = "user_copy_allocation",
        schema = "futuros_operaciones"
)
public class UserCopyAllocationEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "id_user", nullable = false)
    private UUID idUser;

    @Column(name = "wallet_id", nullable = false, length = 128)
    private String walletId;

    @Builder.Default
    @Column(name = "copy_strategy_code", nullable = false, length = 64)
    private String copyStrategyCode = "MOVEMENT_ALL";

    @Column(name = "copy_strategy_slug", length = 80)
    private String copyStrategySlug;

    @Column(name = "copy_strategy_label", length = 120)
    private String copyStrategyLabel;

    @Column(name = "copy_mode", length = 80)
    private String copyMode;

    @Column(name = "strategy_source_endpoint", length = 180)
    private String strategySourceEndpoint;

    @Column(name = "rank_within_strategy")
    private Integer rankWithinStrategy;

    @Column(name = "global_rank")
    private Integer globalRank;

    @Column(name = "strategy_score", precision = 18, scale = 6)
    private BigDecimal strategyScore;

    @Column(name = "allocation_pct", precision = 9, scale = 6, nullable = false)
    private BigDecimal allocationPct;

    @Column(name = "score")
    private Integer score;

    @Convert(converter = UserCopyAllocationStatusConverter.class)
    @Column(name = "status", nullable = false, length = 40)
    private Status status;

    @Column(name = "updated_at", nullable = false, columnDefinition = "timestamp with time zone")
    private OffsetDateTime updatedAt;

    @Column(name = "ends_at", columnDefinition = "timestamp with time zone")
    private OffsetDateTime endsAt;

    @Column(name = "is_active")
    private boolean isActive;

    @Builder.Default
    @Column(name = "execution_mode", nullable = false, length = 16)
    private String executionMode = "LIVE";

    @Column(name = "status_reason", length = 160)
    private String statusReason;

    @Column(name = "status_updated_at", columnDefinition = "timestamp with time zone")
    private OffsetDateTime statusUpdatedAt;

    @Column(name = "status_cooldown_until", columnDefinition = "timestamp with time zone")
    private OffsetDateTime statusCooldownUntil;

    @Column(name = "leverage_override", precision = 10, scale = 2)
    private BigDecimal leverageOverride;

    @Builder.Default
    @Enumerated(EnumType.STRING)
    @Column(name = "copy_min_notional_mode", nullable = false, length = 32)
    private CopyMinNotionalMode copyMinNotionalMode = CopyMinNotionalMode.INHERIT;

    @Column(name = "copy_min_notional_max_usdt", precision = 18, scale = 8)
    private BigDecimal copyMinNotionalMaxUsdt;

    @Column(name = "copy_min_notional_min_score")
    private Integer copyMinNotionalMinScore;

    @Column(name = "copy_min_notional_min_history_days")
    private Integer copyMinNotionalMinHistoryDays;

    @Column(name = "copy_min_notional_min_operations")
    private Integer copyMinNotionalMinOperations;

    public enum Status {
        ACTIVE,
        EXIT_ONLY,
        PAUSED,
        PAUSED_BY_NEGATIVE_PNL,
        PAUSED_BY_STALE_METRIC,
        PAUSED_BY_RISK,
        DISABLED_MANUAL,
        CLOSED
    }

    @PrePersist
    void prePersist() {
        final OffsetDateTime now = OffsetDateTime.now();

        if (status == null) status = Status.ACTIVE;
        if (updatedAt == null) updatedAt = now;
        if (copyMinNotionalMode == null) copyMinNotionalMode = CopyMinNotionalMode.INHERIT;
        executionMode = normalizeExecutionMode(executionMode);
        statusReason = normalize(statusReason);
        if (statusUpdatedAt == null) statusUpdatedAt = now;

        walletId = normalize(walletId);
        copyStrategyCode = normalizeStrategyCode(copyStrategyCode);
        copyStrategySlug = normalize(copyStrategySlug);
        copyStrategyLabel = normalize(copyStrategyLabel);
        copyMode = normalize(copyMode);
        strategySourceEndpoint = normalize(strategySourceEndpoint);

        if (strategyScore != null) {
            strategyScore = strategyScore.setScale(6, RoundingMode.HALF_UP);
        }

        if (allocationPct != null) {
            allocationPct = allocationPct.setScale(6, RoundingMode.HALF_UP);
        }
        if (leverageOverride != null) {
            leverageOverride = leverageOverride.setScale(2, RoundingMode.HALF_UP);
        }

        applyEndsAtRule(now);
    }

    @PreUpdate
    void preUpdate() {
        final OffsetDateTime now = OffsetDateTime.now();
        updatedAt = now;
        if (copyMinNotionalMode == null) copyMinNotionalMode = CopyMinNotionalMode.INHERIT;
        executionMode = normalizeExecutionMode(executionMode);
        statusReason = normalize(statusReason);
        if (statusUpdatedAt == null) statusUpdatedAt = now;

        walletId = normalize(walletId);
        copyStrategyCode = normalizeStrategyCode(copyStrategyCode);
        copyStrategySlug = normalize(copyStrategySlug);
        copyStrategyLabel = normalize(copyStrategyLabel);
        copyMode = normalize(copyMode);
        strategySourceEndpoint = normalize(strategySourceEndpoint);

        if (strategyScore != null) {
            strategyScore = strategyScore.setScale(6, RoundingMode.HALF_UP);
        }

        if (allocationPct != null) {
            allocationPct = allocationPct.setScale(6, RoundingMode.HALF_UP);
        }
        if (leverageOverride != null) {
            leverageOverride = leverageOverride.setScale(2, RoundingMode.HALF_UP);
        }

        applyEndsAtRule(now);
    }

    private void applyEndsAtRule(OffsetDateTime now) {
        if (status == Status.CLOSED) {
            if (endsAt == null) endsAt = now;
        } else {
            endsAt = null;
        }
    }

    private static String normalize(String s) {
        if (s == null) return null;
        String t = s.trim();
        return t.isEmpty() ? null : t;
    }

    private static String normalizeStrategyCode(String s) {
        String t = normalize(s);
        if (t == null) return "MOVEMENT_ALL";
        return t.toUpperCase(java.util.Locale.ROOT).replace('-', '_');
    }

    public boolean allowsNewEntries(OffsetDateTime now) {
        if (!isActive || endsAt != null || status != Status.ACTIVE) {
            return false;
        }
        if (statusCooldownUntil == null) {
            return true;
        }
        final OffsetDateTime effectiveNow = now == null ? OffsetDateTime.now() : now;
        return !statusCooldownUntil.isAfter(effectiveNow);
    }

    public boolean allowsExitOnly() {
        return isActive && endsAt == null && (status == Status.ACTIVE || status == Status.EXIT_ONLY
                || status == Status.PAUSED_BY_NEGATIVE_PNL || status == Status.PAUSED_BY_STALE_METRIC
                || status == Status.PAUSED_BY_RISK);
    }

    public boolean isShadowMode() {
        return "SHADOW".equalsIgnoreCase(executionMode);
    }

    public static String normalizeExecutionMode(String value) {
        String t = normalize(value);
        if (t == null) return "LIVE";
        String mode = t.toUpperCase(java.util.Locale.ROOT).replace('-', '_');
        return "SHADOW".equals(mode) ? "SHADOW" : "LIVE";
    }
}
