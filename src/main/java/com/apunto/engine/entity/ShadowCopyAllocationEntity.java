package com.apunto.engine.entity;

import com.apunto.engine.shared.metric.MetricStrategyIdentity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
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
import java.util.Locale;
import java.util.UUID;

@Entity
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "shadow_copy_allocation", schema = "futuros_operaciones")
public class ShadowCopyAllocationEntity {

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

    @Builder.Default
    @Column(name = "scope_type", nullable = false, length = 32)
    private String scopeType = "ALL";

    @Builder.Default
    @Column(name = "scope_value", nullable = false, length = 160)
    private String scopeValue = "ALL";

    @Column(name = "strategy_key", nullable = false, length = 420)
    private String strategyKey;

    @Column(name = "metric_generation_id", length = 80)
    private String metricGenerationId;

    @Column(name = "wallet_profile_id")
    private Long walletProfileId;

    @Column(name = "shadow_validation_id")
    private Long shadowValidationId;

    @Builder.Default
    @Column(name = "shadow_version", nullable = false)
    private Integer shadowVersion = 1;

    @Column(name = "linked_live_allocation_id")
    private Long linkedLiveAllocationId;

    @Column(name = "promoted_to_live_at", columnDefinition = "timestamp with time zone")
    private OffsetDateTime promotedToLiveAt;

    @Column(name = "source_ranking_version", length = 80)
    private String sourceRankingVersion;

    @Builder.Default
    @Column(name = "status", nullable = false, length = 40)
    private String status = "SHADOW_ACTIVE";

    @Builder.Default
    @Column(name = "allocation_pct", nullable = false, precision = 9, scale = 6)
    private BigDecimal allocationPct = BigDecimal.ZERO;

    @Column(name = "target_live_allocation_pct", precision = 9, scale = 6)
    private BigDecimal targetLiveAllocationPct;

    @Column(name = "rank_within_strategy")
    private Integer rankWithinStrategy;

    @Column(name = "global_rank")
    private Integer globalRank;

    @Column(name = "strategy_score", precision = 18, scale = 6)
    private BigDecimal strategyScore;

    @Column(name = "decision_score")
    private Integer decisionScore;

    @Column(name = "copy_guard_status", length = 40)
    private String copyGuardStatus;

    @Column(name = "copy_guard_action", length = 40)
    private String copyGuardAction;

    @Column(name = "copy_guard_reasons", columnDefinition = "text")
    private String copyGuardReasons;

    @Column(name = "last_validation_reason", length = 300)
    private String lastValidationReason;

    @Column(name = "wallet_last_activity_at", columnDefinition = "timestamp with time zone")
    private OffsetDateTime walletLastActivityAt;

    @Column(name = "wallet_last_opened_at", columnDefinition = "timestamp with time zone")
    private OffsetDateTime walletLastOpenedAt;

    @Column(name = "wallet_last_closed_at", columnDefinition = "timestamp with time zone")
    private OffsetDateTime walletLastClosedAt;

    @Column(name = "strategy_last_activity_at", columnDefinition = "timestamp with time zone")
    private OffsetDateTime strategyLastActivityAt;

    @Column(name = "strategy_last_opened_at", columnDefinition = "timestamp with time zone")
    private OffsetDateTime strategyLastOpenedAt;

    @Column(name = "strategy_last_closed_at", columnDefinition = "timestamp with time zone")
    private OffsetDateTime strategyLastClosedAt;

    @Column(name = "last_seen_at", nullable = false, columnDefinition = "timestamp with time zone")
    private OffsetDateTime lastSeenAt;

    @Column(name = "created_at", nullable = false, columnDefinition = "timestamp with time zone")
    private OffsetDateTime createdAt;

    @Column(name = "updated_at", nullable = false, columnDefinition = "timestamp with time zone")
    private OffsetDateTime updatedAt;

    @Column(name = "ends_at", columnDefinition = "timestamp with time zone")
    private OffsetDateTime endsAt;

    @Builder.Default
    @Column(name = "is_active", nullable = false)
    private boolean active = true;

    @PrePersist
    void prePersist() {
        OffsetDateTime now = OffsetDateTime.now();
        if (createdAt == null) createdAt = now;
        if (updatedAt == null) updatedAt = now;
        if (lastSeenAt == null) lastSeenAt = now;
        normalize();
    }

    @PreUpdate
    void preUpdate() {
        updatedAt = OffsetDateTime.now();
        normalize();
    }

    private void normalize() {
        walletId = normalizeLower(walletId);
        copyStrategyCode = normalizeStrategy(copyStrategyCode);
        scopeType = normalizeScopeType(scopeType, copyStrategyCode);
        scopeValue = normalizeScopeValue(scopeValue, copyStrategyCode);
        strategyKey = normalizeStrategyKey(strategyKey, walletId, copyStrategyCode, scopeType, scopeValue);
        status = normalizeStatus(status);
        copyGuardStatus = normalizeNullable(copyGuardStatus);
        copyGuardAction = normalizeNullable(copyGuardAction);
        if (shadowVersion == null || shadowVersion < 1) shadowVersion = 1;
        if (allocationPct == null) allocationPct = BigDecimal.ZERO;
        allocationPct = allocationPct.setScale(6, RoundingMode.HALF_UP);
        if (targetLiveAllocationPct != null) targetLiveAllocationPct = targetLiveAllocationPct.setScale(6, RoundingMode.HALF_UP);
        if (strategyScore != null) strategyScore = strategyScore.setScale(6, RoundingMode.HALF_UP);
    }

    public boolean isLinkedToLive() {
        return linkedLiveAllocationId != null;
    }

    private static String normalizeLower(String raw) {
        String value = normalize(raw);
        return value == null ? null : value.toLowerCase(Locale.ROOT);
    }

    private static String normalizeStrategy(String raw) {
        String value = normalize(raw);
        return value == null ? "MOVEMENT_ALL" : value.toUpperCase(Locale.ROOT).replace('-', '_');
    }

    private static String normalizeScopeType(String raw, String strategy) {
        return MetricStrategyIdentity.scopeType(raw, strategy);
    }

    private static String normalizeScopeValue(String raw, String strategy) {
        return MetricStrategyIdentity.scopeValue(raw, strategy);
    }

    private static String normalizeStatus(String raw) {
        String value = normalize(raw);
        return value == null ? "SHADOW_ACTIVE" : value.toUpperCase(Locale.ROOT).replace('-', '_');
    }

    private static String normalizeNullable(String raw) {
        String value = normalize(raw);
        return value == null ? null : value.toUpperCase(Locale.ROOT).replace('-', '_');
    }

    private static String normalizeStrategyKey(String current, String wallet, String strategy, String scopeType, String scopeValue) {
        return MetricStrategyIdentity.canonicalKey(wallet, strategy, scopeType, scopeValue);
    }

    private static String normalize(String raw) {
        if (raw == null) return null;
        String value = raw.trim();
        return value.isEmpty() ? null : value;
    }
}
