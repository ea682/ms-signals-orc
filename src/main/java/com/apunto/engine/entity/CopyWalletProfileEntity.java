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

@Entity
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "copy_wallet_profile", schema = "futuros_operaciones")
public class CopyWalletProfileEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "wallet_id", nullable = false, length = 128)
    private String walletId;

    @Column(name = "copy_profile_code", nullable = false, length = 64)
    private String copyProfileCode;

    @Column(name = "copy_profile_category", nullable = false, length = 40)
    private String copyProfileCategory;

    @Builder.Default
    @Column(name = "scope_type", nullable = false, length = 32)
    private String scopeType = "ALL";

    @Builder.Default
    @Column(name = "scope_value", nullable = false, length = 160)
    private String scopeValue = "ALL";

    @Column(name = "profile_config_hash", length = 80)
    private String profileConfigHash;

    @Column(name = "profile_key", nullable = false, length = 420)
    private String profileKey;

    @Builder.Default
    @Column(name = "status", nullable = false, length = 40)
    private String status = "SHADOW_TESTING";

    @Column(name = "historical_score", precision = 18, scale = 6)
    private BigDecimal historicalScore;

    @Column(name = "shadow_score", precision = 18, scale = 6)
    private BigDecimal shadowScore;

    @Column(name = "validated_ranking_score", precision = 18, scale = 6)
    private BigDecimal validatedRankingScore;

    @Column(name = "copy_guard_status", length = 40)
    private String copyGuardStatus;

    @Column(name = "copy_guard_action", length = 40)
    private String copyGuardAction;

    @Column(name = "last_seen_at", nullable = false, columnDefinition = "timestamp with time zone")
    private OffsetDateTime lastSeenAt;

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

    @Column(name = "last_validation_reason", length = 300)
    private String lastValidationReason;

    @Column(name = "last_validation_reason_code", length = 120)
    private String lastValidationReasonCode;

    @Column(name = "cooldown_until", columnDefinition = "timestamp with time zone")
    private OffsetDateTime cooldownUntil;

    @Column(name = "created_at", nullable = false, columnDefinition = "timestamp with time zone")
    private OffsetDateTime createdAt;

    @Column(name = "updated_at", nullable = false, columnDefinition = "timestamp with time zone")
    private OffsetDateTime updatedAt;

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
        copyProfileCode = normalizeUpper(copyProfileCode, "MOVEMENT_ALL");
        copyProfileCategory = normalizeUpper(copyProfileCategory, "CORE_COPY_PROFILE");
        scopeType = MetricStrategyIdentity.scopeType(scopeType, copyProfileCode);
        scopeValue = MetricStrategyIdentity.scopeValue(scopeValue, copyProfileCode);
        status = normalizeUpper(status, "SHADOW_TESTING");
        profileKey = MetricStrategyIdentity.canonicalKey(walletId, copyProfileCode, scopeType, scopeValue);
        copyGuardStatus = normalizeNullable(copyGuardStatus);
        copyGuardAction = normalizeNullable(copyGuardAction);
        lastValidationReasonCode = normalizeNullable(lastValidationReasonCode);
        if (historicalScore != null) historicalScore = historicalScore.setScale(6, RoundingMode.HALF_UP);
        if (shadowScore != null) shadowScore = shadowScore.setScale(6, RoundingMode.HALF_UP);
        if (validatedRankingScore != null) validatedRankingScore = validatedRankingScore.setScale(6, RoundingMode.HALF_UP);
    }

    private static String normalizeLower(String value) {
        if (value == null) return null;
        String clean = value.trim().toLowerCase(Locale.ROOT);
        return clean.isEmpty() ? null : clean;
    }

    private static String normalizeLowerOr(String value, String fallback) {
        String clean = normalizeLower(value);
        return clean == null ? fallback : clean;
    }

    private static String normalizeUpper(String value, String fallback) {
        if (value == null || value.isBlank()) return fallback;
        return value.trim().toUpperCase(Locale.ROOT).replace('-', '_');
    }

    private static String normalizeTextOr(String value, String fallback) {
        if (value == null || value.isBlank()) return fallback;
        return value.trim();
    }

    private static String normalizeNullable(String value) {
        if (value == null || value.isBlank()) return null;
        return value.trim().toUpperCase(Locale.ROOT).replace('-', '_');
    }
}
