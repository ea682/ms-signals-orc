package com.apunto.engine.entity;

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
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

@Entity
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "user_wallet_copy_plan", schema = "futuros_operaciones")
public class UserWalletCopyPlanEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "id_user", nullable = false)
    private UUID idUser;

    @Column(name = "wallet_lc", nullable = false)
    private String walletLc;

    @Column(name = "allocation_pct", nullable = false, precision = 9, scale = 6)
    private BigDecimal allocationPct;

    @Column(name = "score")
    private Integer score;

    @Builder.Default
    @Column(name = "status", nullable = false)
    private String status = "ACTIVE";

    @Builder.Default
    @Column(name = "is_active", nullable = false)
    private boolean active = true;

    @Builder.Default
    @Column(name = "metric_version", nullable = false)
    private Integer metricVersion = 1;

    @Column(name = "max_wallet")
    private Integer maxWallet;

    @Column(name = "user_capital_usd", precision = 18, scale = 8)
    private BigDecimal userCapitalUsd;

    @Column(name = "allocated_capital_usd", precision = 18, scale = 8)
    private BigDecimal allocatedCapitalUsd;

    @Builder.Default
    @Column(name = "copy_min_notional_mode", nullable = false)
    private String copyMinNotionalMode = "INHERIT";

    @Column(name = "copy_min_notional_max_usdt", precision = 18, scale = 8)
    private BigDecimal copyMinNotionalMaxUsdt;

    @Column(name = "copy_min_notional_min_score")
    private Integer copyMinNotionalMinScore;

    @Column(name = "copy_min_notional_min_history_days")
    private Integer copyMinNotionalMinHistoryDays;

    @Column(name = "copy_min_notional_min_operations")
    private Integer copyMinNotionalMinOperations;

    @Builder.Default
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "reason", nullable = false, columnDefinition = "jsonb")
    private Map<String, Object> reason = new LinkedHashMap<>();

    @Builder.Default
    @Column(name = "synced_to_runtime", nullable = false)
    private boolean syncedToRuntime = false;

    @Column(name = "runtime_synced_at", columnDefinition = "timestamp with time zone")
    private OffsetDateTime runtimeSyncedAt;

    @Column(name = "created_at", nullable = false, columnDefinition = "timestamp with time zone")
    private OffsetDateTime createdAt;

    @Column(name = "updated_at", nullable = false, columnDefinition = "timestamp with time zone")
    private OffsetDateTime updatedAt;

    @PrePersist
    void prePersist() {
        OffsetDateTime now = OffsetDateTime.now();
        if (createdAt == null) createdAt = now;
        if (updatedAt == null) updatedAt = now;
        normalize();
    }

    @PreUpdate
    void preUpdate() {
        updatedAt = OffsetDateTime.now();
        normalize();
    }

    private void normalize() {
        walletLc = walletLc == null ? null : walletLc.trim().toLowerCase(Locale.ROOT);
        if (allocationPct == null) allocationPct = BigDecimal.ZERO;
        allocationPct = allocationPct.setScale(6, RoundingMode.HALF_UP);
        if (status == null || status.isBlank()) status = "ACTIVE";
        status = status.trim().toUpperCase(Locale.ROOT);
        if (metricVersion == null || metricVersion < 1) metricVersion = 1;
        if (copyMinNotionalMode == null || copyMinNotionalMode.isBlank()) copyMinNotionalMode = "INHERIT";
        copyMinNotionalMode = copyMinNotionalMode.trim().toUpperCase(Locale.ROOT);
        if (reason == null) reason = new LinkedHashMap<>();
    }
}
