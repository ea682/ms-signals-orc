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
import jakarta.persistence.UniqueConstraint;
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
        schema = "futuros_operaciones",
        uniqueConstraints = @UniqueConstraint(
                name = "uq_user_copy_allocation_user_wallet",
                columnNames = {"id_user", "wallet_id"}
        )
)
public class UserCopyAllocationEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "id_user", nullable = false)
    private UUID idUser;

    @Column(name = "wallet_id", nullable = false, length = 128)
    private String walletId;

    @Column(name = "allocation_pct", precision = 9, scale = 6, nullable = false)
    private BigDecimal allocationPct;

    @Column(name = "score")
    private Integer score;

    @Convert(converter = UserCopyAllocationStatusConverter.class)
    @Column(name = "status", nullable = false, length = 16)
    private Status status;

    @Column(name = "updated_at", nullable = false, columnDefinition = "timestamp with time zone")
    private OffsetDateTime updatedAt;

    @Column(name = "ends_at", columnDefinition = "timestamp with time zone")
    private OffsetDateTime endsAt;

    @Column(name = "is_active")
    private boolean isActive;

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
        PAUSED,
        CLOSED
    }

    @PrePersist
    void prePersist() {
        final OffsetDateTime now = OffsetDateTime.now();

        if (status == null) status = Status.ACTIVE;
        if (updatedAt == null) updatedAt = now;
        if (copyMinNotionalMode == null) copyMinNotionalMode = CopyMinNotionalMode.INHERIT;

        walletId = normalize(walletId);

        if (allocationPct != null) {
            allocationPct = allocationPct.setScale(6, RoundingMode.HALF_UP);
        }

        applyEndsAtRule(now);
    }

    @PreUpdate
    void preUpdate() {
        final OffsetDateTime now = OffsetDateTime.now();
        updatedAt = now;
        if (copyMinNotionalMode == null) copyMinNotionalMode = CopyMinNotionalMode.INHERIT;

        walletId = normalize(walletId);

        if (allocationPct != null) {
            allocationPct = allocationPct.setScale(6, RoundingMode.HALF_UP);
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
}
