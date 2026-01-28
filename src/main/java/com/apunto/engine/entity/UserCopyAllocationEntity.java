package com.apunto.engine.entity;

import com.apunto.engine.entity.converter.UserCopyAllocationStatusConverter;
import jakarta.persistence.*;
import lombok.*;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.OffsetDateTime;

@Entity
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(
        name = "user_copy_allocation",
        schema = "futuros_operaciones",
        uniqueConstraints = @UniqueConstraint(
                name = "uq_user_copy_allocation_max_wallet_wallet",
                columnNames = {"max_wallet", "wallet_id"}
        )
)
public class UserCopyAllocationEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "max_wallet", nullable = false)
    private Integer maxWallet;

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
