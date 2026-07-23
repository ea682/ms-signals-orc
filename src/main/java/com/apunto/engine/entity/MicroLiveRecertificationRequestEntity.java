package com.apunto.engine.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.OffsetDateTime;
import java.util.UUID;

@Entity
@Table(name = "micro_live_recertification_request", schema = "futuros_operaciones")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MicroLiveRecertificationRequestEntity {

    @Id
    private UUID id;

    @Column(name = "certification_id", nullable = false)
    private UUID certificationId;

    @Column(name = "wallet_id", nullable = false, length = 128)
    private String walletId;

    @Column(name = "strategy_code", nullable = false, length = 100)
    private String strategyCode;

    @Column(name = "strategy_version", nullable = false, length = 100)
    private String strategyVersion;

    @Column(name = "user_id", nullable = false)
    private UUID userId;

    @Column(name = "execution_account_id", nullable = false)
    private UUID executionAccountId;

    @Column(name = "requested_at", nullable = false)
    private OffsetDateTime requestedAt;

    @Column(nullable = false)
    private int priority;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 32)
    private Status status;

    @Column(nullable = false)
    private int attempts;

    @Column(name = "reason_code", nullable = false, length = 160)
    private String reasonCode;

    @Column(name = "idempotency_key", nullable = false, length = 320)
    private String idempotencyKey;

    @Column(name = "user_copy_allocation_id")
    private Long userCopyAllocationId;

    @Column(name = "claimed_at")
    private OffsetDateTime claimedAt;

    @Column(name = "next_attempt_at", nullable = false)
    private OffsetDateTime nextAttemptAt;

    @Column(name = "updated_at", nullable = false)
    private OffsetDateTime updatedAt;

    @Column(name = "completed_at")
    private OffsetDateTime completedAt;

    @PrePersist
    void beforeInsert() {
        OffsetDateTime now = OffsetDateTime.now();
        if (id == null) id = UUID.randomUUID();
        if (requestedAt == null) requestedAt = now;
        if (nextAttemptAt == null) nextAttemptAt = now;
        if (updatedAt == null) updatedAt = now;
        if (status == null) status = Status.PENDING_CAPACITY;
        if (priority <= 0) priority = 100;
        if (reasonCode == null || reasonCode.isBlank()) {
            reasonCode = "MICRO_LIVE_RECERTIFICATION_PENDING_CAPACITY";
        }
    }

    @PreUpdate
    void beforeUpdate() {
        updatedAt = OffsetDateTime.now();
    }

    public enum Status {
        PENDING_CAPACITY,
        CLAIMED,
        ADMITTED,
        CANCELLED,
        INELIGIBLE,
        FAILED
    }
}
