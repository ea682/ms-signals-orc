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

import java.math.BigDecimal;
import java.time.OffsetDateTime;

@Entity
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "shadow_wallet_profile_validation", schema = "futuros_operaciones")
public class ShadowWalletProfileValidationEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "wallet_profile_id", nullable = false)
    private Long walletProfileId;

    @Builder.Default
    @Column(name = "status", nullable = false, length = 40)
    private String status = "SHADOW_TESTING";

    @Column(name = "started_at", nullable = false, columnDefinition = "timestamp with time zone")
    private OffsetDateTime startedAt;

    @Column(name = "validated_at", columnDefinition = "timestamp with time zone")
    private OffsetDateTime validatedAt;

    @Column(name = "rejected_at", columnDefinition = "timestamp with time zone")
    private OffsetDateTime rejectedAt;

    @Column(name = "closed_positions", nullable = false)
    private Long closedPositions;

    @Column(name = "open_positions", nullable = false)
    private Long openPositions;

    @Column(name = "net_pnl_usd", nullable = false, precision = 38, scale = 12)
    private BigDecimal netPnlUsd;

    @Column(name = "gross_pnl_usd", nullable = false, precision = 38, scale = 12)
    private BigDecimal grossPnlUsd;

    @Column(name = "fees_usd", nullable = false, precision = 38, scale = 12)
    private BigDecimal feesUsd;

    @Column(name = "slippage_usd", nullable = false, precision = 38, scale = 12)
    private BigDecimal slippageUsd;

    @Column(name = "win_rate", precision = 18, scale = 6)
    private BigDecimal winRate;

    @Column(name = "max_drawdown", precision = 38, scale = 12)
    private BigDecimal maxDrawdown;

    @Column(name = "avg_slippage_bps", precision = 18, scale = 6)
    private BigDecimal avgSlippageBps;

    @Column(name = "simulated_events", nullable = false)
    private Long simulatedEvents;

    @Column(name = "recorded_events", nullable = false)
    private Long recordedEvents;

    @Column(name = "skipped_events", nullable = false)
    private Long skippedEvents;

    @Column(name = "duplicate_events", nullable = false)
    private Long duplicateEvents;

    @Column(name = "error_events", nullable = false)
    private Long errorEvents;

    @Column(name = "last_validation_reason", length = 300)
    private String lastValidationReason;

    @Column(name = "last_validation_reason_code", length = 120)
    private String lastValidationReasonCode;

    @Column(name = "created_at", nullable = false, columnDefinition = "timestamp with time zone")
    private OffsetDateTime createdAt;

    @Column(name = "updated_at", nullable = false, columnDefinition = "timestamp with time zone")
    private OffsetDateTime updatedAt;

    @PrePersist
    void prePersist() {
        OffsetDateTime now = OffsetDateTime.now();
        if (startedAt == null) startedAt = now;
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
        if (status == null || status.isBlank()) status = "SHADOW_TESTING";
        if (closedPositions == null) closedPositions = 0L;
        if (openPositions == null) openPositions = 0L;
        if (netPnlUsd == null) netPnlUsd = BigDecimal.ZERO;
        if (grossPnlUsd == null) grossPnlUsd = BigDecimal.ZERO;
        if (feesUsd == null) feesUsd = BigDecimal.ZERO;
        if (slippageUsd == null) slippageUsd = BigDecimal.ZERO;
        if (simulatedEvents == null) simulatedEvents = 0L;
        if (recordedEvents == null) recordedEvents = 0L;
        if (skippedEvents == null) skippedEvents = 0L;
        if (duplicateEvents == null) duplicateEvents = 0L;
        if (errorEvents == null) errorEvents = 0L;
    }
}
