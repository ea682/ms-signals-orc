package com.apunto.engine.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
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
import java.util.UUID;

@Entity
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "shadow_position_state", schema = "futuros_operaciones")
public class ShadowPositionStateEntity {

    @Id
    @Column(name = "id", nullable = false, columnDefinition = "uuid")
    private UUID id;

    @Column(name = "shadow_allocation_id", nullable = false)
    private Long shadowAllocationId;

    @Column(name = "linked_live_allocation_id")
    private Long linkedLiveAllocationId;

    @Column(name = "wallet_profile_id")
    private Long walletProfileId;

    @Column(name = "shadow_validation_id")
    private Long shadowValidationId;

    @Column(name = "id_user", nullable = false, length = 80)
    private String idUser;

    @Column(name = "wallet_id", nullable = false, length = 128)
    private String walletId;

    @Column(name = "copy_strategy_code", nullable = false, length = 64)
    private String copyStrategyCode;

    @Column(name = "scope_type", nullable = false, length = 32)
    private String scopeType;

    @Column(name = "scope_value", nullable = false, length = 160)
    private String scopeValue;

    @Column(name = "strategy_key", nullable = false, length = 420)
    private String strategyKey;

    @Column(name = "parsymbol", nullable = false, length = 40)
    private String parsymbol;

    @Column(name = "position_side", nullable = false, length = 20)
    private String positionSide;

    @Column(name = "qty", nullable = false, precision = 38, scale = 12)
    private BigDecimal qty;

    @Column(name = "entry_price", precision = 38, scale = 12)
    private BigDecimal entryPrice;

    @Column(name = "mark_price", precision = 38, scale = 12)
    private BigDecimal markPrice;

    @Column(name = "notional_usd", precision = 38, scale = 12)
    private BigDecimal notionalUsd;

    @Column(name = "realized_pnl_usd", nullable = false, precision = 38, scale = 12)
    private BigDecimal realizedPnlUsd;

    @Column(name = "unrealized_pnl_usd", nullable = false, precision = 38, scale = 12)
    private BigDecimal unrealizedPnlUsd;

    @Column(name = "fees_usd", nullable = false, precision = 38, scale = 12)
    private BigDecimal feesUsd;

    @Column(name = "slippage_usd", nullable = false, precision = 38, scale = 12)
    private BigDecimal slippageUsd;

    @Column(name = "status", nullable = false, length = 32)
    private String status;

    @Column(name = "last_source_event_id", length = 120)
    private String lastSourceEventId;

    @Column(name = "last_accepted_event_at", columnDefinition = "timestamp with time zone")
    private OffsetDateTime lastAcceptedEventAt;

    @Column(name = "opened_at", columnDefinition = "timestamp with time zone")
    private OffsetDateTime openedAt;

    @Column(name = "closed_at", columnDefinition = "timestamp with time zone")
    private OffsetDateTime closedAt;

    @Column(name = "updated_at", nullable = false, columnDefinition = "timestamp with time zone")
    private OffsetDateTime updatedAt;

    @PrePersist
    void prePersist() {
        if (id == null) id = UUID.randomUUID();
        if (updatedAt == null) updatedAt = OffsetDateTime.now();
        normalize();
    }

    @PreUpdate
    void preUpdate() {
        updatedAt = OffsetDateTime.now();
        normalize();
    }

    private void normalize() {
        if (copyStrategyCode == null || copyStrategyCode.isBlank()) copyStrategyCode = "MOVEMENT_ALL";
        if (scopeType == null || scopeType.isBlank()) scopeType = "strategy";
        if (scopeValue == null || scopeValue.isBlank()) scopeValue = "default";
        if (positionSide == null || positionSide.isBlank()) positionSide = "BOTH";
        if (status == null || status.isBlank()) status = "OPEN";
        if (qty == null) qty = BigDecimal.ZERO;
        if (realizedPnlUsd == null) realizedPnlUsd = BigDecimal.ZERO;
        if (unrealizedPnlUsd == null) unrealizedPnlUsd = BigDecimal.ZERO;
        if (feesUsd == null) feesUsd = BigDecimal.ZERO;
        if (slippageUsd == null) slippageUsd = BigDecimal.ZERO;
    }
}
