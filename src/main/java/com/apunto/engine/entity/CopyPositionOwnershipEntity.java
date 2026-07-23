package com.apunto.engine.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;
import lombok.Data;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

@Entity
@Table(name = "copy_position_ownership", schema = "futuros_operaciones")
@Data
public class CopyPositionOwnershipEntity {

    @Id
    private UUID id;

    @Column(name = "exchange_account_id", nullable = false)
    private UUID exchangeAccountId;

    @Column(name = "user_copy_allocation_id", nullable = false)
    private Long userCopyAllocationId;

    @Column(name = "source_position_cycle_id", nullable = false)
    private UUID sourcePositionCycleId;

    @Column(name = "economic_cycle_id")
    private UUID economicCycleId;

    @Column(name = "symbol", nullable = false, length = 40)
    private String symbol;

    @Column(name = "position_side", nullable = false, length = 16)
    private String positionSide;

    @Column(name = "owned_qty", nullable = false, precision = 38, scale = 18)
    private BigDecimal ownedQty;

    @Column(name = "actual_binance_qty", precision = 38, scale = 18)
    private BigDecimal actualBinanceQty;

    @Column(name = "fixed_leverage", nullable = false, precision = 12, scale = 4)
    private BigDecimal fixedLeverage;

    @Column(name = "fixed_margin_mode", nullable = false, length = 24)
    private String fixedMarginMode;

    @Column(name = "fixed_position_mode", nullable = false, length = 24)
    private String fixedPositionMode;

    @Column(name = "ownership_status", nullable = false, length = 24)
    private String ownershipStatus;

    @Column(name = "reconciliation_required", nullable = false)
    private boolean reconciliationRequired;

    @Column(name = "created_at", nullable = false)
    private OffsetDateTime createdAt;

    @Column(name = "updated_at", nullable = false)
    private OffsetDateTime updatedAt;

    @Column(name = "closed_at")
    private OffsetDateTime closedAt;

    @PrePersist
    void prePersist() {
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        if (id == null) id = UUID.randomUUID();
        if (ownedQty == null) ownedQty = BigDecimal.ZERO;
        if (ownershipStatus == null) ownershipStatus = "OPEN";
        if (createdAt == null) createdAt = now;
        updatedAt = now;
    }

    @PreUpdate
    void preUpdate() {
        updatedAt = OffsetDateTime.now(ZoneOffset.UTC);
    }
}
