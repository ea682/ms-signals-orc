package com.apunto.engine.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;
import lombok.Data;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

@Entity
@Table(name = "copy_flip_saga", schema = "futuros_operaciones")
@Data
public class CopyFlipSagaEntity {

    @Id
    private UUID id;
    @Column(name = "user_copy_allocation_id", nullable = false)
    private Long userCopyAllocationId;
    @Column(name = "exchange_account_id", nullable = false)
    private UUID exchangeAccountId;
    @Column(name = "old_source_position_cycle_id")
    private UUID oldSourcePositionCycleId;
    @Column(name = "new_source_position_cycle_id", nullable = false)
    private UUID newSourcePositionCycleId;
    @Column(name = "symbol", nullable = false, length = 40)
    private String symbol;
    @Column(name = "old_side", length = 16)
    private String oldSide;
    @Column(name = "new_side", nullable = false, length = 16)
    private String newSide;
    @Column(name = "saga_status", nullable = false, length = 40)
    private String sagaStatus;
    @Column(name = "old_leg_result", length = 80)
    private String oldLegResult;
    @Column(name = "new_leg_result", length = 80)
    private String newLegResult;
    @Column(name = "reason_code", length = 120)
    private String reasonCode;
    @Column(name = "old_close_client_order_id", length = 36)
    private String oldCloseClientOrderId;
    @Column(name = "new_open_client_order_id", length = 36)
    private String newOpenClientOrderId;
    @Column(name = "created_at", nullable = false)
    private OffsetDateTime createdAt;
    @Column(name = "updated_at", nullable = false)
    private OffsetDateTime updatedAt;
    @Column(name = "completed_at")
    private OffsetDateTime completedAt;

    @PrePersist
    void prePersist() {
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        if (id == null) id = UUID.randomUUID();
        if (createdAt == null) createdAt = now;
        updatedAt = now;
    }

    @PreUpdate
    void preUpdate() {
        updatedAt = OffsetDateTime.now(ZoneOffset.UTC);
    }
}
