package com.apunto.engine.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
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
@Table(name = "shadow_copy_operation_event", schema = "futuros_operaciones")
public class ShadowCopyOperationEventEntity {

    @Id
    @Column(name = "id_event", nullable = false, columnDefinition = "uuid")
    private UUID idEvent;

    @Column(name = "shadow_operation_id", columnDefinition = "uuid")
    private UUID shadowOperationId;

    @Column(name = "shadow_position_id", columnDefinition = "uuid")
    private UUID shadowPositionId;

    @Column(name = "shadow_allocation_id", nullable = false)
    private Long shadowAllocationId;

    @Column(name = "linked_live_allocation_id")
    private Long linkedLiveAllocationId;

    @Column(name = "wallet_profile_id")
    private Long walletProfileId;

    @Column(name = "shadow_validation_id")
    private Long shadowValidationId;

    @Column(name = "id_order_origin", nullable = false, length = 120)
    private String idOrderOrigin;

    @Column(name = "id_user", nullable = false, length = 80)
    private String idUser;

    @Column(name = "id_wallet_origin", nullable = false, length = 128)
    private String idWalletOrigin;

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

    @Column(name = "type_operation", length = 20)
    private String typeOperation;

    @Column(name = "event_type", nullable = false, length = 32)
    private String eventType;

    @Column(name = "position_side", length = 20)
    private String positionSide;

    @Column(name = "qty_requested", precision = 38, scale = 12)
    private BigDecimal qtyRequested;

    @Column(name = "qty_executed", precision = 38, scale = 12)
    private BigDecimal qtyExecuted;

    @Column(name = "price", precision = 38, scale = 12)
    private BigDecimal price;

    @Column(name = "notional_usd", precision = 38, scale = 12)
    private BigDecimal notionalUsd;

    @Column(name = "previous_qty", precision = 38, scale = 12)
    private BigDecimal previousQty;

    @Column(name = "resulting_qty", precision = 38, scale = 12)
    private BigDecimal resultingQty;

    @Column(name = "realized_pnl_usd", precision = 38, scale = 12)
    private BigDecimal realizedPnlUsd;

    @Column(name = "fee_usd", nullable = false, precision = 38, scale = 12)
    private BigDecimal feeUsd;

    @Column(name = "slippage_bps", precision = 18, scale = 6)
    private BigDecimal slippageBps;

    @Column(name = "slippage_usd", nullable = false, precision = 38, scale = 12)
    private BigDecimal slippageUsd;

    @Column(name = "decision", length = 40)
    private String decision;

    @Column(name = "decision_reason", length = 180)
    private String decisionReason;

    @Column(name = "source_movement_key", length = 160)
    private String sourceMovementKey;

    @Column(name = "delay_ms")
    private Long delayMs;

    @Column(name = "trace_id", length = 120)
    private String traceId;

    @Column(name = "source", length = 80)
    private String source;

    @Column(name = "reason_code", length = 80)
    private String reasonCode;

    @Column(name = "event_time", nullable = false, columnDefinition = "timestamp with time zone")
    private OffsetDateTime eventTime;

    @Column(name = "date_creation", nullable = false, columnDefinition = "timestamp with time zone")
    private OffsetDateTime dateCreation;

    @PrePersist
    void prePersist() {
        OffsetDateTime now = OffsetDateTime.now();
        if (idEvent == null) idEvent = UUID.randomUUID();
        if (eventTime == null) eventTime = now;
        if (dateCreation == null) dateCreation = now;
        if (feeUsd == null) feeUsd = BigDecimal.ZERO;
        if (slippageUsd == null) slippageUsd = BigDecimal.ZERO;
        if (copyStrategyCode == null || copyStrategyCode.isBlank()) copyStrategyCode = "MOVEMENT_ALL";
        if (scopeType == null || scopeType.isBlank()) scopeType = "strategy";
        if (scopeValue == null || scopeValue.isBlank()) scopeValue = "default";
    }
}
