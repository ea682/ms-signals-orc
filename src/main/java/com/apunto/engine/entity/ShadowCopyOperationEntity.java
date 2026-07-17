package com.apunto.engine.entity;

import com.apunto.engine.shared.metric.MetricStrategyIdentity;
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
@Table(name = "shadow_copy_operation", schema = "futuros_operaciones")
public class ShadowCopyOperationEntity {

    @Id
    @Column(name = "id_operation", nullable = false, columnDefinition = "uuid")
    private UUID idOperation;

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

    @Column(name = "id_order_origin", nullable = false, length = 120)
    private String idOrderOrigin;

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

    @Column(name = "type_operation", nullable = false, length = 20)
    private String typeOperation;

    @Column(name = "size_usd", precision = 38, scale = 12)
    private BigDecimal sizeUsd;

    @Column(name = "size_par", precision = 38, scale = 12)
    private BigDecimal sizePar;

    @Column(name = "price_entry", precision = 38, scale = 12)
    private BigDecimal priceEntry;

    @Column(name = "price_close", precision = 38, scale = 12)
    private BigDecimal priceClose;

    @Column(name = "simulated_fee_usd", nullable = false, precision = 38, scale = 12)
    private BigDecimal simulatedFeeUsd;

    @Column(name = "simulated_slippage_usd", nullable = false, precision = 38, scale = 12)
    private BigDecimal simulatedSlippageUsd;

    @Column(name = "realized_pnl_usd", precision = 38, scale = 12)
    private BigDecimal realizedPnlUsd;

    @Column(name = "status", nullable = false, length = 32)
    private String status;

    @Column(name = "date_creation", nullable = false, columnDefinition = "timestamp with time zone")
    private OffsetDateTime dateCreation;

    @Column(name = "date_close", columnDefinition = "timestamp with time zone")
    private OffsetDateTime dateClose;

    @Column(name = "is_active", nullable = false)
    private boolean active;

    @PrePersist
    void prePersist() {
        if (idOperation == null) idOperation = UUID.randomUUID();
        if (dateCreation == null) dateCreation = OffsetDateTime.now();
        normalize();
    }

    @PreUpdate
    void preUpdate() {
        normalize();
    }

    private void normalize() {
        copyStrategyCode = MetricStrategyIdentity.strategyCode(copyStrategyCode);
        scopeType = MetricStrategyIdentity.scopeType(scopeType, copyStrategyCode);
        scopeValue = MetricStrategyIdentity.scopeValue(scopeValue, copyStrategyCode);
        strategyKey = MetricStrategyIdentity.canonicalKey(idWalletOrigin, copyStrategyCode, scopeType, scopeValue);
        if (status == null || status.isBlank()) status = active ? "OPEN" : "CLOSED";
        if (simulatedFeeUsd == null) simulatedFeeUsd = BigDecimal.ZERO;
        if (simulatedSlippageUsd == null) simulatedSlippageUsd = BigDecimal.ZERO;
    }
}
