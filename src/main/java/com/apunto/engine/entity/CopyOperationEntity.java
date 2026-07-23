package com.apunto.engine.entity;


import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

@Entity
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Table(name = "copy_operation", schema = "futuros_operaciones")
public class CopyOperationEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "id_operation", nullable = false, columnDefinition = "uuid")
    private UUID idOperation;

    @Column(name = "id_user", nullable = false)
    private String idUser;

    @Column(name = "id_orden", nullable = false)
    private String idOrden;

    @Column(name = "id_order_origin", nullable = false)
    private String idOrderOrigin;

    @Column(name = "id_wallet_origin", nullable = false)
    private String idWalletOrigin;

    @Column(name = "parsymbol", nullable = false)
    private String parsymbol;

    @Column(name = "type_operation", nullable = false)
    private String typeOperation;

    @Column(name = "leverage", precision = 50, scale = 2, nullable = false)
    private BigDecimal leverage;

    @Column(name = "size_usd", precision = 38, scale = 12, nullable = false)
    private BigDecimal siseUsd;

    @Column(name = "size_par", precision = 38, scale = 12, nullable = false)
    private BigDecimal sizePar;

    @Column(name = "price_entry", precision = 38, scale = 12, nullable = false)
    private BigDecimal priceEntry;

    @Column(name = "price_close", precision = 38, scale = 12)
    private BigDecimal priceClose;

    @Column(name = "date_creation", nullable = false,
            columnDefinition = "timestamp with time zone")
    private OffsetDateTime dateCreation;

    @Column(name = "date_close",
            columnDefinition = "timestamp with time zone")
    private OffsetDateTime dateClose;

    @Column(name = "is_active", nullable = false)
    private boolean active;

    @Column(name = "user_copy_allocation_id")
    private Long userCopyAllocationId;

    @Column(name = "copy_strategy_code", length = 64)
    private String copyStrategyCode;

    @Column(name = "execution_mode", length = 16, nullable = false)
    private String executionMode;

    @Column(name = "is_shadow", nullable = false)
    private boolean shadow;

    @Column(name = "shadow_status", length = 32)
    private String shadowStatus;

    @Column(name = "dispatch_intent_id", columnDefinition = "uuid")
    private UUID dispatchIntentId;

    @Column(name = "source_event_id", length = 600)
    private String sourceEventId;

    @Column(name = "client_order_id", length = 36)
    private String clientOrderId;

    @Column(name = "price_status", length = 32)
    private String priceStatus;

    @Column(name = "economic_cycle_id", columnDefinition = "uuid")
    private UUID economicCycleId;

    @Column(name = "exchange_account_id", columnDefinition = "uuid")
    private UUID exchangeAccountId;

    @Column(name = "source_position_cycle_id", columnDefinition = "uuid")
    private UUID sourcePositionCycleId;

    @Column(name = "cycle_sequence")
    private Long cycleSequence;

    @Column(name = "economic_data_status", nullable = false, length = 32)
    private String economicDataStatus;
}
