package com.apunto.engine.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
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
@Table(name = "copy_economic_cycle", schema = "futuros_operaciones")
public class CopyEconomicCycleEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "cycle_id", nullable = false, columnDefinition = "uuid")
    private UUID cycleId;

    @Column(name = "copy_operation_id", nullable = false, unique = true, columnDefinition = "uuid")
    private UUID copyOperationId;

    @Column(name = "cycle_sequence", nullable = false)
    private Long cycleSequence;

    @Column(name = "id_user", nullable = false)
    private String idUser;

    @Column(name = "id_wallet_origin", nullable = false)
    private String idWalletOrigin;

    @Column(name = "user_copy_allocation_id")
    private Long userCopyAllocationId;

    @Column(name = "exchange_account_id", columnDefinition = "uuid")
    private UUID exchangeAccountId;

    @Column(name = "source_position_cycle_id", columnDefinition = "uuid")
    private UUID sourcePositionCycleId;

    @Column(name = "fixed_leverage", precision = 12, scale = 4)
    private BigDecimal fixedLeverage;

    @Column(name = "fixed_margin_mode", length = 24)
    private String fixedMarginMode;

    @Column(name = "fixed_position_mode", length = 24)
    private String fixedPositionMode;

    @Builder.Default
    @Column(name = "virtual_owned_qty", precision = 38, scale = 18, nullable = false)
    private BigDecimal virtualOwnedQty = BigDecimal.ZERO;

    @Column(name = "copy_strategy_code", length = 64)
    private String copyStrategyCode;

    @Column(name = "parsymbol", nullable = false)
    private String parsymbol;

    @Column(name = "position_side", nullable = false)
    private String positionSide;

    @Column(name = "execution_mode", nullable = false, length = 16)
    private String executionMode;

    @Column(name = "source_first_event_id", length = 600)
    private String sourceFirstEventId;

    @Column(name = "source_last_event_id", length = 600)
    private String sourceLastEventId;

    @Column(name = "opened_at", nullable = false, columnDefinition = "timestamp with time zone")
    private OffsetDateTime openedAt;

    @Column(name = "closed_at", columnDefinition = "timestamp with time zone")
    private OffsetDateTime closedAt;

    @Column(name = "cycle_status", nullable = false, length = 32)
    private String cycleStatus;

    @Column(name = "economic_data_status", nullable = false, length = 32)
    private String economicDataStatus;

    @Column(name = "gross_realized_pnl", precision = 38, scale = 12)
    private BigDecimal grossRealizedPnl;

    @Column(name = "net_realized_pnl", precision = 38, scale = 12)
    private BigDecimal netRealizedPnl;

    @Column(name = "unrealized_pnl", precision = 38, scale = 12)
    private BigDecimal unrealizedPnl;

    @Column(name = "entry_fee", precision = 38, scale = 12)
    private BigDecimal entryFee;

    @Column(name = "exit_fee", precision = 38, scale = 12)
    private BigDecimal exitFee;

    @Column(name = "total_fees", precision = 38, scale = 12)
    private BigDecimal totalFees;

    @Column(name = "funding_paid", precision = 38, scale = 12)
    private BigDecimal fundingPaid;

    @Column(name = "funding_received", precision = 38, scale = 12)
    private BigDecimal fundingReceived;

    @Column(name = "net_funding", precision = 38, scale = 12)
    private BigDecimal netFunding;

    @Column(name = "created_at", nullable = false, columnDefinition = "timestamp with time zone")
    private OffsetDateTime createdAt;

    @Column(name = "updated_at", nullable = false, columnDefinition = "timestamp with time zone")
    private OffsetDateTime updatedAt;
}
