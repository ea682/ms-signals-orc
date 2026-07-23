package com.apunto.engine.entity;

import com.apunto.engine.dto.client.BinanceExecutionFillClientDto;
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
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

@Entity
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "copy_operation_event", schema = "futuros_operaciones")
public class CopyOperationEventEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "id_event", nullable = false, columnDefinition = "uuid")
    private UUID idEvent;

    @Column(name = "id_operation", columnDefinition = "uuid")
    private UUID idOperation;

    @Column(name = "economic_cycle_id", columnDefinition = "uuid")
    private UUID economicCycleId;

    @Column(name = "exchange_account_id", columnDefinition = "uuid")
    private UUID exchangeAccountId;

    @Column(name = "source_position_cycle_id", columnDefinition = "uuid")
    private UUID sourcePositionCycleId;

    @Column(name = "dispatch_intent_id", columnDefinition = "uuid")
    private UUID dispatchIntentId;

    @Column(name = "execution_intent_id", columnDefinition = "uuid")
    private UUID executionIntentId;

    @Column(name = "user_copy_allocation_id")
    private Long userCopyAllocationId;

    @Column(name = "copy_strategy_code", length = 64)
    private String copyStrategyCode;

    @Column(name = "scope_type", length = 32)
    private String scopeType;

    @Column(name = "scope_value", length = 180)
    private String scopeValue;

    @Column(name = "strategy_key", length = 520)
    private String strategyKey;

    @Column(name = "metric_generation_id", length = 80)
    private String metricGenerationId;

    @Column(name = "execution_mode", length = 16, nullable = false)
    private String executionMode;

    @Column(name = "is_shadow", nullable = false)
    private boolean shadow;

    @Column(name = "decision", length = 40)
    private String decision;

    @Column(name = "decision_reason", length = 160)
    private String decisionReason;

    @Column(name = "source_movement_key", length = 160)
    private String sourceMovementKey;

    @Column(name = "id_order_origin", nullable = false)
    private String idOrderOrigin;

    @Column(name = "id_user", nullable = false)
    private String idUser;

    @Column(name = "id_wallet_origin", nullable = false)
    private String idWalletOrigin;

    @Column(name = "parsymbol", nullable = false)
    private String parsymbol;

    @Column(name = "type_operation", nullable = false)
    private String typeOperation;

    @Column(name = "event_type", nullable = false)
    private String eventType;

    @Column(name = "copy_intent")
    private String copyIntent;

    @Column(name = "binance_order_id")
    private String binanceOrderId;

    @Column(name = "client_order_id")
    private String clientOrderId;

    @Column(name = "side")
    private String side;

    @Column(name = "position_side")
    private String positionSide;

    @Column(name = "qty_requested", precision = 38, scale = 12)
    private BigDecimal qtyRequested;

    @Column(name = "qty_executed", precision = 38, scale = 12)
    private BigDecimal qtyExecuted;

    @Column(name = "price", precision = 38, scale = 12)
    private BigDecimal price;

    @Column(name = "price_status", length = 32)
    private String priceStatus;

    @Column(name = "notional_usd", precision = 38, scale = 12)
    private BigDecimal notionalUsd;

    @Column(name = "previous_qty", precision = 38, scale = 12)
    private BigDecimal previousQty;

    @Column(name = "resulting_qty", precision = 38, scale = 12)
    private BigDecimal resultingQty;

    @Column(name = "realized_pnl_usd", precision = 38, scale = 12)
    private BigDecimal realizedPnlUsd;

    @Column(name = "fee_usd", precision = 38, scale = 12)
    private BigDecimal feeUsd;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "trade_ids", columnDefinition = "jsonb")
    private List<String> tradeIds;

    @Column(name = "requested_quantity", precision = 38, scale = 12)
    private BigDecimal requestedQuantity;

    @Column(name = "executed_quantity", precision = 38, scale = 12)
    private BigDecimal executedQuantity;

    @Column(name = "average_fill_price", precision = 38, scale = 12)
    private BigDecimal averageFillPrice;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "individual_fills", columnDefinition = "jsonb")
    private List<BinanceExecutionFillClientDto> individualFills;

    @Column(name = "entry_price", precision = 38, scale = 12)
    private BigDecimal entryPrice;

    @Column(name = "exit_price", precision = 38, scale = 12)
    private BigDecimal exitPrice;

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

    @Column(name = "gross_realized_pnl", precision = 38, scale = 12)
    private BigDecimal grossRealizedPnl;

    @Column(name = "net_realized_pnl", precision = 38, scale = 12)
    private BigDecimal netRealizedPnl;

    @Column(name = "unrealized_pnl", precision = 38, scale = 12)
    private BigDecimal unrealizedPnl;

    @Column(name = "expected_price", precision = 38, scale = 12)
    private BigDecimal expectedPrice;

    @Column(name = "actual_price", precision = 38, scale = 12)
    private BigDecimal actualPrice;

    @Column(name = "slippage_bps", precision = 38, scale = 12)
    private BigDecimal slippageBps;

    @Column(name = "slippage_usd", precision = 38, scale = 12)
    private BigDecimal slippageUsd;

    @Column(name = "submitted_at", columnDefinition = "timestamp with time zone")
    private OffsetDateTime submittedAt;

    @Column(name = "accepted_at", columnDefinition = "timestamp with time zone")
    private OffsetDateTime acceptedAt;

    @Column(name = "filled_at", columnDefinition = "timestamp with time zone")
    private OffsetDateTime filledAt;

    @Column(name = "persisted_at", columnDefinition = "timestamp with time zone")
    private OffsetDateTime persistedAt;

    @Column(name = "source_to_submit_latency_ms")
    private Long sourceToSubmitLatencyMs;

    @Column(name = "submit_to_fill_latency_ms")
    private Long submitToFillLatencyMs;

    @Column(name = "end_to_end_latency_ms")
    private Long endToEndLatencyMs;

    @Column(name = "economic_data_status", nullable = false, length = 32)
    private String economicDataStatus;

    @Column(name = "strategy_version", length = 64)
    private String strategyVersion;

    @Column(name = "sizing_policy_version", length = 64)
    private String sizingPolicyVersion;

    @Column(name = "symbol_mapping_version", length = 64)
    private String symbolMappingVersion;

    @Column(name = "fee_model_version", length = 64)
    private String feeModelVersion;

    @Column(name = "funding_model_version", length = 64)
    private String fundingModelVersion;

    @Column(name = "slippage_model_version", length = 64)
    private String slippageModelVersion;

    @Column(name = "liquidity_model_version", length = 64)
    private String liquidityModelVersion;

    @Column(name = "calibration_capital_usd", precision = 38, scale = 12)
    private BigDecimal calibrationCapitalUsd;

    @Column(name = "target_leverage", precision = 12, scale = 4)
    private BigDecimal targetLeverage;

    @Column(name = "calibration_target_notional_usd", precision = 38, scale = 12)
    private BigDecimal calibrationTargetNotionalUsd;

    @Column(name = "copy_action", length = 32)
    private String copyAction;

    @Column(name = "notional_band", length = 32)
    private String notionalBand;

    @Column(name = "trace_id")
    private String traceId;

    @Column(name = "source")
    private String source;

    @Column(name = "reason_code")
    private String reasonCode;

    @Column(name = "event_time", nullable = false, columnDefinition = "timestamp with time zone")
    private OffsetDateTime eventTime;

    @Column(name = "date_creation", nullable = false, columnDefinition = "timestamp with time zone")
    private OffsetDateTime dateCreation;
}
