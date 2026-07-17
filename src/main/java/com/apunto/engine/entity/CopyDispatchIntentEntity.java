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
import java.time.ZoneOffset;
import java.util.UUID;

@Entity
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "copy_dispatch_intent", schema = "futuros_operaciones")
public class CopyDispatchIntentEntity {
    @Id
    @Column(name = "id", nullable = false, columnDefinition = "uuid")
    private UUID id;

    @Column(name = "idempotency_key", nullable = false, length = 64)
    private String idempotencyKey;
    @Column(name = "id_user", nullable = false, length = 80)
    private String idUser;
    @Column(name = "user_copy_allocation_id")
    private Long userCopyAllocationId;
    @Column(name = "execution_mode", nullable = false, length = 16)
    private String executionMode;
    @Column(name = "wallet_id", length = 180)
    private String walletId;
    @Column(name = "strategy_code", length = 64)
    private String strategyCode;
    @Column(name = "scope_type", length = 32)
    private String scopeType;
    @Column(name = "scope_value", length = 180)
    private String scopeValue;
    @Column(name = "metric_generation_id", length = 80)
    private String metricGenerationId;
    @Column(name = "source_event_id", nullable = false, length = 600)
    private String sourceEventId;
    @Column(name = "id_order_origin", length = 120)
    private String idOrderOrigin;
    @Column(name = "source_event_type", length = 40)
    private String sourceEventType;
    @Column(name = "copy_intent", nullable = false, length = 40)
    private String copyIntent;
    @Column(name = "symbol", nullable = false, length = 40)
    private String symbol;
    @Column(name = "side", length = 12)
    private String side;
    @Column(name = "position_side", length = 12)
    private String positionSide;
    @Column(name = "reduce_only", nullable = false)
    private boolean reduceOnly;
    @Column(name = "requested_qty", precision = 38, scale = 18)
    private BigDecimal requestedQty;
    @Column(name = "requested_margin_usd", precision = 38, scale = 12, nullable = false)
    private BigDecimal requestedMarginUsd;
    @Column(name = "requested_notional_usd", precision = 38, scale = 12, nullable = false)
    private BigDecimal requestedNotionalUsd;
    @Column(name = "notional_band", length = 32)
    private String notionalBand;
    @Column(name = "reference_price", precision = 38, scale = 18)
    private BigDecimal referencePrice;
    @Column(name = "requested_leverage")
    private Integer requestedLeverage;
    @Column(name = "user_max_concurrent_positions")
    private Integer userMaxConcurrentPositions;
    @Column(name = "reserved_position_count", nullable = false)
    private short reservedPositionCount;
    @Column(name = "reservation_status", nullable = false, length = 24)
    private String reservationStatus;
    @Column(name = "client_order_id", nullable = false, length = 36)
    private String clientOrderId;
    @Column(name = "binance_order_id")
    private Long binanceOrderId;
    @Column(name = "binance_status", length = 32)
    private String binanceStatus;
    @Column(name = "executed_qty", precision = 38, scale = 18)
    private BigDecimal executedQty;
    @Column(name = "persisted_executed_qty", precision = 38, scale = 18, nullable = false)
    private BigDecimal persistedExecutedQty;
    @Column(name = "average_price", precision = 38, scale = 18)
    private BigDecimal averagePrice;
    @Column(name = "cumulative_quote_qty", precision = 38, scale = 18)
    private BigDecimal cumulativeQuoteQty;
    @Column(name = "average_price_status", nullable = false, length = 32)
    private String averagePriceStatus;
    @Column(name = "status", nullable = false, length = 32)
    private String status;
    @Column(name = "attempts", nullable = false)
    private int attempts;
    @Column(name = "reconciliation_attempts", nullable = false)
    private int reconciliationAttempts;
    @Column(name = "request_hash", nullable = false, length = 64)
    private String requestHash;
    @Column(name = "response_snapshot", columnDefinition = "text")
    private String responseSnapshot;
    @Column(name = "last_error_code", length = 80)
    private String lastErrorCode;
    @Column(name = "last_error_detail", length = 1000)
    private String lastErrorDetail;
    @Column(name = "copy_operation_id", columnDefinition = "uuid")
    private UUID copyOperationId;
    @Column(name = "copy_operation_event_id", columnDefinition = "uuid")
    private UUID copyOperationEventId;
    @Column(name = "claimed_by", length = 128)
    private String claimedBy;
    @Column(name = "created_at", nullable = false, columnDefinition = "timestamp with time zone")
    private OffsetDateTime createdAt;
    @Column(name = "claimed_at", columnDefinition = "timestamp with time zone")
    private OffsetDateTime claimedAt;
    @Column(name = "sent_at", columnDefinition = "timestamp with time zone")
    private OffsetDateTime sentAt;
    @Column(name = "acknowledged_at", columnDefinition = "timestamp with time zone")
    private OffsetDateTime acknowledgedAt;
    @Column(name = "filled_at", columnDefinition = "timestamp with time zone")
    private OffsetDateTime filledAt;
    @Column(name = "persisted_at", columnDefinition = "timestamp with time zone")
    private OffsetDateTime persistedAt;
    @Column(name = "next_reconciliation_at", columnDefinition = "timestamp with time zone")
    private OffsetDateTime nextReconciliationAt;
    @Column(name = "updated_at", nullable = false, columnDefinition = "timestamp with time zone")
    private OffsetDateTime updatedAt;

    @PrePersist
    void prePersist() {
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        if (id == null) id = UUID.randomUUID();
        if (createdAt == null) createdAt = now;
        updatedAt = now;
        if (status == null) status = "CREATED";
        if (reservationStatus == null) reservationStatus = "UNRESERVED";
        if (averagePriceStatus == null) averagePriceStatus = "NOT_AVAILABLE";
        if (requestedMarginUsd == null) requestedMarginUsd = BigDecimal.ZERO;
        if (requestedNotionalUsd == null) requestedNotionalUsd = BigDecimal.ZERO;
    }

    @PreUpdate
    void preUpdate() {
        updatedAt = OffsetDateTime.now(ZoneOffset.UTC);
    }
}
