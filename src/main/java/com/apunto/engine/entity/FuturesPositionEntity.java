package com.apunto.engine.entity;

import com.apunto.engine.shared.enums.PositionSide;
import com.apunto.engine.shared.enums.PositionStatus;
import com.fasterxml.jackson.databind.JsonNode;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.JdbcType;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.dialect.PostgreSQLEnumJdbcType;
import org.hibernate.type.SqlTypes;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

@Entity
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Table(name = "futures_position", schema = "futuros_operaciones")
public class FuturesPositionEntity {

    @Id
    @GeneratedValue
    @Column(name = "id", nullable = false, updatable = false)
    private UUID idFuturesPosition;

    @Column(name = "platform", nullable = false)
    @Builder.Default
    private String platform = "unknown";

    @Column(name = "venue")
    private String venue;

    @Column(name = "chain_id")
    private Integer chainId;

    @Column(name = "external_id", nullable = false)
    private String externalId;

    @Column(name = "account_id", nullable = false)
    private String accountId;

    @Column(name = "symbol", nullable = false)
    private String symbol;

    @Enumerated(EnumType.STRING)
    @JdbcType(PostgreSQLEnumJdbcType.class)
    @Column(name = "status", nullable = false, columnDefinition = "futuros_operaciones.position_status")
    @Builder.Default
    private PositionStatus status = PositionStatus.OPEN;

    @Enumerated(EnumType.STRING)
    @JdbcType(PostgreSQLEnumJdbcType.class)
    @Column(name = "side", columnDefinition = "futuros_operaciones.position_side")
    private PositionSide side;

    @Column(name = "operation_type", nullable = false)
    private String operationType;

    @Column(name = "leverage", nullable = false, precision = 50, scale = 2)
    private BigDecimal leverage;

    @Column(name = "size_qty", precision = 38, scale = 18)
    private BigDecimal sizeQty;

    @Column(name = "notional_usd", precision = 38, scale = 18)
    private BigDecimal notionalUsd;

    @Column(name = "margin_used_usd", precision = 38, scale = 18)
    private BigDecimal marginUsedUsd;

    @Column(name = "size_legacy", nullable = false, precision = 38, scale = 12)
    private BigDecimal sizeLegacy;

    @Column(name = "entry_price", nullable = false, precision = 38, scale = 18)
    private BigDecimal entryPrice;

    @Column(name = "mark_price", nullable = false, precision = 38, scale = 18)
    @Builder.Default
    private BigDecimal markPrice = BigDecimal.ZERO;

    @Column(name = "exit_price", precision = 38, scale = 18)
    private BigDecimal exitPrice;

    @Column(name = "unrealized_pnl_usd", precision = 38, scale = 18)
    private BigDecimal unrealizedPnlUsd;

    @Column(name = "realized_pnl_usd", precision = 38, scale = 18)
    private BigDecimal realizedPnlUsd;

    @Column(name = "liquidation_price", precision = 38, scale = 18)
    private BigDecimal liquidationPrice;

    @Column(name = "source_ts")
    private OffsetDateTime sourceTs;

    @Column(name = "created_at", nullable = false)
    private OffsetDateTime createdAt;

    @Column(name = "updated_at", nullable = false)
    private OffsetDateTime updatedAt;

    @Column(name = "closed_at")
    private OffsetDateTime closedAt;

    @Column(name = "ingested_at", nullable = false)
    private OffsetDateTime ingestedAt;

    @Column(name = "is_active", nullable = false)
    @Builder.Default
    private Boolean isActive = true;

    @Column(name = "has_account_issue", nullable = false)
    @Builder.Default
    private Boolean hasAccountIssue = false;

    @Column(name = "failed_attempts", nullable = false)
    @Builder.Default
    private Integer failedAttempts = 0;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "raw", columnDefinition = "jsonb")
    private JsonNode raw;

    @Column(name = "client_order_id")
    private String clientOrderId;

    @Column(name = "close_client_order_id")
    private String closeClientOrderId;
}
