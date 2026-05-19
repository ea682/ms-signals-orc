package com.apunto.engine.entity;

import com.fasterxml.jackson.databind.JsonNode;
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
import java.util.UUID;

@Entity
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "operation_movement_event", schema = "futuros_operaciones")
public class OperationMovementEventEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "id_event", nullable = false, columnDefinition = "uuid")
    private UUID idEvent;

    @Column(name = "id_order_origin", nullable = false, columnDefinition = "uuid")
    private UUID idOrderOrigin;

    @Column(name = "movement_key", nullable = false, length = 600)
    private String movementKey;

    @Column(name = "idempotency_key", length = 600)
    private String idempotencyKey;

    @Column(name = "position_key", nullable = false, length = 300)
    private String positionKey;

    @Column(name = "id_wallet_origin", nullable = false, length = 180)
    private String idWalletOrigin;

    @Column(name = "parsymbol", nullable = false, length = 40)
    private String parsymbol;

    @Column(name = "type_operation", nullable = false, length = 20)
    private String typeOperation;

    @Column(name = "event_type", nullable = false, length = 30)
    private String eventType;

    @Column(name = "delta_type", nullable = false, length = 30)
    private String deltaType;

    @Column(name = "source_event_type", length = 30)
    private String sourceEventType;

    @Column(name = "status", length = 30)
    private String status;

    @Column(name = "size_qty", precision = 38, scale = 18)
    private BigDecimal sizeQty;

    @Column(name = "signed_size_qty", precision = 38, scale = 18)
    private BigDecimal signedSizeQty;

    @Column(name = "previous_size_qty", precision = 38, scale = 18)
    private BigDecimal previousSizeQty;

    @Column(name = "resulting_size_qty", precision = 38, scale = 18)
    private BigDecimal resultingSizeQty;

    @Column(name = "delta_size_qty", precision = 38, scale = 18)
    private BigDecimal deltaSizeQty;

    @Column(name = "notional_usd", precision = 38, scale = 18)
    private BigDecimal notionalUsd;

    @Column(name = "margin_used_usd", precision = 38, scale = 18)
    private BigDecimal marginUsedUsd;

    @Column(name = "entry_price", precision = 38, scale = 18)
    private BigDecimal entryPrice;

    @Column(name = "mark_price", precision = 38, scale = 18)
    private BigDecimal markPrice;

    @Column(name = "exit_price", precision = 38, scale = 18)
    private BigDecimal exitPrice;

    @Column(name = "realized_pnl_usd", precision = 38, scale = 18)
    private BigDecimal realizedPnlUsd;

    @Column(name = "leverage", precision = 38, scale = 18)
    private BigDecimal leverage;

    @Column(name = "wallet_version")
    private Long walletVersion;

    @Column(name = "snapshot_version")
    private Long snapshotVersion;

    @Column(name = "source_ts", columnDefinition = "timestamp with time zone")
    private OffsetDateTime sourceTs;

    @Column(name = "detected_at", columnDefinition = "timestamp with time zone")
    private OffsetDateTime detectedAt;

    @Column(name = "published_at", columnDefinition = "timestamp with time zone")
    private OffsetDateTime publishedAt;

    @Column(name = "event_time", nullable = false, columnDefinition = "timestamp with time zone")
    private OffsetDateTime eventTime;

    @Column(name = "trace_id", length = 128)
    private String traceId;

    @Column(name = "source", length = 80)
    private String source;

    @Column(name = "reason_code", length = 120)
    private String reasonCode;

    @Column(name = "copy_eligible_users")
    private Integer copyEligibleUsers;

    @Column(name = "copy_submitted_tasks")
    private Integer copySubmittedTasks;

    @Column(name = "copy_business_skipped")
    private Integer copyBusinessSkipped;

    @Column(name = "copy_fallback_jobs")
    private Integer copyFallbackJobs;

    @Column(name = "copy_fallback_used")
    private Boolean copyFallbackUsed;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "raw", columnDefinition = "jsonb")
    private JsonNode raw;

    @Column(name = "date_creation", nullable = false, columnDefinition = "timestamp with time zone")
    private OffsetDateTime dateCreation;
}
