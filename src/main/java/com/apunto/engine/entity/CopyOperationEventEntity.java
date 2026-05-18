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
@Table(name = "copy_operation_event", schema = "futuros_operaciones")
public class CopyOperationEventEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "id_event", nullable = false, columnDefinition = "uuid")
    private UUID idEvent;

    @Column(name = "id_operation", columnDefinition = "uuid")
    private UUID idOperation;

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
