package com.apunto.engine.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CopyOperationEventRecordCommand {
    private UUID idOperation;
    private UUID dispatchIntentId;
    private Long userCopyAllocationId;
    private String copyStrategyCode;
    private String executionMode;
    private Boolean shadow;
    private String decision;
    private String decisionReason;
    private String sourceMovementKey;
    private String idOrderOrigin;
    private String idUser;
    private String idWalletOrigin;
    private String parsymbol;
    private String typeOperation;
    private String eventType;
    private String copyIntent;
    private String binanceOrderId;
    private String clientOrderId;
    private String side;
    private String positionSide;
    private BigDecimal qtyRequested;
    private BigDecimal qtyExecuted;
    private BigDecimal price;
    private String priceStatus;
    private BigDecimal notionalUsd;
    private BigDecimal previousQty;
    private BigDecimal resultingQty;
    private BigDecimal realizedPnlUsd;
    private BigDecimal feeUsd;
    private String traceId;
    private String source;
    private String reasonCode;
    private OffsetDateTime eventTime;
}
