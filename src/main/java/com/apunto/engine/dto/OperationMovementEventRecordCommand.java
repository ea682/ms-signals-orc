package com.apunto.engine.dto;

import com.fasterxml.jackson.databind.JsonNode;
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
public class OperationMovementEventRecordCommand {
    private UUID idOrderOrigin;
    private String movementKey;
    private String idempotencyKey;
    private String positionKey;
    private String idWalletOrigin;
    private String parsymbol;
    private String typeOperation;
    private String eventType;
    private String deltaType;
    private String sourceEventType;
    private String status;
    private BigDecimal sizeQty;
    private BigDecimal signedSizeQty;
    private BigDecimal previousSizeQty;
    private BigDecimal resultingSizeQty;
    private BigDecimal deltaSizeQty;
    private BigDecimal notionalUsd;
    private BigDecimal marginUsedUsd;
    private BigDecimal entryPrice;
    private BigDecimal markPrice;
    private BigDecimal exitPrice;
    private BigDecimal realizedPnlUsd;
    private BigDecimal leverage;
    private BigDecimal rawNotionalUsd;
    private BigDecimal positionNotionalUsd;
    private BigDecimal closedNotionalUsd;
    private BigDecimal closedMarginUsedUsd;
    private BigDecimal effectiveCloseQty;
    private BigDecimal effectiveEntryPrice;
    private BigDecimal effectiveExitPrice;
    private BigDecimal effectiveRealizedPnlUsd;
    private String normalizationStatus;
    private String normalizationReason;
    private Long walletVersion;
    private Long snapshotVersion;
    private OffsetDateTime sourceTs;
    private OffsetDateTime detectedAt;
    private OffsetDateTime publishedAt;
    private OffsetDateTime eventTime;
    private String traceId;
    private String source;
    private String reasonCode;
    private Integer copyEligibleUsers;
    private Integer copySubmittedTasks;
    private Integer copyBusinessSkipped;
    private Integer copyFallbackJobs;
    private Boolean copyFallbackUsed;
    private JsonNode raw;
}
