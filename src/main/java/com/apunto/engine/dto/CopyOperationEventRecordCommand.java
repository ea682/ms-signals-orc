package com.apunto.engine.dto;

import com.apunto.engine.dto.client.BinanceExecutionFillClientDto;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CopyOperationEventRecordCommand {
    private UUID idOperation;
    private UUID economicCycleId;
    private UUID exchangeAccountId;
    private UUID sourcePositionCycleId;
    private UUID dispatchIntentId;
    private Long userCopyAllocationId;
    private String copyStrategyCode;
    private String scopeType;
    private String scopeValue;
    private String strategyKey;
    private String generationId;
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
    private List<String> tradeIds;
    private List<BinanceExecutionFillClientDto> individualFills;
    private BigDecimal averageFillPrice;
    private BigDecimal entryPrice;
    private BigDecimal exitPrice;
    private BigDecimal entryFee;
    private BigDecimal exitFee;
    private BigDecimal totalFees;
    private BigDecimal fundingPaid;
    private BigDecimal fundingReceived;
    private BigDecimal netFunding;
    private BigDecimal grossRealizedPnl;
    private BigDecimal netRealizedPnl;
    private BigDecimal unrealizedPnl;
    private BigDecimal expectedPrice;
    private BigDecimal actualPrice;
    private BigDecimal slippageBps;
    private BigDecimal slippageUsd;
    private OffsetDateTime submittedAt;
    private OffsetDateTime acceptedAt;
    private OffsetDateTime filledAt;
    private Long sourceToSubmitLatencyMs;
    private Long submitToFillLatencyMs;
    private Long endToEndLatencyMs;
    private String economicDataStatus;
    private String strategyVersion;
    private String sizingPolicyVersion;
    private String symbolMappingVersion;
    private String feeModelVersion;
    private String fundingModelVersion;
    private String slippageModelVersion;
    private String liquidityModelVersion;
    private BigDecimal calibrationCapitalUsd;
    private BigDecimal targetLeverage;
    private BigDecimal calibrationTargetNotionalUsd;
    private String copyAction;
    private String notionalBand;
    private String traceId;
    private String source;
    private String reasonCode;
    private OffsetDateTime eventTime;
}
