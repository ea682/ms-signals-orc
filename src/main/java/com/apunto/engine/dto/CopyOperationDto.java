package com.apunto.engine.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;


@AllArgsConstructor
@Data
@Builder
public class CopyOperationDto {

    private UUID idOperation;
    private String idOrden;
    private String idUser;
    private String idOrderOrigin;
    private String idWalletOrigin;
    private String parsymbol;
    private String typeOperation;
    private BigDecimal leverage;
    private BigDecimal siseUsd;
    private BigDecimal sizePar;
    private BigDecimal priceEntry;
    private BigDecimal priceClose;
    private OffsetDateTime dateCreation;
    private OffsetDateTime dateClose;

    private boolean active;

    private Long userCopyAllocationId;
    private String copyStrategyCode;
    private String executionMode;
    private boolean shadow;
    private String shadowStatus;
    private UUID dispatchIntentId;
    private String sourceEventId;
    private String clientOrderId;
    private String priceStatus;
    private UUID economicCycleId;
    private UUID exchangeAccountId;
    private UUID sourcePositionCycleId;
    private String fixedMarginMode;
    private String fixedPositionMode;
    private Long cycleSequence;
    private String economicDataStatus;
}
