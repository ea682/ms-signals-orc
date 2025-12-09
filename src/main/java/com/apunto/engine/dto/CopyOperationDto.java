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
}
