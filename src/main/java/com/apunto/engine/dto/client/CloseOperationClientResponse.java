package com.apunto.engine.dto.client;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CloseOperationClientResponse {
    @NotBlank
    @NotNull
    private String symbol;

    @NotNull
    @DecimalMin(value = "0.00000001", inclusive = false)
    private BigDecimal operationQty;
}
