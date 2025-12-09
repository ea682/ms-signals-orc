package com.apunto.engine.dto.client;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class BinanceFuturesSymbolInfoClientDto {

    private String symbol;
    private String pair;
    private String contractType;
    private Long deliveryDate;
    private Long onboardDate;
    private String status;
    private String maintMarginPercent;
    private String requiredMarginPercent;
    private String baseAsset;
    private String quoteAsset;
    private String marginAsset;
    private Integer pricePrecision;
    private Integer quantityPrecision;
    private Integer baseAssetPrecision;
    private Integer quotePrecision;
    private String underlyingType;
    private List<String> underlyingSubType;
    private Integer settlePlan;
    private String triggerProtect;
    private String liquidationFee;
    private String marketTakeBound;
    private Integer maxMoveOrderLimit;
    private List<BinanceFuturesSymbolFilterDto> filters;

    @JsonProperty("orderTypes")
    private List<String> orderTypes;

    private List<String> timeInForce;

    private List<String> permissionSets;
}
