package com.apunto.engine.dto.client;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FuturesConvertToBnbClientResponse {
    private boolean success;
    private boolean pending;
    private String fromAsset;
    private String toAsset;
    private String fromAmount;
    private String toAmount;
    private String quoteId;
    private String orderId;
    private String orderStatus;
    private String ratio;
    private String inverseRatio;
    private Long validTimestamp;
    private Long createTime;
    private String message;
    private String errorMessage;
}
