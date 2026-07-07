package com.apunto.engine.dto.client;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FuturesAssetBalanceClientResponse {
    private String accountAlias;
    private String asset;
    private String walletBalance;
    private String availableBalance;
    private String marginBalance;
    private String totalWalletBalance;
    private String crossWalletBalance;
    private String crossUnPnl;
    private String maxWithdrawAmount;
    private Boolean marginAvailable;
    private Long updateTime;
}
