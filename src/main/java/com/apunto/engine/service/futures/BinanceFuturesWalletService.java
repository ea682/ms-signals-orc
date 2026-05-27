package com.apunto.engine.service.futures;

import com.apunto.engine.dto.UserDetailDto;
import com.apunto.engine.dto.client.FuturesAssetBalanceClientResponse;
import com.apunto.engine.dto.client.FuturesConvertToBnbClientResponse;
import com.apunto.engine.shared.enums.FuturesCapitalAsset;

import java.math.BigDecimal;

public interface BinanceFuturesWalletService {

    FuturesAssetBalanceClientResponse getAssetBalance(UserDetailDto userDetail, String asset);

    FuturesConvertToBnbClientResponse convertStableAssetToBnb(UserDetailDto userDetail,
                                                              FuturesCapitalAsset fromAsset,
                                                              BigDecimal amount);
}
