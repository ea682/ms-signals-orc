package com.apunto.engine.service.futures;

import com.apunto.engine.shared.enums.FuturesCapitalAsset;

import java.math.BigDecimal;

public interface FuturesBnbPriceService {

    BigDecimal getBnbPrice(FuturesCapitalAsset quoteAsset);
}
