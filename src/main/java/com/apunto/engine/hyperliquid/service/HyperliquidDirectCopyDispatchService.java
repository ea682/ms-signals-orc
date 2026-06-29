package com.apunto.engine.hyperliquid.service;

import com.apunto.engine.hyperliquid.dto.HyperliquidDirectCopyDispatchResult;
import com.apunto.engine.hyperliquid.dto.HyperliquidMappedDelta;

public interface HyperliquidDirectCopyDispatchService {

    HyperliquidDirectCopyDispatchResult dispatch(HyperliquidMappedDelta mappedDelta);

    default HyperliquidDirectCopyDispatchResult dispatch(HyperliquidMappedDelta mappedDelta, long eventReceivedNs) {
        return dispatch(mappedDelta);
    }
}
