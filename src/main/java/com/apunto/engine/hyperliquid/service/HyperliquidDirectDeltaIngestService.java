package com.apunto.engine.hyperliquid.service;

import com.apunto.engine.hyperliquid.dto.HyperliquidDeltaAcceptedResponse;
import com.apunto.engine.hyperliquid.dto.HyperliquidMappedDelta;

public interface HyperliquidDirectDeltaIngestService {

    HyperliquidDeltaAcceptedResponse accept(HyperliquidMappedDelta mappedDelta);
}
