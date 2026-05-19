package com.apunto.engine.service;

import com.apunto.engine.events.OperacionEvent;
import com.apunto.engine.hyperliquid.dto.HyperliquidDirectCopyDispatchResult;
import com.apunto.engine.hyperliquid.dto.HyperliquidMappedDelta;

public interface OperationMovementEventService {
    void recordAsync(HyperliquidMappedDelta mappedDelta, HyperliquidDirectCopyDispatchResult dispatchResult, String reasonCode);

    void recordAsync(OperacionEvent event, String source, String traceId, String reasonCode);
}
