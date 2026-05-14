package com.apunto.engine.hyperliquid.service;

import com.apunto.engine.events.OperacionEvent;
import com.apunto.engine.hyperliquid.dto.HyperliquidDirectCopyDispatchResult;

public interface HyperliquidDirectCopyDispatchService {

    HyperliquidDirectCopyDispatchResult dispatch(OperacionEvent event);
}
