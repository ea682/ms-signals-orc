package com.apunto.engine.hyperliquid.service;

import com.apunto.engine.hyperliquid.model.HyperliquidCopyLifecycleDecision;
import com.apunto.engine.hyperliquid.model.HyperliquidDeltaType;
import com.apunto.engine.jobs.model.CopyJobAction;

public interface HyperliquidCopyLifecycleGuard {

    HyperliquidCopyLifecycleDecision decide(CopyJobAction action, HyperliquidDeltaType deltaType, boolean activeCopy);
}
