package com.apunto.engine.service.copy.dispatch;

public interface CopyOrderRuntimePreflight {

    CopyOrderRuntimePreflightDecision evaluate(CopyDispatchRequest request);
}
