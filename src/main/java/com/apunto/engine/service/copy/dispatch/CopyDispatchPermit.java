package com.apunto.engine.service.copy.dispatch;

import com.apunto.engine.dto.client.BinanceFuturesOrderClientResponse;

import java.util.UUID;

public record CopyDispatchPermit(
        UUID intentId,
        Decision decision,
        BinanceFuturesOrderClientResponse knownResponse,
        String existingStatus
) {
    public enum Decision { SEND, REUSE_ACKNOWLEDGED, RECONCILE_EXISTING, NOOP_PERSISTED, CONFLICT, REJECTED }

    public static CopyDispatchPermit send(UUID id) {
        return new CopyDispatchPermit(id, Decision.SEND, null, "DISPATCHING");
    }

    public static CopyDispatchPermit reuse(UUID id, BinanceFuturesOrderClientResponse response, String status) {
        return new CopyDispatchPermit(id, Decision.REUSE_ACKNOWLEDGED, response, status);
    }

    public static CopyDispatchPermit reconcile(UUID id, String status) {
        return new CopyDispatchPermit(id, Decision.RECONCILE_EXISTING, null, status);
    }

    public static CopyDispatchPermit noop(UUID id, String status) {
        return new CopyDispatchPermit(id, Decision.NOOP_PERSISTED, null, status);
    }

    public static CopyDispatchPermit conflict(UUID id, String status) {
        return new CopyDispatchPermit(id, Decision.CONFLICT, null, status);
    }

    public static CopyDispatchPermit rejected(UUID id, String status) {
        return new CopyDispatchPermit(id, Decision.REJECTED, null, status);
    }
}
