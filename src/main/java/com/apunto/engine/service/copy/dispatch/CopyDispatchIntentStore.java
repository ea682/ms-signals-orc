package com.apunto.engine.service.copy.dispatch;

import com.apunto.engine.dto.client.BinanceFuturesOrderClientResponse;

import java.util.UUID;

public interface CopyDispatchIntentStore {

    CopyDispatchPermit acquire(CopyDispatchRequest request);

    void acknowledge(UUID intentId,
                     NormalizedBinanceExecution execution,
                     BinanceFuturesOrderClientResponse response);

    void markAmbiguous(UUID intentId, String reasonCode, String detail);

    void markRejected(UUID intentId, String reasonCode, String detail);

    default void linkRequiredEvent(UUID intentId, UUID copyOperationEventId) {
    }

    void markPersistencePending(String clientOrderId, String reasonCode, String detail);

    default void markPersistencePending(UUID intentId, String clientOrderId, String reasonCode, String detail) {
        markPersistencePending(clientOrderId, reasonCode, detail);
    }

    default void markPersisted(String clientOrderId, UUID copyOperationId) {
        // Optional for non-persistent test stores.
    }

    default void markPersisted(UUID intentId, String clientOrderId, UUID copyOperationId) {
        markPersisted(clientOrderId, copyOperationId);
    }
}
