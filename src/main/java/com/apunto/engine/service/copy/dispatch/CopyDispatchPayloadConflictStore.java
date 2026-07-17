package com.apunto.engine.service.copy.dispatch;

import java.util.UUID;

public interface CopyDispatchPayloadConflictStore {
    void upsert(CopyDispatchPayloadConflictRecord record);
    boolean markManualReview(UUID intentId, String existingHash, String incomingHash);
}
