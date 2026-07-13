package com.apunto.engine.service.copy.dispatch;

import com.apunto.engine.entity.CopyDispatchIntentEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.OffsetDateTime;
import java.util.Set;
import java.util.UUID;

@Component
public class CopyDispatchPayloadConflictRecorder {

    private static final Set<String> TERMINAL = Set.of(
            "PERSISTED", "REJECTED", "FAILED_FINAL", "CANCELLED", "MANUAL_REVIEW");

    private final CopyDispatchPayloadConflictStore store;
    private final CopyDispatchPayloadSnapshotFactory snapshotFactory;
    private final Clock clock;

    @Autowired
    public CopyDispatchPayloadConflictRecorder(CopyDispatchPayloadConflictStore store,
                                               CopyDispatchPayloadSnapshotFactory snapshotFactory) {
        this(store, snapshotFactory, Clock.systemUTC());
    }

    CopyDispatchPayloadConflictRecorder(CopyDispatchPayloadConflictStore store,
                                        CopyDispatchPayloadSnapshotFactory snapshotFactory,
                                        Clock clock) {
        this.store = store;
        this.snapshotFactory = snapshotFactory;
        this.clock = clock;
    }

    @Transactional
    public CopyDispatchPayloadConflictRecord record(CopyDispatchIntentEntity intent,
                                                    CopyDispatchRequest request) {
        CopyDispatchPayloadComparison comparison = snapshotFactory.compare(intent, request);
        boolean nonTerminal = intent.getStatus() == null || !TERMINAL.contains(intent.getStatus());
        boolean manualReview = nonTerminal && store.markManualReview(
                intent.getId(), intent.getRequestHash(), request.requestHash());
        CopyDispatchPayloadConflictRecord conflict = new CopyDispatchPayloadConflictRecord(
                UUID.randomUUID(), intent.getId(), intent.getIdempotencyKey(), intent.getRequestHash(),
                request.requestHash(), intent.getStatus(), comparison.existingPayload(),
                comparison.incomingPayload(), comparison.fieldDiff(), manualReview,
                OffsetDateTime.now(clock));
        store.upsert(conflict);
        return conflict;
    }
}
