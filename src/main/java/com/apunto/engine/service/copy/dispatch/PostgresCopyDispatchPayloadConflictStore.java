package com.apunto.engine.service.copy.dispatch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.Map;
import java.util.UUID;

@Repository
@RequiredArgsConstructor
public class PostgresCopyDispatchPayloadConflictStore implements CopyDispatchPayloadConflictStore {

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    @Override
    public void upsert(CopyDispatchPayloadConflictRecord record) {
        jdbcTemplate.update("""
                        INSERT INTO futuros_operaciones.copy_dispatch_payload_conflict (
                            id, dispatch_intent_id, idempotency_key, existing_hash, incoming_hash,
                            existing_status, existing_payload, incoming_payload, field_diff,
                            manual_review_required, alert_status, conflict_count,
                            first_seen_at, last_seen_at
                        ) VALUES (?, ?, ?, ?, ?, ?, CAST(? AS jsonb), CAST(? AS jsonb),
                                  CAST(? AS jsonb), ?, 'OPEN', 1, ?, ?)
                        ON CONFLICT (dispatch_intent_id, incoming_hash) DO UPDATE SET
                            conflict_count = copy_dispatch_payload_conflict.conflict_count + 1,
                            last_seen_at = EXCLUDED.last_seen_at,
                            alert_status = 'OPEN',
                            manual_review_required =
                                copy_dispatch_payload_conflict.manual_review_required
                                OR EXCLUDED.manual_review_required
                        """,
                record.id(), record.dispatchIntentId(), record.idempotencyKey(),
                record.existingHash(), record.incomingHash(), record.existingStatus(),
                json(record.existingPayload()), json(record.incomingPayload()),
                json(record.fieldDiff()), record.manualReviewRequired(),
                record.occurredAt(), record.occurredAt());
    }

    @Override
    public boolean markManualReview(UUID intentId, String existingHash, String incomingHash) {
        return jdbcTemplate.update("""
                        UPDATE futuros_operaciones.copy_dispatch_intent
                        SET status = 'MANUAL_REVIEW',
                            last_error_code = 'IDEMPOTENCY_PAYLOAD_CONFLICT',
                            last_error_detail = concat(
                                'request hash mismatch; existing=', ?, '; incoming=', ?),
                            next_reconciliation_at = NULL,
                            updated_at = now()
                        WHERE id = ?
                          AND request_hash = ?
                          AND status NOT IN (
                              'PERSISTED', 'REJECTED', 'FAILED_FINAL', 'CANCELLED', 'MANUAL_REVIEW'
                          )
                        """, existingHash, incomingHash, intentId, existingHash) == 1;
    }

    private String json(Map<?, ?> value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException ex) {
            throw new IllegalArgumentException("Dispatch conflict payload is not serializable", ex);
        }
    }
}
