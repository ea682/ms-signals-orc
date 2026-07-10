-- Explicit terminal state for ambiguous/reconciliation outcomes that require an operator.
-- Additive state extension; it does not release reservations or rewrite existing intents.
ALTER TABLE futuros_operaciones.copy_dispatch_intent
    DROP CONSTRAINT IF EXISTS chk_copy_dispatch_status;

ALTER TABLE futuros_operaciones.copy_dispatch_intent
    ADD CONSTRAINT chk_copy_dispatch_status CHECK (
        status IN (
            'CREATED', 'CLAIMED', 'DISPATCHING', 'ACKNOWLEDGED', 'NEW',
            'PARTIALLY_FILLED', 'FILLED', 'RECONCILING', 'PERSISTENCE_PENDING',
            'PERSISTED', 'REJECTED', 'FAILED_FINAL', 'CANCELLED', 'MANUAL_REVIEW'
        )
    );

CREATE INDEX IF NOT EXISTS ix_copy_dispatch_intent_manual_review
    ON futuros_operaciones.copy_dispatch_intent (updated_at, id)
    WHERE status = 'MANUAL_REVIEW';

-- Dispatch-backed events are unique by cumulative progress. Legacy rows still
-- need race-safe clientOrderId idempotency without preventing multiple partials.
CREATE UNIQUE INDEX IF NOT EXISTS ux_copy_operation_event_legacy_client_order_id
    ON futuros_operaciones.copy_operation_event (client_order_id)
    WHERE client_order_id IS NOT NULL AND dispatch_intent_id IS NULL;
