-- Immutable, sanitized evidence for an idempotency key reused with another payload.
CREATE TABLE IF NOT EXISTS futuros_operaciones.copy_dispatch_payload_conflict (
    id uuid PRIMARY KEY,
    dispatch_intent_id uuid NOT NULL,
    idempotency_key varchar(64) NOT NULL,
    existing_hash varchar(64) NOT NULL,
    incoming_hash varchar(64) NOT NULL,
    existing_status varchar(32),
    existing_payload jsonb NOT NULL,
    incoming_payload jsonb NOT NULL,
    field_diff jsonb NOT NULL,
    manual_review_required boolean NOT NULL DEFAULT false,
    alert_status varchar(24) NOT NULL DEFAULT 'OPEN',
    conflict_count integer NOT NULL DEFAULT 1,
    first_seen_at timestamp with time zone NOT NULL,
    last_seen_at timestamp with time zone NOT NULL,
    CONSTRAINT fk_copy_dispatch_payload_conflict_intent
        FOREIGN KEY (dispatch_intent_id)
        REFERENCES futuros_operaciones.copy_dispatch_intent(id)
        ON DELETE RESTRICT,
    CONSTRAINT ux_copy_dispatch_payload_conflict
        UNIQUE (dispatch_intent_id, incoming_hash),
    CONSTRAINT chk_copy_dispatch_payload_conflict_alert
        CHECK (alert_status IN ('OPEN', 'ACKNOWLEDGED', 'RESOLVED')),
    CONSTRAINT chk_copy_dispatch_payload_conflict_count
        CHECK (conflict_count > 0)
);

CREATE INDEX IF NOT EXISTS ix_copy_dispatch_payload_conflict_open
    ON futuros_operaciones.copy_dispatch_payload_conflict (alert_status, last_seen_at)
    WHERE alert_status = 'OPEN';

COMMENT ON TABLE futuros_operaciones.copy_dispatch_payload_conflict IS
    'Sanitized field-level evidence for blocked idempotency payload conflicts; never stores credentials.';
COMMENT ON COLUMN futuros_operaciones.copy_dispatch_payload_conflict.conflict_count IS
    'Number of observations for the same durable intent and incoming request hash.';

