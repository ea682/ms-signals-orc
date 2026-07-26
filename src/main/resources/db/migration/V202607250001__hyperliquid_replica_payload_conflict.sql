ALTER TABLE futuros_operaciones.hyperliquid_direct_ingest_dedupe
    ADD COLUMN IF NOT EXISTS first_payload jsonb NULL;

ALTER TABLE futuros_operaciones.hyperliquid_direct_ingest_dedupe
    ADD COLUMN IF NOT EXISTS fingerprint_algorithm varchar(80) NULL;

COMMENT ON COLUMN futuros_operaciones.hyperliquid_direct_ingest_dedupe.first_payload IS
    'Sanitized first-seen economic payload used to audit active-active replica divergence.';

COMMENT ON COLUMN futuros_operaciones.hyperliquid_direct_ingest_dedupe.fingerprint_algorithm IS
    'Versioned canonical economic fingerprint algorithm. Replica-local versions are excluded.';

CREATE TABLE IF NOT EXISTS futuros_operaciones.hyperliquid_replica_payload_conflict (
    conflict_id bigserial PRIMARY KEY,
    idempotency_key varchar(600) NOT NULL,
    existing_fingerprint varchar(64) NOT NULL,
    incoming_fingerprint varchar(64) NOT NULL,
    fingerprint_algorithm varchar(80) NOT NULL,
    first_payload jsonb NULL,
    incoming_payload jsonb NOT NULL,
    differing_fields jsonb NOT NULL DEFAULT '[]'::jsonb,
    resolution_status varchar(50) NOT NULL DEFAULT 'UNRESOLVED',
    first_detected_at timestamptz NOT NULL DEFAULT now(),
    last_detected_at timestamptz NOT NULL DEFAULT now(),
    observation_count bigint NOT NULL DEFAULT 1,
    resolved_at timestamptz NULL,
    authoritative_fingerprint varchar(64) NULL,
    resolution_reason varchar(240) NULL,
    CONSTRAINT uq_hyperliquid_replica_payload_conflict_variant
        UNIQUE (idempotency_key, incoming_fingerprint),
    CONSTRAINT chk_hyperliquid_replica_payload_conflict_status
        CHECK (resolution_status IN (
            'UNRESOLVED',
            'AUTHORITATIVE_VARIANT_OBSERVED',
            'RESOLVED',
            'ESCALATED'
        ))
);

CREATE INDEX IF NOT EXISTS ix_hyperliquid_replica_payload_conflict_unresolved
    ON futuros_operaciones.hyperliquid_replica_payload_conflict (
        resolution_status, first_detected_at
    )
    WHERE resolution_status IN (
        'UNRESOLVED', 'AUTHORITATIVE_VARIANT_OBSERVED', 'ESCALATED'
    );

COMMENT ON TABLE futuros_operaciones.hyperliquid_replica_payload_conflict IS
    'Durable, sanitized evidence for conflicting Sentinel payloads. It never authorizes a second economic order.';
