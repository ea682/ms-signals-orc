ALTER TABLE futuros_operaciones.hyperliquid_direct_ingest_dedupe
    ADD COLUMN IF NOT EXISTS payload_fingerprint varchar(64) NULL;

COMMENT ON COLUMN futuros_operaciones.hyperliquid_direct_ingest_dedupe.payload_fingerprint IS
    'SHA-256 of canonical non-sensitive event fields. Detects accidental idempotency-key reuse with a different payload.';
