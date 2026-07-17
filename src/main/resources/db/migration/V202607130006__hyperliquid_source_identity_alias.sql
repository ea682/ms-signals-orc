-- Seed the source-identity key used by replica-safe Hyperliquid ingest from the
-- legacy key format. Legacy rows are retained for audit and in-flight workers.
-- The canonical alias makes an already-processed Kafka replay a duplicate when
-- Signals is deployed before both Hyperliquid replicas are rolled.

WITH legacy_claims AS (
    SELECT
        d.*,
        'hyperliquid:' || d.wallet || ':' || d.symbol || ':' || d.side || ':' || d.delta_type || ':' AS legacy_prefix
    FROM futuros_operaciones.hyperliquid_direct_ingest_dedupe d
    WHERE d.wallet IS NOT NULL
      AND d.symbol IS NOT NULL
      AND d.side IS NOT NULL
      AND d.delta_type IS NOT NULL
),
parsed_claims AS (
    SELECT
        l.*,
        substring(l.idempotency_key FROM char_length(l.legacy_prefix) + 1) AS external_id
    FROM legacy_claims l
    WHERE lower(left(l.idempotency_key, char_length(l.legacy_prefix))) = lower(l.legacy_prefix)
),
canonical_claims AS (
    SELECT
        p.*,
        CASE
            WHEN p.external_id LIKE 'snapshot-recovery-open|%' THEN
                'hyperliquid:recovery:' || lower(p.wallet) || ':' || upper(p.symbol) || ':' || p.external_id
            WHEN lower(left(
                    p.external_id,
                    char_length(p.wallet || '|' || p.symbol || '|' || p.delta_type || '|')
                 )) = lower(p.wallet || '|' || p.symbol || '|' || p.delta_type || '|') THEN
                'hyperliquid:trade:' || lower(p.wallet) || ':' || upper(p.symbol) || ':' ||
                substring(
                    p.external_id
                    FROM char_length(p.wallet || '|' || p.symbol || '|' || p.delta_type || '|') + 1
                )
            ELSE NULL
        END AS canonical_key
    FROM parsed_claims p
)
INSERT INTO futuros_operaciones.hyperliquid_direct_ingest_dedupe (
    idempotency_key,
    dedupe_key,
    position_key,
    wallet,
    symbol,
    side,
    delta_type,
    source_ts_ms,
    payload_fingerprint,
    status,
    attempt_count,
    duplicate_count,
    first_seen_at,
    last_seen_at,
    lease_until,
    processed_at,
    failed_at,
    last_reason_code,
    last_error_class,
    last_error_message
)
SELECT
    c.canonical_key,
    c.dedupe_key,
    c.position_key,
    c.wallet,
    c.symbol,
    c.side,
    c.delta_type,
    c.source_ts_ms,
    c.payload_fingerprint,
    c.status,
    c.attempt_count,
    c.duplicate_count,
    c.first_seen_at,
    c.last_seen_at,
    c.lease_until,
    c.processed_at,
    c.failed_at,
    'CANONICAL_ALIAS_MIGRATED_FROM_LEGACY_KEY',
    c.last_error_class,
    c.last_error_message
FROM canonical_claims c
WHERE c.canonical_key IS NOT NULL
  AND c.canonical_key <> c.idempotency_key
ORDER BY c.first_seen_at, c.idempotency_key
ON CONFLICT (idempotency_key) DO NOTHING;

