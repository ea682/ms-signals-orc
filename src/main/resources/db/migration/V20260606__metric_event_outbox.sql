CREATE TABLE IF NOT EXISTS futuros_operaciones.metric_event_outbox (
    id bigserial PRIMARY KEY,
    event_type text NOT NULL,
    aggregate_key text NOT NULL,
    kafka_key text NOT NULL,
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz,
    locked_at timestamptz,
    locked_by text,
    attempts integer NOT NULL DEFAULT 0,
    last_error text
);

ALTER TABLE futuros_operaciones.metric_event_outbox
    ADD COLUMN IF NOT EXISTS locked_at timestamptz;

ALTER TABLE futuros_operaciones.metric_event_outbox
    ADD COLUMN IF NOT EXISTS locked_by text;

CREATE INDEX IF NOT EXISTS ix_metric_event_outbox_pending
ON futuros_operaciones.metric_event_outbox (published_at, locked_at, created_at, id)
WHERE published_at IS NULL;

CREATE INDEX IF NOT EXISTS ix_metric_event_outbox_kafka_key_created
ON futuros_operaciones.metric_event_outbox (kafka_key, created_at DESC);
