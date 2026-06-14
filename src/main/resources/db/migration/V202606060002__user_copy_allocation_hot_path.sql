CREATE INDEX IF NOT EXISTS ix_user_copy_allocation_wallet_active_users
ON futuros_operaciones.user_copy_allocation (lower(wallet_id), id_user)
WHERE ends_at IS NULL
  AND is_active = true
  AND lower(status) = 'active';

CREATE INDEX IF NOT EXISTS ix_user_copy_allocation_user_active_wallets
ON futuros_operaciones.user_copy_allocation (id_user, lower(wallet_id))
WHERE ends_at IS NULL
  AND is_active = true
  AND lower(status) = 'active';

CREATE INDEX IF NOT EXISTS ix_user_copy_allocation_user_wallet_any_status
ON futuros_operaciones.user_copy_allocation (id_user, lower(wallet_id));

CREATE INDEX IF NOT EXISTS ix_copy_operation_event_user_wallet_time
ON futuros_operaciones.copy_operation_event (id_user, lower(id_wallet_origin), event_time DESC);

CREATE INDEX IF NOT EXISTS ix_copy_operation_event_wallet_time
ON futuros_operaciones.copy_operation_event (lower(id_wallet_origin), event_time DESC);

CREATE INDEX IF NOT EXISTS ix_metric_event_outbox_event_pending
ON futuros_operaciones.metric_event_outbox (event_type, created_at, id)
WHERE published_at IS NULL;
