-- Validate multi-instance protections before enabling more than one ms-signals-orc replica.
SELECT indexname, indexdef
FROM pg_indexes
WHERE schemaname = 'futuros_operaciones'
  AND indexname IN (
      'hyperliquid_direct_ingest_dedupe_pkey',
      'ux_copy_operation_origin_user_type',
      'ux_copy_operation_user_order_id',
      'ux_copy_operation_event_client_order_id',
      'ux_operation_movement_event_movement_key'
  )
ORDER BY indexname;

SELECT status, count(*)
FROM futuros_operaciones.hyperliquid_direct_ingest_dedupe
GROUP BY status
ORDER BY status;
