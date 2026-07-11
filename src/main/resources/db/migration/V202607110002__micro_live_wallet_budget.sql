-- MICRO_LIVE has one shared 100 USDC budget per user + wallet, not per strategy.
SET lock_timeout = '60s';
SET statement_timeout = '15min';

UPDATE futuros_operaciones.copy_dispatch_intent cdi
SET wallet_id = lower(btrim(uca.wallet_id))
FROM futuros_operaciones.user_copy_allocation uca
WHERE cdi.user_copy_allocation_id = uca.id
  AND cdi.execution_mode = 'MICRO_LIVE'
  AND (cdi.wallet_id IS NULL OR btrim(cdi.wallet_id) = '');

-- A failed concurrent build leaves an index that exists but is not valid. Drop
-- both new indexes so retrying this non-transactional migration is deterministic.
DROP INDEX CONCURRENTLY IF EXISTS futuros_operaciones.ix_copy_dispatch_intent_micro_wallet_budget;
DROP INDEX CONCURRENTLY IF EXISTS futuros_operaciones.ix_copy_operation_micro_wallet_budget;

CREATE INDEX CONCURRENTLY ix_copy_dispatch_intent_micro_wallet_budget
    ON futuros_operaciones.copy_dispatch_intent (
        id_user, lower(wallet_id), execution_mode, reservation_status
    )
    INCLUDE (requested_margin_usd, reserved_position_count)
    WHERE execution_mode = 'MICRO_LIVE' AND reservation_status = 'PENDING';

CREATE INDEX CONCURRENTLY ix_copy_operation_micro_wallet_budget
    ON futuros_operaciones.copy_operation (
        id_user, lower(id_wallet_origin), execution_mode
    )
    INCLUDE (size_usd, leverage)
    WHERE execution_mode = 'MICRO_LIVE'
      AND is_active = true
      AND COALESCE(is_shadow, false) = false;
