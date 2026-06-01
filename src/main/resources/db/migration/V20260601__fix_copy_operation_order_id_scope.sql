-- Safety fix for early HA package candidates.
-- Do not enforce Binance id_orden globally because order ids may collide across copied user accounts.
DROP INDEX IF EXISTS futuros_operaciones.ux_copy_operation_order_id;

CREATE UNIQUE INDEX IF NOT EXISTS ux_copy_operation_user_order_id
    ON futuros_operaciones.copy_operation (id_user, id_orden)
    WHERE id_orden IS NOT NULL;
