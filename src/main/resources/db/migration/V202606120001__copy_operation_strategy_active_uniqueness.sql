-- Permite que un mismo usuario copie el mismo origin/symbol/side con estrategias distintas,
-- pero evita duplicar la misma copia activa dentro de la misma estrategia.
-- Legacy: ux_copy_operation_origin_user_type bloqueaba wallet+usuario+lado sin considerar estrategia.
DROP INDEX IF EXISTS futuros_operaciones.ux_copy_operation_origin_user_type;
DROP INDEX IF EXISTS futuros_operaciones.ux_copy_operation_origin_user_type_strategy_active;

UPDATE futuros_operaciones.copy_operation
SET copy_strategy_code = 'MOVEMENT_ALL'
WHERE copy_strategy_code IS NULL OR btrim(copy_strategy_code) = '';

CREATE UNIQUE INDEX IF NOT EXISTS ux_copy_operation_origin_user_type_strategy_active
    ON futuros_operaciones.copy_operation (
        id_order_origin,
        id_user,
        type_operation,
        COALESCE(copy_strategy_code, 'MOVEMENT_ALL')
    )
    WHERE is_active = true;

CREATE INDEX IF NOT EXISTS ix_copy_operation_origin_user_strategy_time
    ON futuros_operaciones.copy_operation (
        id_order_origin,
        id_user,
        COALESCE(copy_strategy_code, 'MOVEMENT_ALL'),
        date_creation DESC
    );
