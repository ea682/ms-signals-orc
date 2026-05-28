ALTER TABLE futuros_operaciones.detail_user
    ADD COLUMN IF NOT EXISTS capital_asset varchar(4) NOT NULL DEFAULT 'USDT';

UPDATE futuros_operaciones.detail_user
SET capital_asset = 'USDT'
WHERE capital_asset IS NULL OR trim(capital_asset) = '';

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'chk_detail_user_capital_asset'
    ) THEN
        ALTER TABLE futuros_operaciones.detail_user
            ADD CONSTRAINT chk_detail_user_capital_asset
            CHECK (capital_asset IN ('USDT', 'USDC'));
    END IF;
END $$;

COMMENT ON COLUMN futuros_operaciones.detail_user.capital_asset IS
    'Moneda estable usada para capital, copytrading y mantenimiento automatico de BNB. Valores permitidos: USDT o USDC.';
