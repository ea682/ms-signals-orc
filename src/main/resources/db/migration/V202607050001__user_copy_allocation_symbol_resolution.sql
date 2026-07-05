ALTER TABLE futuros_operaciones.user_copy_allocation
    ADD COLUMN IF NOT EXISTS source_symbol text,
    ADD COLUMN IF NOT EXISTS target_symbol text,
    ADD COLUMN IF NOT EXISTS capital_asset varchar(4),
    ADD COLUMN IF NOT EXISTS resolved_quote_asset varchar(8),
    ADD COLUMN IF NOT EXISTS symbol_resolution_status varchar(32),
    ADD COLUMN IF NOT EXISTS symbol_resolution_reason varchar(80);
