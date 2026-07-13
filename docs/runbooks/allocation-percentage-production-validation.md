# Runbook - allocation_pct SHADOW / MICRO_LIVE / LIVE

## Alcance

Validar `V202607110007__allocation_percentage_contract.sql` sin ejecutar ordenes
Binance. Todas las consultas de preflight son read-only. No aplicar updates
manuales: la clasificacion y correccion pertenecen exclusivamente a Flyway.

## Preflight read-only

```sql
-- MICRO_LIVE que cumplen toda la evidencia del sentinel historico.
select micro.id, micro.id_user, lower(micro.wallet_id) as wallet_lc,
       micro.copy_strategy_code, micro.scope_type, micro.scope_value,
       micro.allocation_pct, micro.linked_shadow_allocation_id,
       shadow.allocation_pct as shadow_allocation_pct,
       shadow.target_live_allocation_pct
from futuros_operaciones.user_copy_allocation micro
join futuros_operaciones.shadow_copy_allocation shadow
  on shadow.id = micro.linked_shadow_allocation_id
where micro.execution_mode = 'MICRO_LIVE'
  and micro.allocation_pct = 0.000001
  and coalesce(shadow.allocation_pct, 0) <= 0
  and shadow.target_live_allocation_pct is null
  and micro.promoted_from_shadow_at is not null;

-- Planes sentinel+100 asociados a esas allocations.
select plan.*
from futuros_operaciones.user_wallet_copy_plan plan
where plan.allocation_pct = 0.000001
  and plan.allocated_capital_usd = 100
  and exists (
      select 1
      from futuros_operaciones.user_copy_allocation micro
      join futuros_operaciones.shadow_copy_allocation shadow
        on shadow.id = micro.linked_shadow_allocation_id
      where micro.id_user = plan.id_user
        and lower(micro.wallet_id) = plan.wallet_lc
        and micro.execution_mode = 'MICRO_LIVE'
        and micro.allocation_pct = 0.000001
        and coalesce(shadow.allocation_pct, 0) <= 0
        and shadow.target_live_allocation_pct is null
  );

-- LIVE sospechoso solo cuando el valor coincide con procedencia MICRO legacy.
select live.id, live.id_user, lower(live.wallet_id) as wallet_lc,
       live.copy_strategy_code, live.scope_type, live.scope_value,
       live.allocation_pct, live.linked_shadow_allocation_id, live.status
from futuros_operaciones.user_copy_allocation live
where live.execution_mode = 'LIVE'
  and live.is_active = true
  and live.ends_at is null
  and lower(live.status) = 'active'
  and live.allocation_pct = 0.000001
  and (
      exists (
          select 1
          from futuros_operaciones.shadow_copy_allocation shadow
          where shadow.id = live.linked_shadow_allocation_id
            and coalesce(shadow.allocation_pct, 0) <= 0
            and shadow.target_live_allocation_pct is null
      )
      or exists (
          select 1
          from futuros_operaciones.user_wallet_copy_plan plan
          where plan.id_user = live.id_user
            and plan.wallet_lc = lower(live.wallet_id)
            and plan.allocation_pct = 0.000001
            and plan.allocated_capital_usd = 100
      )
  );

-- Sentinel ambiguo: se informa, pero no se convierte automaticamente.
select micro.*
from futuros_operaciones.user_copy_allocation micro
left join futuros_operaciones.shadow_copy_allocation shadow
  on shadow.id = micro.linked_shadow_allocation_id
where micro.execution_mode = 'MICRO_LIVE'
  and micro.allocation_pct = 0.000001
  and not (shadow.id is not null
           and coalesce(shadow.allocation_pct, 0) <= 0
           and shadow.target_live_allocation_pct is null
           and micro.promoted_from_shadow_at is not null);

-- Exposicion antes del despliegue.
select id_user, lower(wallet_id) as wallet_lc,
       sum(allocation_pct) filter (where execution_mode = 'LIVE') as live_pct,
       count(*) filter (where execution_mode = 'MICRO_LIVE') as micro_profiles
from futuros_operaciones.user_copy_allocation
where is_active = true and ends_at is null
group by id_user, lower(wallet_id)
order by id_user, wallet_lc;
```

## Post-deploy

```sql
-- Cero sentinel nuevo en MICRO_LIVE. Legacy ambiguo queda identificado aparte.
select count(*) as invalid_new_micro_rows
from futuros_operaciones.user_copy_allocation
where execution_mode = 'MICRO_LIVE'
  and allocation_pct = 0.000001
  and coalesce(allocation_pct_source, '') <> 'LEGACY_MICRO_PCT_IGNORED';

-- Contrato fijo MICRO_LIVE.
select count(*) as valid_micro_rows
from futuros_operaciones.user_copy_allocation
where execution_mode = 'MICRO_LIVE'
  and is_active = true and ends_at is null and lower(status) = 'active'
  and allocation_pct is null
  and sizing_mode = 'FIXED_CAPITAL'
  and allocation_pct_source = 'FIXED_MICRO_BUDGET';

-- Ninguna LIVE ejecutable sin porcentaje economico trazable.
select *
from futuros_operaciones.user_copy_allocation
where execution_mode = 'LIVE'
  and is_active = true and ends_at is null and lower(status) = 'active'
  and (allocation_pct is null or allocation_pct <= 0
       or sizing_mode <> 'PERCENTAGE'
       or allocation_pct_source is null);

-- Fuente y snapshot de cada LIVE actual.
select id, id_user, lower(wallet_id) as wallet_lc, copy_strategy_code,
       scope_type, scope_value, allocation_pct, wallet_total_allocation_pct,
       allocation_pct_source, allocation_pct_source_id,
       allocation_pct_calculated_at, allocation_pct_valid_until
from futuros_operaciones.user_copy_allocation
where execution_mode = 'LIVE' and is_active = true and ends_at is null;

-- Todo LIVE minimo debe tener procedencia economica demostrable; se informa
-- separado porque 0.000001 tambien puede ser un porcentaje real excepcional.
select id, id_user, lower(wallet_id) as wallet_lc, copy_strategy_code,
       scope_type, scope_value, status, allocation_pct_source,
       allocation_pct_source_id, allocation_pct_calculated_at
from futuros_operaciones.user_copy_allocation
where execution_mode = 'LIVE'
  and is_active = true and ends_at is null
  and allocation_pct = 0.000001
order by id_user, wallet_lc, copy_strategy_code, scope_type, scope_value;

-- Dos estrategias no pueden multiplicar el total de su wallet.
select detail.id_user, detail.wallet_lc,
       sum(strategy_allocation_pct) as strategy_sum,
       max(wallet_total_allocation_pct) as wallet_total,
       count(*) as strategy_count
from futuros_operaciones.live_allocation_distribution_detail detail
join futuros_operaciones.live_allocation_distribution_run run using (distribution_id)
where run.status = 'COMPLETED'
  and run.distribution_id = (
      select latest.distribution_id
      from futuros_operaciones.live_allocation_distribution_run latest
      where latest.id_user = run.id_user
      order by latest.calculated_at desc, latest.created_at desc
      limit 1
  )
group by detail.id_user, detail.wallet_lc
having sum(strategy_allocation_pct) > max(wallet_total_allocation_pct);

-- El ultimo snapshot por usuario debe estar COMPLETED y vigente.
select distinct on (id_user)
       id_user, distribution_id, status, reason_code,
       user_total_allocation_pct, calculated_at, valid_until
from futuros_operaciones.live_allocation_distribution_run
order by id_user, calculated_at desc, created_at desc;

-- Diagnostico de elegibilidad mode-aware equivalente al repositorio.
select execution_mode, count(*)
from futuros_operaciones.user_copy_allocation
where is_active = true and ends_at is null and lower(status) = 'active'
  and (execution_mode = 'MICRO_LIVE'
       or (execution_mode = 'LIVE' and coalesce(allocation_pct, 0) > 0))
group by execution_mode;
```

El presupuesto V3 se valida en runtime con las metricas y logs de
`CopyBudgetResolver` / `PostgresCopyDispatchIntentStore`: capital total 100 por
`user + wallet`, leverage 5x, sin margen fijo por operacion y sin maximo global
de posiciones. `userMaxConcurrentPositions` se valida solo cuando el usuario lo
configura. REDUCE/CLOSE no consumen una reserva nueva y permanecen permitidos.

## Alertas

- Alertar si aumenta `copy_legacy_allocation_sentinel_total` despues del deploy.
- Alertar si el ultimo run permanece `STAGED` o `FAILED` por mas de un ciclo.
- Alertar por `LIVE_ALLOCATION_PCT_TOTAL_EXCEEDED` o distribucion >1.
- No alertar por un missing transitorio: la promocion queda reintentable y
  MICRO_LIVE continua operativa.

## Rollback

1. No revertir constraints mientras existan binarios nuevos activos.
2. Un rollback de aplicacion conserva el fail-closed de LIVE y puede impedir que
   un binario antiguo escriba su sentinel; es preferible a reabrir el riesgo.
3. Restaurar filas solo desde backup/preflight y por identidad exacta.
4. Nunca inventar un porcentaje LIVE durante rollback.
