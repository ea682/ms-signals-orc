# SDD - Runtime MICRO_LIVE/LIVE Hot Path (V2, Superseded)

> Superseded on 2026-07-13 by
> `copy-trading-proportional-portfolio-v3-sdd.md`. In particular, the fixed
> USD 20 per-operation budget, global five-position limit, source-margin
> exposure formula and USD 1,000 equity fallback below are historical rules
> and MUST NOT be used by V3 runtime.

## Problema

Una allocation real ya promovida a `MICRO_LIVE` o `LIVE` puede volver a ser filtrada en el runtime por el guard historico de metricas (`SHADOW_ONLY`, `SUMMARY_NOT_FINAL_LIVE_BLOCKED`, `METRIC_COPY_GUARD_PAUSE_OPEN`). Eso rompe el flujo:

`SUMMARY -> SHADOW -> MICRO_LIVE -> LIVE`

El caso observado fue la allocation `505` (`MICRO_LIVE`, `MOVEMENT_ALL`, linked shadow `18`), donde SHADOW registro un OPEN real, pero el hot path dejo `eligibleUsers=0` por `METRIC_COPY_GUARD_PAUSE_OPEN`.

## Regla de Negocio

La unidad real de copia es la allocation activa, no el perfil summary original.

Una `user_copy_allocation` con:

- `execution_mode in ('MICRO_LIVE', 'LIVE')`
- `status = ACTIVE`
- `is_active = true`
- `ends_at is null`
- promocion real registrada (`linked_shadow_allocation_id` o `promoted_from_shadow_at`)

no puede ser bloqueada solamente por `SHADOW_ONLY`, `SUMMARY_NOT_FINAL_LIVE_BLOCKED` o `METRIC_COPY_GUARD_PAUSE_OPEN`.

Esos estados solo bloquean:

- LIVE directo sin promocion valida.
- Promociones no validadas.
- Entradas sin allocation real promovida.

## Hot Path

El hot path de copia debe usar datos runtime:

`Evento Hyperliquid -> CandidateResolver cache -> Guard runtime cache -> Budget/Sizing cache -> Dispatch Binance async`

Para `LIVE` no debe introducir llamadas a DB ni llamadas REST lentas dentro de candidate/guard/budget. Los refresh de allocations, capital, symbols y posiciones activas deben ocurrir fuera del evento de mercado.

## MICRO_LIVE Sizing

`MICRO_LIVE` es prueba con presupuesto acotado:

- `total-capital-usd = 100`
- `max-margin-per-operation-usd = 20`
- `max-concurrent-positions = 5`
- `margin-buffer-pct = 0.03`

Reglas:

- No usa `allocation_pct` para reducir el tamano por operacion.
- No usa 100 USDC por operacion.
- `copyMarginUsd <= 20`.
- `openMarginUsedUsd + copyMarginUsd <= 100`.
- `openPositionsCount < 5`.
- Sin margen restante: `MICRO_LIVE_TOTAL_MARGIN_EXCEEDED`.
- Con cinco posiciones/reservas: `MICRO_LIVE_MAX_CONCURRENT_POSITIONS_EXCEEDED`.
- Con saldo disponible menor a 100 USDC: `MICRO_LIVE_INSUFFICIENT_AVAILABLE_BALANCE`.
- Si falta exposure de origen, puede seguir con fixed-per-operation.

## LIVE Sizing

`LIVE` copia porcentaje de exposicion de la wallet origen aplicado al capital asignado del usuario:

`sourceExposurePct = sourcePositionMarginUsd / sourceAccountEquityUsd`

`copyMarginUsd = allocatedCapitalUsd * sourceExposurePct`

Reglas:

- No usa 20 USDC fijo.
- No usa 100 USDC fijo.
- No copia monto absoluto de la wallet origen.
- Si falta exposure confiable: `SOURCE_EXPOSURE_DATA_MISSING`.
- Si falta capital asignado: `LIVE_ALLOCATED_CAPITAL_MISSING`.

## Guard Runtime

Para `MICRO_LIVE` promovido:

- Ignorar `SHADOW_ONLY`, `SUMMARY_NOT_FINAL_LIVE_BLOCKED`, `METRIC_COPY_GUARD_PAUSE_OPEN`.
- Bloquear riesgos reales: `DISABLED`, `BLOCKED`, `DATA_RISK_CRITICAL`, `MANUAL_PAUSE`, `USER_DISABLED`, `CAPITAL_MISSING`, `SYMBOL_UNAVAILABLE`.

Para `LIVE` promovido:

- Ignorar `SHADOW_ONLY`, `SUMMARY_NOT_FINAL_LIVE_BLOCKED`, `METRIC_COPY_GUARD_PAUSE_OPEN`.
- Bloquear riesgos reales: los anteriores mas `SOURCE_EXPOSURE_DATA_MISSING`.

## Logs Requeridos

- `copy.candidate.resolve.started`
- `copy.candidate.resolve.allocation_seen`
- `copy.candidate.resolve.allocation_matched`
- `copy.candidate.resolve.allocation_filtered`
- `copy.candidate.resolve.finished`
- `copy.guard.decision`
- `copy.budget.resolved`
- `copy.dispatch.order_intent`
- `copy.dispatch.order_submitted`
- `copy.dispatch.order_rejected`
- `user_copy_allocation.runtime_cache.invalidated`
- `user_copy_allocation.runtime_cache.refreshed`

## Persistencia de Rechazos

Una allocation real activa que fue evaluada y rechazada no puede desaparecer solo en logs. Debe registrarse en `copy_operation_event` con:

- `user_copy_allocation_id`
- `execution_mode`
- `wallet_id`
- `symbol`
- `side`
- `event_type`
- `source_order_id / origin_id`
- `decision = REJECT/SKIP`
- `reason_code`
- `event_time`
- `date_creation`

## Criterios de Aceptacion

- Allocation `505` MICRO_LIVE ACTIVE matchea eventos OPEN LONG `MOVEMENT_ALL`.
- Allocation `506` SHORT_ONLY no matchea OPEN LONG.
- Allocation `506` SHORT_ONLY matchea OPEN SHORT.
- MICRO_LIVE promovido ignora `SHADOW_ONLY/PAUSE_OPEN` del guard summary.
- LIVE promovido ignora `SHADOW_ONLY/PAUSE_OPEN` del guard summary.
- LIVE directo sin promocion sigue bloqueado por `SHADOW_ONLY`.
- MICRO_LIVE usa max 20 USDC de margen por operacion y max 100 USDC total.
- LIVE usa exposure porcentual de origen.
- LIVE rechaza si no hay exposure confiable.
- Candidate/guard/budget del hot path no hacen mutacion de DB para pausar una allocation real por `SHADOW_ONLY`.
