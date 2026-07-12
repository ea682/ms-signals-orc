# SDD - Copy Promotion SHADOW to MICRO_LIVE to LIVE

## Objetivo

Cerrar el flujo productivo de copy trading por estrategia:

`wallet_id + copy_strategy_code + scope_type + scope_value`

La wallet no es la unidad real de decision. Cada estrategia se evalua, audita y promueve de forma independiente.

## Contrato productivo

### SHADOW

SHADOW no envia ordenes reales. Sirve para juntar evidencia por estrategia antes de arriesgar capital del usuario.

Un SHADOW puede pasar a MICRO_LIVE solo si:

- La promocion SHADOW esta habilitada.
- La asignacion SHADOW esta activa, sin `ends_at` y sin `linked_live_allocation_id`.
- Tiene antiguedad minima, eventos minimos y posiciones cerradas minimas.
- Tiene cobertura suficiente segun `shadow-promotion-rolling-coverage-sdd.md`: rolling por allocation cuando el modo es `ROLLING`, historica solo en `LEGACY/AUDIT`.
- Cumple PnL/drawdown si esas politicas estan habilitadas.
- `copy_guard_status` no esta bloqueado y `copy_guard_action` no bloquea entrada.
- Existe usuario activo.
- Existe API Binance activa con key y secret.
- Existe capital, `capital_asset` y `max_wallet` configurados en `detail_user`.
- El usuario no supero `max_wallet` contando asignaciones activas LIVE/MICRO_LIVE.
- El simbolo origen resuelve a un simbolo target disponible en Binance.
- No existe ya una asignacion abierta LIVE/MICRO_LIVE para la misma unidad estrategica.
- `user_wallet_copy_plan` puede estar vacio en la primera promocion. Ese plan es salida de la promocion, no requisito previo.
- `shadow_copy_allocation.allocation_pct` vacio o cero no bloquea por si solo la primera promocion si `detail_user` tiene capital valido. En ese caso se usa una asignacion minima runtime para crear el plan y la allocation MICRO_LIVE, manteniendo el capital real limitado por la politica MICRO_LIVE.

Si alguna regla falla, se escribe auditoria con `decision=SHADOW_PROMOTION_REJECTED`.

### MICRO_LIVE

MICRO_LIVE ejecuta ordenes reales con capital acotado. Es la etapa de validacion real antes de LIVE.

La asignacion creada desde SHADOW debe:

- Usar `execution_mode=MICRO_LIVE`.
- Quedar ejecutable con `status=ACTIVE`, `is_active=true` y `ends_at=null`.
- Usar un `copy_mode` permitido por `chk_user_copy_allocation_copy_mode`.
- Resolver `copy_mode` desde `copy_strategy_code` y solo usar `sourceCopyMode` como compatibilidad controlada. Nunca debe persistir `SKIP`, `copy_movement_all_events`, `copy_short_events`, `copy_long_events` ni otro valor que no pertenezca al set permitido por DB.
- Usar capital inicial limitado por `COPY_MICRO_LIVE_INITIAL_CAPITAL_USD` y `COPY_MICRO_LIVE_MAX_CAPITAL_USD`.
- Mantener `linked_shadow_allocation_id`.
- Mantener `source_symbol`, `target_symbol`, `capital_asset` y estado de resolucion de simbolo.
- Marcar el SHADOW como enlazado con `linked_live_allocation_id`.
- No escribir `PROMOTED_TO_MICRO_LIVE` en `shadow_copy_allocation.status`, porque produccion no lo permite en `chk_shadow_copy_allocation_status`.
- Registrar la promocion SHADOW -> MICRO_LIVE en `last_validation_reason`, `copy_promotion_audit` y logs estructurados. El `status` de SHADOW debe quedar en un valor permitido por DB, por ejemplo `SHADOW_VALIDATED`.
- Crear o actualizar `user_wallet_copy_plan` para la wallet del usuario usando la configuracion activa de `detail_user`.
- Escribir auditoria `MICRO_LIVE_CREATED` para la promocion efectiva SHADOW -> MICRO_LIVE.

`SHADOW_ONLY` / `SUMMARY_NOT_FINAL_LIVE_BLOCKED` bloquea LIVE directo, pero no debe pausar una allocation `MICRO_LIVE` ya creada desde una promocion SHADOW validada para MICRO_LIVE. Si el sync de `/joyas` ve `shadow_live_validation_failed action=SHADOW_ONLY status=SHADOW_ONLY`, debe mantener o reactivar la allocation `MICRO_LIVE` enlazada al SHADOW, con reason `MICRO_LIVE_NOT_PAUSED_BY_SHADOW_ONLY`. Riesgos reales como `DATA_RISK`, `PAUSE_OPEN`, usuario/API/capital invalido o hard blockers siguen pudiendo pausar o bloquear segun corresponda.

#### Sizing MICRO_LIVE

MICRO_LIVE no usa `allocation_pct` para definir el tamano real de orden.

El presupuesto real de MICRO_LIVE se resuelve con `budgetMode=FIXED_USD`:

- `budgetUsd = 100 USDC`; no se reduce silenciosamente por saldo.
- El limite productivo obligatorio es `copy.micro-live.max-capital-usd=100`.
- `allocation_pct` puede seguir existiendo por compatibilidad/constraints, pero no define sizing real para MICRO_LIVE.
- Si `accountCapitalUsd < 100`, se bloquea con `MICRO_LIVE_INSUFFICIENT_AVAILABLE_BALANCE`.

Reason codes:

- `MICRO_LIVE_FIXED_BUDGET_USD`
- `MICRO_LIVE_BUDGET_APPLIED`
- `MICRO_LIVE_NOT_USING_ALLOCATION_PCT`
- `MICRO_LIVE_INSUFFICIENT_AVAILABLE_BALANCE`

La promocion es idempotente. Si una carrera de concurrencia crea primero la asignacion, el segundo intento no rompe el job: re-lee la asignacion existente, enlaza el SHADOW si hace falta y audita `SHADOW_PROMOTION_NOOP` con reason `ALREADY_PROMOTED`.

Si ya existe una asignacion MICRO_LIVE/LIVE abierta para la misma unidad estrategica, la idempotencia tiene prioridad sobre la validacion de capital actual: el SHADOW se enlaza como `ALREADY_PROMOTED` y no debe degradarse a `NO_CAPITAL_CONFIG`.

Si una carrera de concurrencia crea primero `user_wallet_copy_plan`, el promoter debe re-leerlo y continuar con la allocation MICRO_LIVE. Un plan existente valido debe auditarse/loguearse como reusado, no duplicarse.

Valores permitidos para `user_copy_allocation.copy_mode`:

- `copy_all_metric_movements`
- `copy_only_short_events`
- `copy_only_long_events`
- `copy_open_and_full_close_only`
- `copy_first_open_final_close`
- `copy_strategy_filtered_events`
- `copy_only_flip_events`

Mapeo productivo:

- `MOVEMENT_ALL` -> `copy_all_metric_movements`
- `SHORT_ONLY` -> `copy_only_short_events`
- `LONG_ONLY` -> `copy_only_long_events`
- `OPEN_CLOSE_ONLY`, `OPEN_AND_FULL_CLOSE_ONLY`, `PURE_OPEN_CLOSE` -> `copy_open_and_full_close_only`
- `FIRST_OPEN_FINAL_CLOSE` -> `copy_first_open_final_close`
- `FLIP_ONLY` -> `copy_only_flip_events`
- Estrategias filtradas soportadas, por ejemplo `SYMBOL_SPECIALIST`, `LOW_LEVERAGE_ONLY`, `TOP_SYMBOLS_ONLY`, `MAJORS_ONLY`, `HIGH_LIQUIDITY_ONLY`, `HIGH_QUALITY_SYMBOLS_ONLY`, `SWING_ONLY` -> `copy_strategy_filtered_events`

Si la estrategia no es reconocida y el `sourceCopyMode` tampoco puede mapearse a un valor permitido, la candidata debe rechazarse con `INVALID_COPY_MODE_MAPPING` y el batch debe continuar.

### SHADOW -> LIVE directo

El flujo por defecto es siempre `SUMMARY -> SHADOW -> MICRO_LIVE -> LIVE`.

`SHADOW -> LIVE` directo queda bloqueado por defecto con `directLivePolicy=REQUIRE_MICRO_LIVE`.

Solo se permite intentar `SHADOW -> LIVE` si:

- `copy.promotion.default-target-mode=LIVE`.
- `copy.promotion.direct-live-policy=ALLOW_DIRECT_LIVE_FOR_LIVE_READY`.
- La asignacion SHADOW no viene de summary puro y tiene evidencia runtime suficiente.
- `last_validation_reason` indica una validacion LIVE explicita, por ejemplo `LIVE_READY_FROM_SHADOW` o `shadow_filters_passed`.
- Pasa los mismos gates de usuario, capital, API, max wallet, copy guard, symbol resolver y `copy_mode` seguro.

Si la policy no lo permite, se rechaza con `MICRO_LIVE_REQUIRED_BY_POLICY` o `DIRECT_LIVE_DISABLED_BY_POLICY`. Si la policy lo permite pero la evidencia no alcanza para LIVE, se rechaza con `LIVE_NOT_READY_FROM_SHADOW`.

Nunca se permite `SUMMARY -> LIVE`.

### LIVE

LIVE queda protegido por la etapa MICRO_LIVE.

La implementacion actual promociona la misma asignacion abierta de `MICRO_LIVE` a `LIVE` porque el indice unico activo protege una sola fila abierta por usuario, wallet, estrategia y scope. Esto evita duplicidad de ejecucion.

#### Sizing LIVE

LIVE si usa `allocation_pct`.

El presupuesto real de LIVE se resuelve con `budgetMode=WEIGHTED_PERCENTAGE`:

- `budgetUsd = accountCapitalUsd * allocationPct`.
- `allocationPct` viene de ranking/score/peso y representa distribucion del capital LIVE disponible.
- Dos wallets LIVE con `allocation_pct=0.51` y `allocation_pct=0.49` sobre `capital=1000` reciben `510` y `490`.
- LIVE nunca se limita a 100 USD solo por ser copy trading.

Reason code:

- `LIVE_WEIGHTED_ALLOCATION_PCT`

Si se requiere en el futuro cerrar la fila MICRO_LIVE y crear una fila LIVE nueva, debe agregarse una migracion de ciclo de vida explicita y un vinculo historico adicional para no romper la unicidad ni duplicar ordenes.

### Capital Binance Futures

`detail_user.capital` representa el `availableBalance` del `capital_asset` configurado en USD-M Futures.

La columna actual esta modelada como entero (`Integer` en la entidad), por lo tanto el valor persistido se redondea hacia abajo. Ejemplo: si Binance devuelve `availableBalance=896.65303430` para `USDC`, se persiste `capital=896` y se loguea el decimal completo como `availableBalance`.

El scheduler de capital:

- Corre con `futures.capital-maintenance.fixed-delay-ms=600000` por defecto.
- Selecciona usuarios activos con API Binance activa.
- Consulta el asset configurado en `capital_asset`.
- No pisa capital con cero ante errores de Binance.
- Actualiza cache runtime para que copy-trading use el valor nuevo sin esperar TTL.
- Loguea `oldCapital`, `newCapital`, `walletBalance`, `availableBalance`, `marginBalance`, `endpoint`, `decision` y `reasonCode`.

## Reason codes obligatorios

- `SHADOW_NOT_READY_MIN_DAYS`
- `SHADOW_NOT_READY_MIN_EVENTS`
- `SHADOW_NOT_READY_MIN_CLOSED_POSITIONS`
- `SHADOW_NOT_READY_COVERAGE`
- `SHADOW_COVERAGE_ROLLING_READY`
- `SHADOW_COVERAGE_ROLLING_BELOW_THRESHOLD`
- `SHADOW_COVERAGE_ROLLING_INSUFFICIENT_SAMPLE`
- `SHADOW_COVERAGE_ROLLING_NO_EVENTS`
- `SHADOW_COVERAGE_ROLLING_QUERY_FAILED`
- `SHADOW_NOT_READY_COPY_GUARD`
- `SHADOW_NOT_READY_PNL`
- `SHADOW_NOT_READY_DRAWDOWN`
- `NO_ACTIVE_USER`
- `NO_ACTIVE_BINANCE_API_KEY`
- `CAPITAL_CONFIG_FOUND_FROM_USER_DETAIL`
- `CAPITAL_CONFIG_MISSING_FROM_USER_DETAIL`
- `COPY_PLAN_CREATED`
- `COPY_PLAN_ALREADY_EXISTS`
- `COPY_MODE_RESOLVED`
- `COPY_MODE_MAPPING_FALLBACK`
- `COPY_MODE_CONSTRAINT_SAFE`
- `INVALID_COPY_MODE_MAPPING`
- `DIRECT_LIVE_DISABLED_BY_POLICY`
- `DIRECT_LIVE_ALLOWED_BY_POLICY`
- `MICRO_LIVE_REQUIRED_BY_POLICY`
- `LIVE_READY_FROM_SHADOW`
- `LIVE_NOT_READY_FROM_SHADOW`
- `LIVE_ALLOCATION_CREATED`
- `LIVE_ALLOCATION_ALREADY_EXISTS`
- `MICRO_TO_LIVE_COPY_MODE_RESOLVED`
- `MICRO_LIVE_VALIDATION_PASSED`
- `MICRO_LIVE_CREATED`
- `MICRO_LIVE_ALREADY_EXISTS`
- `MICRO_LIVE_ACTIVATED`
- `MICRO_LIVE_NOT_PAUSED_BY_SHADOW_ONLY`
- `MICRO_LIVE_FIXED_BUDGET_USD`
- `MICRO_LIVE_BUDGET_APPLIED`
- `MICRO_LIVE_NOT_USING_ALLOCATION_PCT`
- `MICRO_LIVE_INSUFFICIENT_AVAILABLE_BALANCE`
- `LIVE_WEIGHTED_ALLOCATION_PCT`
- `SHADOW_PROMOTION_STATUS_RECORDED`
- `SHADOW_PROMOTION_STATUS_SKIPPED_INVALID_DB_STATUS`
- `PROMOTED_TO_MICRO_LIVE_RECORDED_AS_REASON`
- `SHADOW_STATUS_CONSTRAINT_SAFE`
- `MICRO_LIVE_SHADOW_VALIDATION_FAILED`
- `MICRO_LIVE_PAUSED_BY_REAL_RISK`
- `CAPITAL_REFRESH_FAILED`
- `SKIPPED_API_KEY_INACTIVE`
- `MICRO_LIVE_ALLOCATION_CREATED`
- `MICRO_LIVE_ALLOCATION_ALREADY_EXISTS`
- `NO_CAPITAL_CONFIG`
- `MAX_WALLET_REACHED`
- `SYMBOL_TARGET_NOT_AVAILABLE`
- `SYMBOL_RESOLVER_FAILED`
- `ALREADY_PROMOTED`
- `PROMOTION_FAILED`

## Observabilidad

Eventos de promocion:

- `event=copy.promotion.shadow_to_micro.started`
- `event=copy.promotion.shadow_candidate.evaluated`
- `event=copy.promotion.shadow_to_micro.rejected`
- `event=copy.promotion.copy_mode.resolved`
- `event=copy.promotion.micro_to_live.copy_mode.resolved`
- `event=copy.promotion.direct_live.policy_checked`
- `event=copy.promotion.shadow_to_live.created`
- `event=copy.promotion.shadow_to_live.rejected`
- `event=copy.promotion.micro_to_live.created`
- `event=copy.promotion.micro_to_live.rejected`
- `event=copy.promotion.shadow_to_micro.created`
- `event=copy.promotion.shadow_to_micro.noop`
- `event=copy.promotion.shadow_post_promote.status_safe`
- `event=copy.promotion.micro_live.validation.passed`
- `event=copy.promotion.micro_live.not_paused_by_shadow_only`
- `event=user_copy_allocation.micro_live.activated`
- `event=user_copy_allocation.micro_live.paused`
- `event=copy.budget.resolved`
- `event=futures.capital_maintenance.started`
- `event=futures.capital_maintenance.user.selected`
- `event=futures.capital_maintenance.binance.balance.fetched`
- `event=futures.capital_maintenance.capital.updated`
- `event=futures.capital_maintenance.skipped`
- `event=futures.capital_maintenance.failed`
- `event=copy.promotion.shadow_to_micro.candidate_failed`
- `event=copy.promotion.shadow_to_micro.finished`
- `event=copy.promotion.micro_to_live.created`
- `event=copy.promotion.micro_to_live.finished`

Eventos de ejecucion/SLI:

- `event=copy.execution.order_submitted`
- `event=copy.execution.order_ack`
- `event=copy.execution.order_filled`
- `event=copy.latency.measured`
- `event=copy.slippage.measured`

Los logs de latencia/slippage permiten SLI online en Loki/Grafana. Para reconstruccion historica SQL completa todavia se requiere persistir tiempos y precios de extremo a extremo en una tabla dedicada o extender `copy_operation_event`.

## Validacion post-deploy

Despues de desplegar el artefacto con Flyway habilitado, ejecutar:

`src/main/resources/db/validation/shadow_micro_live_promotion_validation.sql`

La validacion debe confirmar:

- Tablas `user_wallet_copy_plan` y `copy_promotion_audit` existen.
- Indices de promocion existen.
- No hay duplicados activos LIVE/MICRO_LIVE para la misma unidad estrategica.
- No hay SHADOW enlazado a una asignacion inexistente.
- El wallet objetivo muestra razon de rechazo/promocion en `copy_promotion_audit` o `last_validation_reason`.
