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
- Tiene cobertura suficiente.
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
- Usar capital inicial limitado por `COPY_MICRO_LIVE_INITIAL_CAPITAL_USD` y `COPY_MICRO_LIVE_MAX_CAPITAL_USD`.
- Mantener `linked_shadow_allocation_id`.
- Mantener `source_symbol`, `target_symbol`, `capital_asset` y estado de resolucion de simbolo.
- Marcar el SHADOW como enlazado con `linked_live_allocation_id`.
- Crear o actualizar `user_wallet_copy_plan` para la wallet del usuario usando la configuracion activa de `detail_user`.
- Escribir auditoria `MICRO_LIVE_CREATED` para la promocion efectiva SHADOW -> MICRO_LIVE.

La promocion es idempotente. Si una carrera de concurrencia crea primero la asignacion, el segundo intento no rompe el job: re-lee la asignacion existente, enlaza el SHADOW si hace falta y audita `SHADOW_PROMOTION_NOOP` con reason `ALREADY_PROMOTED`.

Si ya existe una asignacion MICRO_LIVE/LIVE abierta para la misma unidad estrategica, la idempotencia tiene prioridad sobre la validacion de capital actual: el SHADOW se enlaza como `ALREADY_PROMOTED` y no debe degradarse a `NO_CAPITAL_CONFIG`.

Si una carrera de concurrencia crea primero `user_wallet_copy_plan`, el promoter debe re-leerlo y continuar con la allocation MICRO_LIVE. Un plan existente valido debe auditarse/loguearse como reusado, no duplicarse.

### LIVE

LIVE queda protegido por la etapa MICRO_LIVE.

La implementacion actual promociona la misma asignacion abierta de `MICRO_LIVE` a `LIVE` porque el indice unico activo protege una sola fila abierta por usuario, wallet, estrategia y scope. Esto evita duplicidad de ejecucion.

Si se requiere en el futuro cerrar la fila MICRO_LIVE y crear una fila LIVE nueva, debe agregarse una migracion de ciclo de vida explicita y un vinculo historico adicional para no romper la unicidad ni duplicar ordenes.

## Reason codes obligatorios

- `SHADOW_NOT_READY_MIN_DAYS`
- `SHADOW_NOT_READY_MIN_EVENTS`
- `SHADOW_NOT_READY_MIN_CLOSED_POSITIONS`
- `SHADOW_NOT_READY_COVERAGE`
- `SHADOW_NOT_READY_COPY_GUARD`
- `SHADOW_NOT_READY_PNL`
- `SHADOW_NOT_READY_DRAWDOWN`
- `NO_ACTIVE_USER`
- `NO_ACTIVE_BINANCE_API_KEY`
- `CAPITAL_CONFIG_FOUND_FROM_USER_DETAIL`
- `CAPITAL_CONFIG_MISSING_FROM_USER_DETAIL`
- `COPY_PLAN_CREATED`
- `COPY_PLAN_ALREADY_EXISTS`
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
- `event=copy.promotion.shadow_to_micro.created`
- `event=copy.promotion.shadow_to_micro.noop`
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
