# Validacion flujo copy trading - produccion

## Objetivo

Revisar que el runtime de copy trading no abra, aumente, reduzca, cierre o flipee posiciones sin una asignacion activa y sin respetar la estrategia seleccionada desde metricas.

## Flujo validado

### OPEN

- Requiere asignacion activa `user_copy_allocation` para `wallet + strategy`.
- Requiere metrica compatible con esa asignacion.
- Aplica guard de PnL/estado/cooldown antes de crear nueva exposicion.
- Si no existe asignacion real, se bloquea con `reasonCode=allocation_missing`.

### INCREASE / RESIZE hacia arriba

- Solo se permite si ya existe copia activa.
- Requiere asignacion activa real para la estrategia actual.
- No se permite aumentar usando solo metadata vieja guardada en `copy_operation`.
- Respeta minNotional y shadow/live.

### REDUCE / RESIZE hacia abajo

- Se permite como salida/gestion de una copia existente.
- Puede ejecutarse aunque la asignacion este en `exit_only` o bloqueada para nuevas entradas.
- No crea nueva exposicion.

### CLOSE

- Solo cierra si hay copia activa.
- Usa precio de referencia del evento origen cuando existe.
- Se permite aunque el guard bloquee nuevas entradas.

### FLIP

- Cierra/reduce la posicion copiada del lado anterior.
- Abre la nueva direccion solo si existe asignacion activa real y la estrategia lo permite.
- Si la estrategia activa no permite el nuevo lado, no abre exposicion nueva.

### PURE_OPEN_CLOSE

- Permite OPEN inicial de la misma operacion origen.
- Permite UPDATE del mismo origen para mantener la pierna sin cerrar por ruido/no-op.
- No permite RESIZE/FLIP como continuidad limpia; esos eventos sacan la posicion del patron puro.
- El CLOSE se atiende por copia activa, no abre exposicion.

### LONG_ONLY / SHORT_ONLY

- El router filtra targets por lado.
- El selector de metrica prioriza estrategias que tengan asignacion activa del usuario.
- Evita que una wallet duplicada en joyas seleccione una estrategia distinta a la que el usuario activo.

### SHADOW

- Usa el mismo camino logico que LIVE, pero genera respuesta dummy FILLED.
- Persiste `executionMode=SHADOW`, `isShadow=true` y `shadowStatus=FILLED`.
- No envia orden real a Binance.

## Cambios aplicados

1. `resolveWalletMetric` ahora prioriza metricas con asignacion abierta para el usuario, wallet y strategy.
2. Nueva exposicion en rebalance/open/reopen/increase requiere asignacion real con id.
3. `allocationAllowsNewExposure` ya no permite `allocation == null`.
4. `PURE_OPEN_CLOSE` permite `UPDATE` del mismo origen para no cerrar por actualizaciones sin resize.

## Validacion manual recomendada

Buscar logs:

- `event=copy_open_metric_lookup_hit` con `matchedWithOpenAllocation`.
- `event=copy.exposure.skip reasonCode=allocation_missing`.
- `event=copy.exposure.skip reasonCode=allocation_not_openable`.
- `event=rebalance.target.skip reason=strategy_filter`.
- `event=copy.shadow.order_filled mode=SHADOW`.

SQL util:

```sql
SELECT
    execution_mode,
    copy_strategy_code,
    count(*) AS rows_count,
    count(*) FILTER (WHERE is_active = true) AS active_rows
FROM futuros_operaciones.copy_operation
GROUP BY execution_mode, copy_strategy_code
ORDER BY rows_count DESC;
```

```sql
SELECT
    decision,
    decision_reason,
    execution_mode,
    copy_strategy_code,
    count(*) AS rows_count
FROM futuros_operaciones.copy_operation_event
WHERE event_time >= now() - interval '24 hours'
GROUP BY decision, decision_reason, execution_mode, copy_strategy_code
ORDER BY rows_count DESC;
```


## Hallazgos criticos corregidos en esta version

- `UPDATE`, `NO_CHANGE` y `UNKNOWN` ya no disparan orden de copy; solo registran/actualizan estado si corresponde en el pipeline de origen.
- `FLIP` ahora puede atender tanto a usuarios con copia activa del lado anterior como a usuarios con asignacion activa compatible para abrir la nueva direccion.
- Las referencias de precio para cierre usan `effectiveExitPrice` cuando viene normalizado desde Hyperliquid, antes de caer a `markPrice` o `entryPrice`.
- La nueva exposicion requiere una asignacion real activa; metadata vieja en `copy_operation` no puede abrir/aumentar por si sola.

## Limitacion intencional

El runtime actual mantiene una sola copia activa por `originId + userId + side`. Si en el futuro se quiere copiar la misma wallet y mismo origen varias veces para el mismo usuario con estrategias distintas en paralelo, hay que cambiar cache, idempotencia y el indice unico de `copy_operation` para incluir `user_copy_allocation_id`. En esta version eso queda bloqueado por diseno para evitar exposicion duplicada accidental.
