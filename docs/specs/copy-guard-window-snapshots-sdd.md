# SDD - Copy Guard Window Snapshots

## Problema

`ms-signals-orc` necesita ventanas de riesgo (`1d`, `3d`, `1w`, `2w`, `1mo`, `2mo`, `3mo`, `all`) para decidir `ALLOW`, `REDUCE_CAPITAL`, `PAUSE_OPEN`, `SHADOW_ONLY` y `DISABLED`, pero no puede volver a pedir `simulation=full` ni recalcular simulaciones historicas en el hot path de copy trading.

## Decision Arquitectonica

La fuente de ventanas vive en `ms-metrica-cuenta`, porque ese servicio ya posee el read-model historico y la matematica de simulacion. El contrato nuevo es un snapshot liviano:

`GET /operaciones/metrica/copy-guard/windows?limit=...&dayz=...&mode=snapshot`

El endpoint devuelve solo datos compactos de copy guard por:

`walletId + copyStrategyCode + scopeType + scopeValue`

`ms-signals-orc` consume ese endpoint en refresh/cache, no en el flujo caliente de orden. La ruta caliente queda:

`Hyperliquid event -> allocation cache -> copy guard snapshot cache -> sizing -> Binance async`

## Reglas

- `/operaciones/metrica/joyas?simulation=summary` no cambia y no debe usar `full`.
- El snapshot se calcula en batch por grupos de estrategia, no por wallet individual.
- `metric-wallet.copy-guard.windows` define que ventanas son obligatorias para decidir.
- `metric-wallet.copy-guard.available-windows` define que ventanas se materializan.
- `require-window-data=true` bloquea nuevas aperturas si falta una ventana requerida.
- `require-window-data=false` permite continuar con warning.
- Snapshot stale se trata como `STALE_COPY_GUARD_SNAPSHOT` si supera `stale-snapshot-max-age`.
- `1w` negativo reduce capital.
- `2w` negativo pausa nuevas aperturas.
- `1mo`/`2mo` negativo mueve a shadow-only.
- `3mo` negativo deshabilita/manual review.
- `all` negativo reduce o pausa segun umbral.
- `MICRO_LIVE` promovido desde SHADOW no debe bloquearse solo por `SHADOW_ONLY` historico.
- `LIVE` promovido tampoco debe bloquearse por `SHADOW_ONLY` historico; riesgos reales siguen bloqueando.

## Payload Compacto

Cada snapshot contiene:

- `walletId`
- `copyStrategyCode`
- `scopeType`
- `scopeValue`
- `status`
- `action`
- `allowNewEntries`
- `capitalMultiplier`
- `reasons`
- `windows`
- `computedAt`
- `expiresAt`
- `sourceVersion`

Cada ventana contiene:

- `complete`
- `operations`
- `closedOperations`
- `pnlGrossUsd`
- `feesUsd`
- `slippageUsd`
- `pnlNetUsd`
- `roiPct`
- `maxDrawdownPct`
- `winRatePct`
- `profitFactor`
- `action`
- `status`
- `reasonCode`
- `capitalMultiplier`
- `windowStartAt`
- `windowEndAt`

## Performance

- El endpoint de snapshots puede hacer calculo historico, pero queda fuera del runtime LIVE.
- El calculo es batch/cacheado por TTL y por grupos de estrategia.
- `ms-signals-orc` no llama `full` para discovery/sync por defecto.
- `ms-signals-orc` no consulta DB ni REST para decidir copy guard en el hot path cuando el snapshot esta precargado.

## Tests

- Summary/sync no llama `simulation=full`.
- Copy guard usa ventanas del snapshot.
- `1w` negativo reduce capital.
- `2w` negativo pausa aperturas.
- `1mo` negativo devuelve shadow-only.
- Ventanas positivas permiten abrir.
- Ventana requerida faltante bloquea cuando `require-window-data=true`.
- Ventana faltante permite con warning cuando `require-window-data=false`.
- Snapshot stale se marca y decide segun config.
- MICRO_LIVE promovido ignora `SHADOW_ONLY` historico.
- LIVE runtime usa snapshot/cache, no endpoint externo en hot path.
- El endpoint de metricas calcula por lote, no N llamadas pesadas por wallet.
