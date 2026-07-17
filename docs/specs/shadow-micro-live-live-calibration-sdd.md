# SDD: SHADOW, MICRO_LIVE And LIVE Calibration

Estado: APROBADO PARA IMPLEMENTACION  
Fecha: 2026-07-14  
Repositorio: `ms-signals-orc`

## 1. Objetivo

SHADOW debe modelar la misma accion y sizing que LIVE y explicar la distancia con la
ejecucion real. La calibracion nunca convierte simulacion en garantia ni promueve sola.

## 2. Evidencia requerida

Cada fill/movimiento registra UTC: sourceObservedAt, signalAcceptedAt, decisionAt,
dispatchAt, venueAckAt, firstFillAt, lastFillAt y reconciledAt; source/target price,
quantity/notional, fees, funding, slippage, rounding, mark, order type, maker/taker,
liquidity/volatility/time bucket, symbol/side/action, versions y reason codes.

SHADOW, MICRO y LIVE se comparan solo con el mismo source event set, strategyKey, capital,
leverage, versions y ventana. Un par incompleto queda UNKNOWN `CALIBRATION_PAIR_INCOMPLETE`.

## 3. Dimensiones y estadistica

Segmentos: symbol, side, action, notional band, volatility, liquidity, time bucket,
maker/taker, quote asset, model version y market regime. Por segmento se calculan count,
p50/p75/p90, robust mean, stddev, effective sample, oldest/newest/age, confidence y version
para fill, fees, funding, slippage, latency, venue basis y PnL error.

Fallback jerarquico:

1. exact segment;
2. symbol+side+action+notional;
3. symbol+action;
4. action+notional;
5. action;
6. global conservative prior.

Cada fallback baja confidence y agrega reason. Stale calibration nunca se trata como
vigente. Cero solo es valido con muestras reales cero; sin evidencia se usa prior
conservador/UNKNOWN, nunca cero silencioso.

## 4. ShadowRealityScore

Componentes: PnL capture, fill error, fee error, funding error, slippage error, latency
error, venue basis error y missed movement. Se normalizan por tolerancias configurables y
se ponderan por sample confidence. Cualquier componente obligatorio UNKNOWN limita el
score maximo y bloquea `realValidated`.

Estados: simulated, calibrated, partiallyRealValidated, realValidated, degraded.
`realValidated` requiere evidencia LIVE real suficiente; este cambio no la fabrica.

## 5. MICRO_LIVE y LIVE

MICRO_LIVE es independiente, 100 USDC total y x5. Sirve para evidencia real limitada,
no como simulacion. LIVE conserva configuracion productiva, pero ambos modos y sus jobs de
promocion quedan disabled por default. No se ejecutan llamadas privadas ni ordenes reales.

Promotion gates: FULL valido, SHADOW suficiente, calibration confidence, positive OOS/
post-selection, copyability, risk/capacity y aprobacion/feature flag manual. Degradacion
no bloquea exits. Reentry necesita varias ventanas sanas.

## 6. Persistencia e idempotencia

Reutilizar evidencia V3 de ETL y tablas de execution/dispatch. Agregar dimensiones y
estadisticos con migraciones aditivas, sin duplicar el calibrador. Idempotency key incluye
event-set hash, strategyKey, capital/leverage, modes, window y model versions. Conflictos
de payload se rechazan y auditan.

Ownership concreto:

- ETL empareja EXECUTABLE_SHADOW/MICRO_LIVE sobre el mismo conjunto de eventos y persiste
  cada ventana en `copy_micro_live_calibration_v3`.
- Signals no repite esa reconciliacion: agrega las ventanas ya aceptadas, calcula
  percentiles/mean robusto/confidence/fallback y expone una lectura interna.
- El endpoint es
  `GET /internal/v1/copy/execution-calibration?strategyKey&generationId&metric&symbol&side&action&notionalBand`.
- `strategyKey` y `generationId` deben coincidir exactamente. Evidencia legacy sin
  generacion queda UNKNOWN con `CALIBRATION_GENERATION_UNKNOWN`; nunca habilita
  MICRO_LIVE/LIVE.
- `sample_count` se usa como peso y confidence usa effective sample size, evitando que
  una ventana agregada cuente igual que una muestra individual.

La evidencia emitida por Signals distingue `sourceEventId` de `idOrderOrigin` y lleva
la identidad capturada al decidir: `strategyKey`, `generationId`, scope, capital,
leverage, accion, banda y versiones. El outbox es la frontera durable tanto para SHADOW
ejecutable separado como para MICRO_LIVE/LIVE. Solo se acepta un par exacto; lineage
ausente o distinto produce UNKNOWN/mismatch fail-closed y nunca se completa consultando
la generacion ACTIVE vigente.

## 7. Observabilidad y pruebas

Contadores calibration total/sample/fallback/stale/unknown y gauge
`signals.shadow.reality_score`. El gauge solo se actualiza con
`PNL_CAPTURE_RATIO` disponible: `clamp(p50 * 100, 0, 100)`. Evidencia UNKNOWN/stale no
sobrescribe el ultimo valor conocido ni se interpreta como cero. Logs incluyen
identidad, dimension/fallback, muestras, confidence/version/reason/elapsedMs.

Tests: p50/p75/p90, robust average, small sample, stale, fallback hierarchy, zero real,
missing evidence, paired identity/window/version mismatch, SHADOW/MICRO/LIVE errors,
reality score y no auto-promotion. ETL contract fixtures prueban serializacion de ambos
lados. Sin evidencia real, el informe debe declarar PARTIALLY_IMPLEMENTED/NOT_REAL_VALIDATED.

## 8. Rollback

Desactivar consumo de calibration version nueva, conservar muestras/resultados, volver a
prior conservador y mantener SHADOW. Nunca cerrar posiciones ni promover por rollback.
