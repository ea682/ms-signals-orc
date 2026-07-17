# SHADOW Execution Calibration - Implementation Report

Fecha: 2026-07-14  
Estado: implementado con evidencia local simulada  
Validacion real: pendiente de pares SHADOW/MICRO_LIVE reales

## 1. Separacion de evidencia

Se distinguen dos mundos:

- SHADOW analitico: sirve para ranking/simulacion, no para calibrar ejecucion.
- `EXECUTABLE_SHADOW`: paso por el mismo contrato de sizing y conserva lineage para
  compararlo con una ejecucion MICRO_LIVE equivalente.

Solo se acepta un par con el mismo strategyKey, generationId, sourceEventId,
allocationId, symbol, side/action y banda de notional. Un mismatch, dato ausente o
generation desconocida produce UNKNOWN/fail-closed; nunca se rellena consultando la
generation ACTIVE actual.

## 2. Pipeline

1. Signals persiste operation/shadow events con generation y strategy lineage.
2. Publica `copy-operation-event-persisted-v1` por outbox.
3. ETL upserta la evidencia economica y publica candidatos de pares.
4. El worker ETL reclama jobs con lock recuperable y calcula el par.
5. `copy_micro_live_calibration_v3` conserva distribuciones/version/confidence.
6. Signals lee por strategy+generation+segmento y aplica fallback explicito.

Metricas calibradas:

- `PNL_CAPTURE_RATIO`
- `FILL_ERROR_BPS`
- `FEE_ERROR_USD`
- `SLIPPAGE_ERROR_BPS`
- `LATENCY_ERROR_MS`
- `PNL_ERROR_USD`

## 3. Estadistica y fallback

`ExecutionCalibrationEngine` calcula p50, p75, p90, robust mean, sample standard
deviation, sample/effective sample count, rango temporal y confidence LOW/MEDIUM/HIGH.

El fallback es jerarquico por segmento: exacto y luego segmentos mas amplios hasta ALL.
Cada respuesta identifica el nivel usado. Muestra pequena reduce confidence; evidencia
stale o ausente queda no disponible con reason code. No se transforma una distribucion
faltante en cero.

## 4. API y observabilidad

Endpoint local de consulta:

```text
GET /internal/v1/copy/execution-calibration
```

Requiere strategyKey canonico, generationId, metrica y segmento. Emite:

- `signals.shadow.calibration.total`
- `signals.shadow.calibration.sample.total`
- timer `signals.shadow.calibration.duration`
- gauge `signals.shadow.reality_score`, actualizado solo por PNL capture conocido;
  UNKNOWN no sobrescribe el ultimo valor valido.
- log estructurado con strategyKey, generation, metric, fallback, muestras,
  confidence, availability, reason codes y elapsed registrado.

No contiene secretos ni habilita dinero.

## 5. Lineage e idempotencia

Las migraciones `V202607140002__copy_economic_calibration_lineage_v3.sql` en Signals y
`V202607140001__wallet_strategy_financial_engine_v3.sql` en ETL agregan campos e
indices sin reescribir historico. La submission usa una clave determinista; repetir el
mismo par es no-op y un payload conflict se rechaza.

## 6. Validacion

Se ejecutaron tests unitarios para p50/p75/p90, fallback, staleness, small sample,
confidence, identity y lectura fail-closed. Las pruebas PostgreSQL de ETL validaron
claim, persistencia, idempotencia y pares ejecutables sobre PostgreSQL 16.14.

La suite completa ETL quedo en 147 tests, 0 fallos/errors, 2 skips opt-in. Signals quedo
en 640 tests, 0 fallos/errors, 4 skips de entorno/opt-in posteriormente cubiertos en los
caminos PostgreSQL relevantes.

## 7. Limite honesto

La implementacion esta lista para recolectar evidencia, pero esta corrida no creo fills
MICRO_LIVE reales. Por tanto `ShadowRealityScore` y las distribuciones reales no pueden
certificarse. Mientras no existan muestras suficientes, la respuesta es UNKNOWN/LOW y
la promocion monetaria permanece bloqueada.

Cutover: habilitar solo SHADOW ejecutable, observar pares y calidad, revisar p90 y
confidence, y autorizar una prueba MICRO_LIVE manual. Rollback: detener el worker de
calibracion; conservar evidencia y mantener CLOSE/REDUCE.
