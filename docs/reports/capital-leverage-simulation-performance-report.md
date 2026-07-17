# Capital x Leverage Simulation And Performance Report

Fecha: 2026-07-14  
Estado: matriz completa implementada y persistida  
Fuente de sizing: `modules/copy-target-core`

## 1. Matriz canonica

Capitales USD:

```text
100, 250, 500, 1000, 5000, 10000, 50000,
100000, 250000, 500000, 1000000
```

Leverage:

```text
x5, x10, x15, x20
```

Resultado: 11 x 4 = 44 escenarios exactos, indices 0..43. Cada escenario reutiliza
`TargetPortfolioCalculator`, guarda notional, margen, legs seleccionadas/omitidas,
coverage, rounding loss, minNotional skips, target portfolio, versiones y evidencia
economica.

El leverage no modifica notional. Una equity missing/stale/invalid conserva el codigo
de bloqueo del core. Sin evidencia historica de fills/precios/order book se registran
UNKNOWN con razones como `HISTORICAL_EXECUTION_EVIDENCE_MISSING`,
`HISTORICAL_BINANCE_PRICE_MISSING`, `LIQUIDITY_CAPACITY_EVIDENCE_MISSING`,
`LIQUIDATION_INPUTS_MISSING` y `VENUE_BASIS_EVIDENCE_MISSING`.

## 2. Worker y persistencia

`CopySimulationWorker` es cold path y esta condicionado por feature flag. Reclama un
lote pequeno, genera exactamente 44 escenarios, persiste uno por uno, puede pausar por
cursor y cede CPU entre escenarios. Locks stale se reencolan y los fallos usan retry
delay. El hot path no ejecuta la matriz.

`V202607140001__institutional_financial_simulation_v3.sql`:

- amplia cursor a 44 e indice a 0..43;
- agrega strategyKey, generationId/status/reasons;
- acepta origen SHADOW, MICRO_LIVE o LIVE solo como evidencia de simulacion;
- agrega economic evidence, calculator/policy versions y field availability;
- exige strategyKey canonico e indexa generation+strategy+status.

Aceptar `execution_mode=LIVE` en el job no habilita ordenes LIVE. Solo evita perder
evidencia de una ejecucion ya registrada.

## 3. Lectura Metrics

Metrics consulta solo el ultimo job COMPLETED con coincidencia exacta
strategyKey+generationId. Una matriz incompleta, antigua o de otra generation retorna
UNKNOWN. El endpoint es:

```text
GET /operaciones/metrica/v2/simulation-matrix
```

No existe llamada HTTP sincronica Signals desde `/joyas`; el read model PostgreSQL
evita ciclos y N+1.

## 4. Performance medida

Hot path Signals, fixture local, 100.000 evaluaciones:

| Metrica | Resultado |
|---|---:|
| p50 | 39.0285 us |
| p95 | 52.3734 us |
| max | 58.7817 us |
| llamadas remotas hot | 0 |

Refresh cold del fixture: 3 llamadas remotas una vez, 16 ms.

Idempotencia+budget local, 20.000 muestras: p50 0.0040 ms, p95 0.0068 ms. Flujo de dos
estrategias y 100 eventos: 0.0137 ms/evento promedio.

Benchmark de coordinacion concurrente:

| Eventos | p95 ms | Throughput eventos/s |
|---:|---:|---:|
| 1 | 0.3227 | 175 |
| 10 | 0.1078 | 36,643 |
| 50 | 0.0619 | 40,906 |
| 100 | 0.0776 | 39,779 |
| 300 | 0.0506 | 171,733 |
| 1000 | 0.0539 | 296,007 |

Para 1000 eventos: p99 0.1387 ms y max 0.9880 ms. Son tiempos en memoria/locales; no
incluyen Kafka, red, Binance ni PostgreSQL productivo.

Lectura HTTP Metrics de matriz 44: cold 7.001 ms, p50 5.650 ms, p95 6.898 ms,
p99 8.462 ms y max 10.533 ms en 100 requests calientes locales.

## 5. Validacion y limites

Core: 36 tests, 0 fallos. Signals: 640 tests, 0 fallos/errors. PostgreSQL snapshot,
migration contract y worker se validaron sobre PostgreSQL 16.14. Se reprodujo y
corrigio la constraint legacy que solo permitia MICRO_LIVE en jobs.

No existe baseline anterior equivalente de 44 escenarios, por lo que no se inventa un
porcentaje de mejora. Faltan order books y fills reales para completar capacity,
stress/emergency exit, liquidation distance, latency break-even, basis y funding
divergence. Esos campos estan preparados, versionados y fail-closed como UNKNOWN.
