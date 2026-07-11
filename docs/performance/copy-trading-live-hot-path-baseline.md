# Baseline del hot path Copy Trading LIVE

Fecha: 2026-07-11
Commit signals: `9b0a83126d5c45df3ee65ffdc0ab290786dbaaa6`
JVM: Java 21
Ordenes reales: ninguna

## Alcance y metodo

Se ejecutaron benchmarks locales deterministas y consultas QA read-only ya existentes. Las cifras locales no incluyen red Binance; miden costo propio del servicio. Las cifras PostgreSQL separan ejecucion preparada de planning frio.

Comando local:

```powershell
.\mvnw.cmd -q "-Dtest=CopyDispatchLocalPerformanceTest,CopyDispatchConcurrencyBenchmarkTest,CopyBudgetResolverTest,HyperliquidCopyCandidateResolverTest,ShadowLiveReadinessEvaluatorTest" -Dcopy.benchmark.enabled=true test
```

Resultado: `PASS`, 42.7 s.

## Baseline local

| Escenario | p50 ms | p95 ms | p99 ms | max ms | throughput eventos/s |
|---|---:|---:|---:|---:|---:|
| identity + budget, una operacion | 0.0034 | 0.0062 | no reportado | no reportado | no reportado |
| 1 tarea concurrente | 0.4583 | 0.4583 | 0.4583 | 0.4583 | 1,925.67 |
| 10 tareas | 0.0364 | 0.3963 | 0.3963 | 0.3963 | 18,730.00 |
| 50 tareas | 0.0246 | 0.0797 | 0.4540 | 0.4540 | 45,289.86 |
| 100 tareas | 0.0286 | 0.1467 | 0.5226 | 0.6548 | 54,827.57 |
| 300 tareas | 0.0158 | 0.0975 | 0.1427 | 0.2004 | 148,082.33 |
| 1000 tareas | 0.0105 | 0.0440 | 0.1237 | 0.3362 | 334,649.62 |

Dos estrategias por evento promediaron `0.0151 ms` para identity + budget. Estas cifras son microbenchmarks de regresion, no una estimacion de latencia de exchange.

## Baseline PostgreSQL read-only

| Lookup | p95 preparado aproximado | Observacion |
|---|---:|---|
| idempotency/intent | 1.318 ms | indice unico por idempotency key |
| budget | 1.447 ms | agregacion de posicion activa + reserva pendiente |
| allocation | 1.552 ms | lookup runtime persistente; el hot path usa snapshot cacheado |
| latest-position | 2.0 ms | puede planificar 38 particiones |
| latest-position planning frio | 22-25 ms | costo de planning, no ejecucion preparada |

El latest-position se usa en el estado de origen una vez por evento. No se observo una llamada por allocation en el camino trazado.

## Queries y transacciones por etapa

| Etapa | Hot path | Queries por evento/allocation |
|---|---|---|
| Evento normalizado -> eligibility | caches de usuarios, allocations y guard | 0 por allocation |
| Eligibility -> sizing | memoria + metadata de simbolo/cache | 0 nuevas por allocation por esta correccion |
| Claim intent | transaccion corta PostgreSQL | 1 insert-on-conflict + lectura/claim segun estado |
| Intent -> Binance adapter | fuera de transaccion | 1 HTTP |
| Persistencia | transaccion separada | operacion + ledger segun fill |
| Reconciliacion | worker `SKIP LOCKED` | batch, fuera del dispatch inicial |

## Pool y locks

- `ms-signals-orc` prod configura aproximadamente Hikari max `40`, min `8` por replica.
- PostgreSQL QA observado: `max_connections` cercano a `100`.
- Dos replicas pueden reservar hasta 80 conexiones; tres superarian el limite antes de contar otros servicios.
- No se cambia el pool sin inventario global.
- Los deadlocks observados estan en mutaciones Shadow/distribucion, no en la llamada HTTP del hot path.

## Criterio after

- p95 local no puede empeorar mas de 5%.
- p99 local no puede empeorar mas de 10%.
- Para valores sub-milisegundo se reporta tambien delta absoluto.
- El after debe usar el mismo comando/JVM y registrar cualquier limitacion de Testcontainers.
