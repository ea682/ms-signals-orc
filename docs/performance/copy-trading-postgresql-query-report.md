# Reporte de queries MICRO_LIVE/LIVE

Fecha: 2026-07-10
Ambientes: PostgreSQL QA read-only para SELECT/EXPLAIN; PostgreSQL local para
concurrencia y escritura.

## 1. Trazado desde codigo

| # | Caso | Archivo / metodo | SQL o acceso | Frecuencia y riesgo |
|---:|---|---|---|---|
| 1 | Allocations activas | `UserCopyAllocationRepository.findAllActiveRuntimeAllocations` | SELECT active LIVE/MICRO | job cada 5 s; 2 rows QA |
| 2 | Snapshot cache-only | `UserCopyAllocationServiceImpl.refreshRuntimeAllocationSnapshot` | query #1 y group en memoria | fuera del evento; keep-LKG |
| 3 | Resolver usuarios | `UserDetailCachedServiceImpl.refreshSnapshot` -> `UserDetailServiceImpl.findAllActive` | `users` + detail + API por user | cada 60 s; N+1 `1+2N` |
| 4 | Crear intent | `CopyDispatchIntentRepository.insertIfAbsent` | INSERT ON CONFLICT idempotency | una vez por first dispatch; local only |
| 5 | Reclamar intent | `PostgresCopyDispatchIntentStore.acquire` | advisory xact lock + insert + lookup + state save | cada dispatch real; serial por user/allocation/mode |
| 6 | Replay | `findByIdempotencyKey` | unique lookup | cada acquire/replay |
| 7 | Payload hash | `PostgresCopyDispatchIntentStore.duplicatePermit` | comparacion Java sobre row #6 | no SQL adicional |
| 8 | Crear reserva | `PostgresCopyDispatchIntentStore.acquire` | update JPA a `PENDING` en la misma transaccion | solo MICRO_LIVE open/increase |
| 9 | Margen reservado | `loadBudgetSnapshot` | aggregate active + pending | una query por first MICRO claim |
| 10 | Open/reserved count | `loadBudgetSnapshot` | mismo aggregate #9 | sin N+1 |
| 11 | Persistir operation | `CopyOperationServiceImpl.upsertActiveOperation/closeOperation` | lookup exacto + save/flush | post-ack/recovery |
| 12 | Persistir event | `CopyOperationEventServiceImpl.recordRequired` | advisory progress lock + lookup + insert + outbox | cada progreso requerido |
| 13 | Por dispatchIntentId | operation/event repositories | unique operation, event partial index | recovery/auditoria |
| 14 | Por clientOrderId | intent/event repositories | intent list, event optional | fallback de persistencia/legacy |
| 15 | Por Binance orderId | no repository de intent solo por order | correlacion DB user+order; reconciliacion HTTP usa clientOrderId | no hot SQL demostrado |
| 16 | Claim reconciliation | `findReconciliationIdsForUpdateSkipLocked` | estado/edad/order/limit/FOR UPDATE SKIP LOCKED | job cada 5 s, batch 50 |
| 17 | SKIP LOCKED | mismo metodo | row locks | exige permiso write; probado local |
| 18 | Partial progress | `findDispatchProgress` + unique progress | cumulative qty/result | cada partial/recovery |
| 19 | MANUAL_REVIEW | `CopyOrderReconciliationService` | load PK + guarded save | al agotar reconciliacion |
| 20 | Intents estancados | query #16 | stale DISPATCHING o reconciliable due | job cada 5 s |
| 21 | Recovery restart | `CopyOrderReconciliationWorker` | claim #16 y lookup Binance por clientOrderId | nunca reenvia create order |
| 22 | Liberar/confirmar reserva | `markRejected` / `markPersisted` | state save con policy | definitivo; ambiguo conserva pending |

`ms-binance-engine` no contiene datasource, JPA, JDBC ni Hikari. Su contribucion
es HTTP/Binance y no genera SQL del hot path.

## 2. Planes QA

| Query | Plan observado | Planning ms | Execution ms | Buffers | Clase |
|---|---|---:|---:|---|---|
| Intent PK | PK Index Scan | no ANALYZE separado | n/a | n/a | ACCEPTABLE, tabla vacia |
| Intent idempotency miss | unique Index Scan | 0,557 | 0,024 | hit 2 | ACCEPTABLE, no escala probada |
| Intent clientOrderId | Index Scan | preliminar | n/a | n/a | ACCEPTABLE, tabla vacia |
| Intent user+Binance order | partial unique Index Scan | preliminar | n/a | n/a | ACCEPTABLE, tabla vacia |
| Operation por dispatch | Seq Scan | 0,203 | 0,009 | heap vacio | NEEDS_ATTENTION solo por falta de cardinalidad |
| Event por dispatch | Seq Scan | 1,199 | 0,009 | heap vacio | NEEDS_ATTENTION solo por falta de cardinalidad |
| Budget aggregate | active seq + pending index | 1,599 | 0,078 | hit 2 | ACCEPTABLE, ramas vacias |
| Runtime allocations | Seq Scan de 2 rows | 1,173 | 0,078 | read 1 | GOOD para cardinalidad actual |
| Manual review queue | partial Index Only Scan | preliminar | n/a | n/a | ACCEPTABLE, vacia |
| Reconciliation filter | Seq Scan + Sort | preliminar | n/a | vacia | NEEDS_ATTENTION, fixture requerido |
| Movement dedupe hit | unique Index Only Scan | 0,038 | 0,083 | read 5, heap fetch 0 | GOOD con 8,8 M rows |
| Latest position sin fecha | 38 Index Scans + Incremental Sort | 22,109 cold | 0,964 | hit 73/read 17; planning hit 15.567/read 690 | NEEDS_ATTENTION |
| Latest position con upper bound | 28 Index Scans, 10 pruned | 25,359 cold | 0,901 | hit 54/read 14 | NEEDS_ATTENTION |
| Detail user por user | Seq Scan | 0,193 | 0,063 | read 8 | NEEDS_ATTENTION al crecer |
| API key por user | Seq Scan | 0,094 | 0,022 | read 1 | NEEDS_ATTENTION al crecer |

`FOR UPDATE SKIP LOCKED` no pudo explicarse con `copy_audit`: PostgreSQL exige
privilegio UPDATE para tomar ese row lock. No se elevo el rol. Se explico el
mismo filtro/order sin lock y se ejecuto la concurrencia en PostgreSQL local.

## 3. Latest position: hallazgo principal

`OperationMovementEventServiceImpl.persist` ejecuta, por cada movimiento no
duplicado:

1. unique lookup en `operation_movement_event_dedupe`;
2. latest movement por `position_key`;
3. INSERT ledger + outbox.

El primer lookup es eficiente. El segundo consulta una tabla particionada por
`event_time`, pero el lookup no posee lower bound y su indice no incluye el
ultimo campo del ORDER BY. El costo warm preparado fue bajo, pero el costo de
planning frio fue 22-25 ms y el catalogo muestra scans masivos en children
vacios. Esto puede afectar el tiempo previo al dispatch, aunque no se puede
atribuir cada contador sin `pg_stat_statements`.

## 4. Percentiles JDBC

Los percentiles completos estan en el baseline. Resumen:

| Query | p50 ms | p95 ms | p99 ms | Contexto |
|---|---:|---:|---:|---|
| Idempotency miss | 1,218 | 1,318 | 1,377 | intent vacio |
| Budget | 1,278 | 1,447 | 2,725 | operation/intent vacios |
| Allocation snapshot | 1,394 | 1,552 | 1,721 | 2 rows |
| Movement guard hit | 1,204 | 1,302 | 1,404 | 8,8 M dedupe rows |
| Latest position preparado | 1,761 | 2,000 | 2,578 | hit real, 38 partitions |
| Latest position no preparado | 4,666 | 6,461 | 6,927 | catalogo ya caliente |
| API key por user | 1,340 | 2,087 | 3,359 | 1 row, sin indice |

El `EXPLAIN` cold inicial vio hasta 25 ms de planning antes de calentar catalogo;
por eso no debe usarse solo el p95 preparado para dimensionar startup o una
replica nueva.

## 5. Locks y transacciones por query

| Query | Lock | Duracion esperada | Riesgo |
|---|---|---|---|
| Acquire MICRO | advisory xact por allocation | hasta commit pre-send | serializacion correcta; no incluir HTTP en tx |
| Intent insert/save | row/index locks | transaccion corta | unique contention por replay |
| Budget aggregate | AccessShare | misma tx que reservation | correcto para snapshot tras advisory lock |
| Event progress | advisory xact por progress | insert/outbox required | serializa partial identico |
| Reconciliation batch | row locks SKIP LOCKED | claim + saveAll flush | batch 50; no HTTP dentro de claim tx |
| Reconcile one | nuevas transacciones cortas | HTTP fuera del claim tx | revisar propagation en runtime |

El codigo de claim devuelve entities detached despues de commit y el worker hace
HTTP en el loop, por lo que el row lock no se conserva durante Binance.

## 6. N+1 y frecuencia

- Candidate resolution usa caches; no hace metric/full simulation en el evento.
- Allocation snapshot: una query cada 5 s.
- User snapshot: una query root + dos por usuario cada 60 s.
- Reconciliation: una claim query cada 5 s mas PK load del batch.
- Dispatch first path: advisory lock, insert-if-absent, idempotency lookup,
  aggregate MICRO y save/flush.
- Replay no vuelve a enviar; reutiliza/reconcilia segun estado y hash.

El N+1 de usuarios no esta en el evento, pero puede consumir conexiones y crear
spikes al crecer el numero de cuentas.

## 7. Integridad QA

Con 0 intents se obtuvieron cero casos para:

- `PERSISTED` sin operation/event;
- ambiguo sin reservation `PENDING`;
- rejection terminal con pending;
- `MANUAL_REVIEW` liberado;
- non-terminal stale mayor a 5 min.

Esto valida la consulta, no el comportamiento productivo. Los escenarios de
escritura fueron validados en el test local.

## 8. Pruebas locales

`CopyTradingPostgresConcurrencyTest` ejecuto 14 tests/0 fallos/0 skips sobre
PostgreSQL local 18 y cubrio:

- dos workers y `SKIP LOCKED`;
- dos replicas/advisory lock/reserva atomica;
- partial igual y partial distinto;
- replay post-restart;
- late ack terminal y `MANUAL_REVIEW`;
- deadlock controlado;
- statement/lock timeout;
- pool exhaustion;
- rollback de ledger;
- crash post-reservation;
- operation/event idempotentes.

La suite tambien soporta Testcontainers PostgreSQL 16 cuando Docker esta
disponible. En este host Docker no estaba instalado; la ejecucion real local fue
en PostgreSQL 18 y debe repetirse en CI con 16.
