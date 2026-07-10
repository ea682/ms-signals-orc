# Reporte final: integridad y performance MICRO_LIVE/LIVE

Fecha: 2026-07-10

Dictamen: `AUDIT_ONLY`

## 1. Resumen ejecutivo

El codigo queda materialmente mas seguro y mas predecible, pero no se declara
listo para canary ni produccion. La razon no es una falla conocida de la suite:
son brechas de evidencia operacional que no deben esconderse en un sistema
financiero.

Bloqueadores para subir de `AUDIT_ONLY`:

1. La migracion `V202607100002__copy_dispatch_manual_review_integrity.sql` no
   esta aplicada en la base inspeccionada. El constraint desplegado aun no acepta
   `MANUAL_REVIEW`.
2. No se ejecuto concurrencia contra PostgreSQL con cardinalidad productiva,
   multiples replicas, deadlock y pool exhaust.
3. La matriz completa de fault injection de procesos/red/DB no esta cerrada.
4. No existe baseline end-to-end de staging con HTTP real entre servicios.
5. No se ejecuto canary MICRO_LIVE. Hacerlo en este trabajo habria violado la
   restriccion de no tocar Binance real.

El sistema conserva por defecto todos los nuevos dispatches reales apagados.

## 2. Hallazgos criticos

| ID | Hallazgo | Riesgo | Correccion |
|---|---|---|---|
| F1 | Gate bloqueado fabricaba fill SHADOW para allocation real | estado local falso y posible desync | gate separado; bloqueo lanza skip sin order/fill local |
| F2 | Cache miss de users/allocations podia leer DB en evento | spikes y cola | snapshots programados, LKG con max age y fail-closed |
| F3 | Sizing volvia a llamar metrics/allocation loaders | HTTP/DB oculto despues del candidate resolver | APIs cache-only por user+wallet, sin loader |
| F4 | Intent podia llegar a PERSISTED sin ledger enlazado | fill no auditable | `recordRequired` retorna UUID, se enlaza y PERSISTED lo exige |
| F5 | No existia estado operacional MANUAL_REVIEW | ambiguos agotados mal clasificados | estado, constraint e indice parcial nuevos |
| F6 | Carrera de progreso del ledger tras 23505 | transaccion abortada / evento no recuperable | advisory xact lock por intent+progreso antes del precheck |
| F7 | Unicidad legacy de clientOrderId se perdia al habilitar partials | duplicados legacy concurrentes | unique parcial solo para rows sin dispatch intent |
| F8 | State policy no cubria todas las escrituras | ack tardio podia sobrescribir terminal | guard de transicion en store y reconciliador |
| F9 | Caches de cliente/settings Binance no eran bounded | crecimiento de heap | Caffeine con maximumSize y expireAfterAccess |
| F10 | Remocion dinamica de lock account+symbol tenia race | dos critical sections para la misma key | 4.096 stripes estables y acotados |
| F11 | Timer de error se registraba como completed | diagnostico de latencia falso | result `error` y counters exactos de reconciliacion |
| F12 | Metrics Binance usaban symbol | cardinalidad creciente | labels reducidos a operation/result/reason |

## 3. Cambios en ms-signals-orc

### 3.1 Barrera real e identidad

- `COPY_NEW_DISPATCH_ENABLED`, `COPY_MICRO_LIVE_ENABLED` y
  `COPY_LIVE_ENABLED` son switches independientes.
- Defaults de nuevos dispatches reales: `false`.
- LIVE conserva dry-run, canary y whitelist para entradas.
- El coordinator solo admite `MICRO_LIVE` o `LIVE` y allocation durable exacta.
- Symbol, source identity, quantity, margin y clientOrderId se validan antes del
  claim.
- Si falta clientOrderId se deriva de la identidad durable; restart/replay usa
  el mismo valor.
- Payload economico distinto con la misma key falla cerrado.

### 3.2 Hot path

- Candidate resolution usa `getUsersCachedOnly` y allocation snapshot.
- Sizing usa `getCandidatesForUserWalletCachedOnly` y
  `getActiveAllocationsForUserWalletCachedOnly`.
- Cache miss o snapshot stale bloquea; no ejecuta HTTP/DB como fallback.
- History LKG tiene timestamp y max age.
- El snapshot de allocations se refresca cada 5 s por defecto y vence a 30 s.
- El snapshot de users se refresca cada 60 s y permite LKG hasta 10 min.
- Estado activo de copy se hidrata fuera del hot path y se refresca programado.

### 3.3 Dispatch, reserva y ledger

- Intent durable y `DISPATCHING/PENDING` existen antes del HTTP.
- MICRO_LIVE usa una consulta agregada para active+pending budget.
- Advisory xact lock serializa budget por user+allocation+mode.
- El mismo progreso del ledger se serializa con un lock mas granular.
- Required event y copy operation deben estar enlazados antes de `PERSISTED`.
- Partial conserva reservation `PENDING`; terminal ejecutado pasa a `CONFIRMED`.
- Rechazo definitivo sin fill pasa a `RELEASED`.
- Un estado con executed qty no puede rechazarse/liberar.

### 3.4 Reconciliacion

- Claim batch usa `FOR UPDATE SKIP LOCKED`.
- Lookup usa orderId primero y clientOrderId como fallback.
- El worker nunca invoca el endpoint de creacion.
- Timeout, 5xx post-body y ack malformado quedan ambiguos; no se reenvian.
- Backoff exponencial acotado y lease evitan reclamo agresivo.
- Agotamiento va a `MANUAL_REVIEW` y conserva reserva ambigua.
- Price-only agotado conserva efecto confirmado y marca revision manual.
- Un item fallido no detiene el batch.

## 4. Cambios en ms-binance-engine

- Cache de clientes: max 10.000, expire-after-access 6 h.
- Cache de settings account+symbol: max 50.000 y TTL configurable.
- Reutilizacion de cliente HTTP; no se crea uno por request.
- Locks account+symbol usan stripes fijos; no hay remocion concurrente.
- Reductions/closes no configuran margin/leverage.
- OPEN warm reutiliza configuracion confirmada; un mismatch no se asume.
- Timer separado de pre-network, network y total.
- Labels de metrics no incluyen symbol, user, wallet, order ni clientOrderId.
- `REJECTED/CANCELED/EXPIRED` con executedQty positivo conserva el fill.
- No hay RetryTemplate, `@Retryable` ni Resilience4j retry en order creation.
- El unico resend explicito es Binance `-1021` (timestamp): ocurre despues de
  rechazo inequívoco, refresca server time y conserva el mismo clientOrderId.
- Duplicate clientOrderId se resuelve con lookup, no con un tercer send.

## 5. Cambios no realizados

- No se alteraron formulas financieras de sizing, PnL, leverage o exposure.
- LIVE no hereda los 20/100/5 de MICRO_LIVE.
- No se retiro durabilidad ni constraint para ganar velocidad.
- No se habilito parallelStream.
- No se agrego panic-close.
- No se enviaron ordenes, cancelaciones o cambios de leverage reales.
- No se aplicaron migraciones ni escrituras a la base inspeccionada.
- No se activaron promociones ni allocations.

## 6. Antes y despues

CPU puro, no end-to-end:

| Medida | Antes | Despues | Cambio |
|---|---:|---:|---:|
| identity+budget p50 | 0.0040 ms | 0.0027 ms | -32.5% |
| identity+budget p95 | 0.0106 ms | 0.0055 ms | -48.1% |
| dos estrategias avg/evento | 0.1554 ms | 0.0181 ms | -88.4% |
| 1.000 eventos p50 | 0.0125 ms | 0.0124 ms | estable |
| 1.000 eventos p95 | 0.0378 ms | 0.0456 ms | +20.6%, aun sub-0.05 ms |
| 1.000 eventos p99 | 0.1214 ms | 0.1200 ms | -1.2% |
| 1.000 eventos throughput | 357,641 EPS | 355,480 EPS | -0.6%, ruido esperado |

Benchmark post-cambio completo:

| Eventos | Threads | p50 ms | p95 ms | p99 ms | max ms | EPS |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 1 | 0.2364 | 0.2364 | 0.2364 | 0.2364 | 90.99 |
| 10 | 10 | 0.0238 | 0.1092 | 0.1092 | 0.1092 | 39,355 |
| 50 | 50 | 0.0139 | 0.0437 | 0.1280 | 0.1280 | 47,819 |
| 100 | 64 | 0.0233 | 0.0834 | 0.2359 | 0.5874 | 39,689 |
| 300 | 64 | 0.0171 | 0.0683 | 0.1192 | 0.1936 | 165,984 |
| 1,000 | 64 | 0.0124 | 0.0456 | 0.1200 | 0.7517 | 355,480 |

Tabla de evidencia pedida:

| Dimension | Antes | Despues | Validez |
|---|---|---|---|
| p50/p95/p99 local CPU | medido | medido arriba | valido solo para identidad+budget |
| throughput CPU | medido | medido arriba | sin DB/HTTP |
| queries hot path | loaders posibles | snapshots + 1 claim/budget agregado | verificado por tests/arquitectura |
| allocations | N/M | 1-20 en identidad; runtime cache-only | falta benchmark 100 allocations real |
| CPU proceso | N/M | N/M | requiere JFR staging |
| memoria | caches sin bound | 10k/50k y stripes 4.096 | falta heap soak |
| GC | N/M | N/M | requiere JFR |
| conexiones | N/M | N/M | requiere datasource/HTTP pool metrics |
| lock time | N/M | N/M | requiere pg_stat y timer de wait |
| E2E ack | N/M | N/M | prohibido inferir sin staging/canary |

## 7. Evidencia de suites

Ultima evidencia previa al cierre documental:

| Servicio | Tests | Failures | Errors | Skipped | Wall |
|---|---:|---:|---:|---:|---:|
| ms-signals-orc | 421 | 0 | 0 | 1 benchmark | 10.695 s |
| ms-binance-engine | 33 | 0 | 0 | 0 | 6.288 s |
| signals package | success | - | - | tests skipped | 7.178 s |
| Binance package | success | - | - | tests skipped | 5.763 s |

La evidencia de signals ya incluye la proteccion final de state transitions.

Tests RED observados antes de GREEN:

- Faltaba `CopyDispatchStatePolicy`.
- Faltaban firmas de timers separadas en Binance.
- Faltaban APIs cache-only del sizing.
- Faltaba lock transaccional de ledger progress.
- Faltaban stripes estables account+symbol.
- Timer de timeout no existia con `result=error`.
- Store/reconciliador no aplicaban la policy en todas las mutaciones.

## 8. Matriz de invariantes financieras

| Invariante | Evidencia | Estado |
|---|---|---|
| Un send por source+allocation | coordinator/key tests + unique key | PASS unit; DB race pendiente |
| Allocations/strategies independientes | identity/coordinator tests | PASS |
| Dos replicas, un send | fake shared store concurrency | PARTIAL: falta PostgreSQL multi-process |
| Intent antes de Binance | store flow + DB SQL | PASS arquitectura |
| Reserva antes de MICRO send | store flow + budget tests | PASS unit; falta integration race |
| ClientOrderId estable | factory/restart tests | PASS |
| Payload conflict fail-closed | coordinator/store tests | PASS |
| Terminal no vuelve a send | state policy + integration architecture | PASS |
| Ambiguo no se reenvia | timeout/500/malformed tests | PASS |
| orderId antes de clientOrderId | worker test | PASS |
| FILLED null avgPrice aceptado | ambos normalizers | PASS |
| Partial aplica delta | persistence tests | PASS unit |
| Cancel/reject con fill conserva efecto | normalizer + reject guard | PASS unit |
| Rechazo sin fill libera | coordinator/budget tests | PASS unit |
| Persistence failure no resend | OPEN/INCREASE/REDUCE/CLOSE tests | PASS |
| PERSISTED exige operation+event+intent | store/persistence/architecture tests | PASS |
| Batch continua por item | worker test | PASS |
| MANUAL_REVIEW retiene ambiguo | service tests + migration | PASS code; migration pending |
| No panic-close | static contract/flow | PASS |

## 9. Matriz de estados

El modelo fisico usa menos estados que el modelo conceptual:

| Conceptual | Fisico |
|---|---|
| CLAIMED/RESERVED/SENDING | `DISPATCHING` + `reservation_status=PENDING` |
| SENT/ACKNOWLEDGED | `ACKNOWLEDGED` o estado Binance normalizado |
| PARTIAL | `PARTIALLY_FILLED` |
| persistence retry | `PERSISTENCE_PENDING` |
| unknown/recovery | `RECONCILING` |
| terminal applied | `PERSISTED` |
| terminal no fill | `REJECTED` + `RELEASED` |
| operator required | `MANUAL_REVIEW` |

Transiciones principales permitidas:

| Desde | Hacia |
|---|---|
| CREATED | DISPATCHING, REJECTED |
| DISPATCHING | NEW, PARTIALLY_FILLED, FILLED, RECONCILING, REJECTED |
| NEW | NEW, RECONCILING, PARTIALLY_FILLED, FILLED, REJECTED, MANUAL_REVIEW |
| PARTIALLY_FILLED | PARTIALLY_FILLED, FILLED, RECONCILING, PERSISTENCE_PENDING, PERSISTED, MANUAL_REVIEW |
| FILLED | FILLED, PERSISTENCE_PENDING, PERSISTED, RECONCILING, MANUAL_REVIEW |
| RECONCILING | RECONCILING, NEW, PARTIALLY_FILLED, FILLED, PERSISTENCE_PENDING, PERSISTED, REJECTED, MANUAL_REVIEW |
| PERSISTENCE_PENDING | PERSISTENCE_PENDING, RECONCILING, PARTIALLY_FILLED, FILLED, PERSISTED, MANUAL_REVIEW |
| PERSISTED | PERSISTED, RECONCILING por price, MANUAL_REVIEW |
| REJECTED/CANCELLED/MANUAL_REVIEW | solo mismo terminal |

Ningun estado salvo CREATED autoriza el primer send.

## 10. Matriz de los 76 tests obligatorios

Leyenda: PASS = test directo/equivalente; PARTIAL = unit/arquitectura sin DB o
proceso real; GAP = falta test dedicado.

| # | Caso | Estado / evidencia |
|---:|---|---|
| 1 | sameSourceEventSameAllocationSendsOnce | PASS coordinator |
| 2 | sameSourceEventDifferentAllocationsMayEachSendOnce | PASS identity/coordinator equivalente |
| 3 | sameSourceEventDifferentStrategiesMayEachSendOnce | PASS |
| 4 | twoReplicasRaceOnlyOneSends | PARTIAL fake durable store |
| 5 | persistedReplayIsNoop | PASS |
| 6 | sameKeyDifferentPayloadFailsClosed | PASS |
| 7 | sameNumericValueDifferentScaleDoesNotConflict | PASS |
| 8 | duplicateDatabaseInsertIsControlledNoop | PARTIAL fake insert; unique SQL verificada |
| 9 | restartUsesSameClientOrderId | PASS |
| 10 | noSourceIdentityRejectsBeforeClaim | PASS |
| 11 | filledWithNullAvgPriceAccepted | PASS ambos servicios |
| 12 | filledWithAveragePriceAccepted | PASS |
| 13 | priceDerivedFromCumQuoteWhenValid | PASS |
| 14 | newWithOrderIdAcknowledged | PASS |
| 15 | partialAcknowledged | PASS |
| 16 | malformedWithoutOrderIdAmbiguous | PASS |
| 17 | timeoutBecomesReconciliation | PASS |
| 18 | HTTP5xxAfterPossibleSendDoesNotRetry | PASS |
| 19 | rejectedWithoutFillReleases | PASS |
| 20 | canceledWithoutFillReleases | PASS |
| 21 | canceledWithFillPersists | PARTIAL normalizer/effect guard |
| 22 | expiredWithFillPersists | GAP test dedicado |
| 23 | fillPersistenceFailureDoesNotResend | PASS |
| 24 | openPersistenceFailureDoesNotResend | PASS |
| 25 | increasePersistenceFailureDoesNotResend | PASS |
| 26 | reducePersistenceFailureDoesNotResend | PASS |
| 27 | closePersistenceFailureDoesNotResend | PASS |
| 28 | requiredLedgerFailureKeepsPersistencePending | PARTIAL architecture/error path |
| 29 | sameProgressDoesNotInsertTwice | PASS precheck + advisory lock contract |
| 30 | intentNotPersistedBeforeRequiredLedger | PASS store guard |
| 31 | dispatchIntentIdLinksOperation | PASS persistence |
| 32 | dispatchIntentIdLinksEvent | PASS persistence |
| 33 | partialFirstDelta | PASS |
| 34 | samePartialNoop | PASS |
| 35 | partialGrowthOnlyPersistsDelta | PARTIAL cumulative tests |
| 36 | finalFillPersistsRemainder | GAP dedicado |
| 37 | outOfOrderLowerExecutedQtyNoop | GAP dedicado |
| 38 | canceledPartialPreservesFill | PARTIAL normalizer |
| 39 | orderMarginMaximum | PASS |
| 40 | allocationBudgetMaximum | PASS |
| 41 | maximumFiveOpenOrReserved | PASS |
| 42 | pendingReservationIncluded | PASS |
| 43 | ambiguousKeepsReservation | PASS |
| 44 | retryDoesNotReserveTwice | PASS |
| 45 | rejectionReleasesReservation | PASS |
| 46 | balanceDoesNotReplaceAllocationBudget | PASS |
| 47 | leverageChangesNotionalNotMarginLimit | PASS |
| 48 | allocationsDoNotShareBudget | PARTIAL identity/query scope; falta DB concurrente |
| 49 | liveUsesExposureSizing | PASS CopyBudgetResolver |
| 50 | liveDoesNotUseFixedTwenty | PASS |
| 51 | liveMissingExposureRejects | PASS |
| 52 | liveInvalidExposureRejects | PARTIAL zero/missing path |
| 53 | liveHotPathNoMetricHTTP | PASS cache-only runtime test |
| 54 | liveHotPathNoFullSimulation | PASS architecture |
| 55 | livePersistenceFailureDoesNotResend | PASS |
| 56 | liveTwoReplicasOnlyOneSend | PARTIAL fake store |
| 57 | liveReplayNoop | PASS |
| 58 | liveDifferentAllocationsIndependent | PASS identity; falta DB multi-replica |
| 59 | orderIdPreferred | PASS |
| 60 | clientOrderIdFallback | PASS |
| 61 | orphanFillDetected | PASS dry-run contract |
| 62 | orphanFillBackfilledIdempotently | PASS persistence unit |
| 63 | batchContinuesAfterOneFailure | PASS |
| 64 | maxAttemptsToManualReview | PASS |
| 65 | workerNeverCallsSend | PASS architecture/interface use |
| 66 | twoWorkersLeaseSameIntentOnce | PARTIAL SKIP LOCKED SQL; falta integration |
| 67 | backoffApplied | PASS |
| 68 | pricePendingRemainsReconcilable | PASS |
| 69 | shadowModeCannotEnterRealCoordinator | PASS |
| 70 | unknownModeFailsClosed | PASS |
| 71 | zeroQuantityRejectedBeforeClaim | PASS |
| 72 | negativeQuantityRejectedBeforeClaim | PASS |
| 73 | blankSymbolRejected | PASS |
| 74 | blankSourceIdentityRejected | PASS |
| 75 | invalidAllocationRejected | PASS |
| 76 | missingMarginForMicroOpenRejected | PASS |

Los `GAP` y `PARTIAL` impiden declarar canary-ready.

## 11. Matriz de fault injection

| Falla | Evidencia | Estado |
|---|---|---|
| DB timeout create intent | zero gateway calls | PASS |
| DB timeout reserve | no caso separado | GAP |
| DB timeout operation/event | persistence pending generico | PARTIAL |
| deadlock | no PostgreSQL integration | GAP |
| pool exhausted | no integration | GAP |
| HTTP connect/read timeout | ambiguous/no resend generico | PARTIAL |
| connection closed post-body | 500/post-body fixture | PARTIAL |
| truncated/invalid JSON | malformed ack | PARTIAL |
| FILLED null avgPrice | PASS |
| FILLED missing updateTime | normalizer tolera; test dedicado falta | PARTIAL |
| repeated PARTIAL | PASS |
| lookup 404 temporal | not-found/backoff | PASS unit |
| Binance 429 / 418 | no fixture dedicado | GAP |
| Binance 500 | PASS |
| restart Binance/signals | recovery unit, no process restart | PARTIAL |
| two replicas | fake shared store | PARTIAL |
| clock skew | -1021 code path auditado; test dedicado falta | PARTIAL |
| empty cache | fail-closed test | PASS |
| allocation disabled mid-event | resolver/gate unit, race integration falta | PARTIAL |

## 12. PostgreSQL

El script read-only completo paso con exit 0. Hallazgos del runtime inspeccionado:

- PostgreSQL 16.10.
- 0 intents, 0 reservas, 0 manual review.
- Unique de idempotency y user+Binance order presentes.
- Unique de dispatch event progress presente.
- Budget pending usa su indice.
- La tabla pequena produce seq scan en varias consultas; no es evidencia de mal
  plan ni de buen plan a cardinalidad real.
- Falta aplicar la nueva migration de MANUAL_REVIEW/legacy clientOrderId.

No se ejecutaron DDL ni DML.

## 13. Observabilidad agregada

Timers: ingest, candidate, guard, sizing, claim, reservation, pre-network,
Binance engine, Binance network, post-ack, end-to-end y reconciliation.

Counters: authorized, duplicate, conflict, ambiguous, rejected, persisted,
manual review, reservation rejected, reconciliation success/failure, partial e
integrity violation.

Gauges: pending, ambiguous, persistence pending, manual review, oldest pending,
reserved margin, open/reserved positions y backlog.

No se agregaron IDs ni symbol como labels. Los IDs siguen disponibles solo en
logs de correlacion y no se imprimen secrets/firmas.

## 14. Requisitos de despliegue

1. Desplegar migration y verificar constraint/indexes.
2. Mantener todos los dispatch switches en false.
3. Desplegar ambos servicios en audit-only.
4. Esperar snapshots users/allocations/metrics y cache de active operations.
5. Ejecutar SQL de validacion y EXPLAIN con cardinalidad staging.
6. Ejecutar fault matrix restante y PostgreSQL concurrency.
7. Abrir MICRO_LIVE solo bajo el runbook.

## 15. Criterio para cambiar el dictamen

- `MICRO_LIVE_CANARY_READY`: migration aplicada, suites/DB concurrency/faults
  criticos verdes, snapshots estables y dashboard operativo.
- `LIVE_CANARY_READY`: ademas baseline LIVE exposure y una fase MICRO sin
  duplicados, over-allocation, stuck intents o lag.
- `PRODUCTION_READY`: canaries completados, SLO medidos, JFR/heap/pool/locks
  dentro de presupuesto y rollback ensayado.

Estado actual final: `AUDIT_ONLY`.
