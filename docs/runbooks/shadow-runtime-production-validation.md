# Runbook de validacion productiva SHADOW runtime

## 1. Alcance y reglas

Este runbook valida el flujo SHADOW -> MICRO_LIVE -> LIVE despues del cambio
`copy-trading-shadow-runtime-reliability-sdd.md`.

Reglas obligatorias:

- no enviar ordenes Binance de prueba;
- no imprimir credenciales ni payloads privados;
- MICRO_LIVE mantiene 100 USDC por user+wallet y leverage 5x, distribuidos
  proporcionalmente, sin monto fijo por operacion ni maximo global de posiciones;
- REDUCE y CLOSE no usan presupuesto MICRO_LIVE;
- LIVE no toma el lock de presupuesto MICRO_LIVE;
- SHADOW nunca autoriza una orden Binance;
- una colision de idempotency key y payload siempre bloquea;
- el canary se detiene ante una posible orden duplicada.

## 2. Preflight

Verificar en el artefacto:

```powershell
jar tf target/ms-signals-orc-*.jar | Select-String `
  'V202607110003__hyperliquid_dedupe_payload_fingerprint.sql|V202607110004__shadow_event_profile_decision_aggregate_index.sql|V202607110005__shadow_position_last_accepted_event_at.sql|V202607110006__shadow_dead_letter_position_replay_order.sql'
```

Verificar que Flyway aplique:

1. `V202607110003`: columna `payload_fingerprint`.
2. `V202607110004`: indice concurrente, fuera de transaccion.
3. `V202607110005`: columna/backfill `last_accepted_event_at`.
4. `V202607110006`: indice concurrente de replay por posicion.

SQL read-only posterior al despliegue:

```sql
select column_name, data_type
from information_schema.columns
where table_schema = 'futuros_operaciones'
  and ((table_name = 'hyperliquid_direct_ingest_dedupe'
        and column_name = 'payload_fingerprint')
    or (table_name = 'shadow_position_state'
        and column_name = 'last_accepted_event_at'));

select indexname, indexdef
from pg_indexes
where schemaname = 'futuros_operaciones'
  and indexname in (
    'ix_shadow_event_profile_decision_aggregate',
    'ix_shadow_event_allocation_rolling_coverage',
    'ix_shadow_dead_letter_position_replay_order'
  );
```

No borrar la columna ni los indices durante rollback: son aditivos y compatibles
con la version anterior.

## 3. Canary

Desplegar primero una replica y observar al menos dos ciclos completos de:

- refresh de metricas/joyas;
- syncDistribution;
- promocion SHADOW/MICRO_LIVE;
- reconciliacion de ordenes;
- replay de dead letters.

No ampliar el canary si aparece cualquiera de estos eventos:

```text
reasonCode=IDEMPOTENCY_KEY_PAYLOAD_CONFLICT
reasonCode=POSTGRES_DEADLOCK_RETRIES_EXHAUSTED
reasonCode=SHADOW_RECOVERY_PERSIST_FAILED_AFTER_LIVE_DISPATCH
reasonCode=SHADOW_COVERAGE_QUERY_FAILED
reasonCode=DISTRIBUTED_DUPLICATE_PAYLOAD_UNVERIFIED
event=copy.integrity.violation
```

## 4. Coverage rolling

Secuencia sana:

```text
event=shadow.coverage.query.started reasonCode=SHADOW_COVERAGE_QUERY_STARTED
event=shadow.coverage.query.succeeded reasonCode=SHADOW_COVERAGE_QUERY_OK fallbackUsed=false
```

No debe reaparecer:

```text
Cannot project java.time.Instant to java.time.OffsetDateTime
SHADOW_COVERAGE_ROLLING_QUERY_FAILED
```

Metricas:

```text
shadow_coverage_query_total{result="success",source="rolling"}
shadow_coverage_query_duration
shadow_coverage_fallback_total
```

En modo ROLLING normal, el incremento de fallback debe ser cero. Un fallo de
query bloquea promocion de forma fail-closed; no usa historico silenciosamente.

## 5. Unidades de distribucion

Por cada profile se espera:

```text
event=copy.distribution.unit.started
event=copy.distribution.unit.completed result=SUCCESS
```

Los campos `userId`, `walletId`, `profileKey`, `lockKey`, `lockWaitMs`,
`transactionMs`, rows y `retryCount` deben estar presentes.

Un fallo aislado debe mostrar:

```text
event=copy.distribution.unit.failed
event=user_copy_allocation.sync_partial result=PARTIAL
```

No debe aparecer `sync_ok` para el mismo ciclo parcial.

Metricas:

```text
copy_distribution_unit_total{result="success|failure"}
copy_distribution_transaction_duration{result="success|failure"}
```

## 6. Deadlocks

Retry recuperable:

```text
event=postgres.deadlock.retry reasonCode=POSTGRES_DEADLOCK_RETRY sqlState=40P01 result=RETRYING
```

Agotamiento, siempre alerta:

```text
event=postgres.deadlock.exhausted reasonCode=POSTGRES_DEADLOCK_RETRIES_EXHAUSTED shouldAlert=true
```

Revisar acumulado PostgreSQL, tomando delta desde el inicio del canary:

```sql
select datname, deadlocks, xact_commit, xact_rollback
from pg_stat_database
where datname = current_database();
```

El canary no avanza con un exhausted. Los retries aislados pueden recuperarse;
una tasa creciente requiere revisar `profileKey`, `lockWaitMs` y orden de lock.

## 7. Dedupe distribuido

Replay sano entre replicas:

```text
event=hyperliquid.direct_ingest.distributed_duplicate
reasonCode=DISTRIBUTED_DUPLICATE_SUPPRESSED
diagnosticSeverity=INFO
decision=NOOP
expected=true
shouldAlert=false
copyImpact=NO_DUPLICATE_ORDER
```

Colision no sana:

```text
event=hyperliquid.direct_ingest.idempotency_payload_conflict
reasonCode=IDEMPOTENCY_KEY_PAYLOAD_CONFLICT
decision=BLOCK
shouldAlert=true
copyImpact=ORDER_NOT_SENT
```

Fila legacy sin fingerprint, fail-closed:

```text
event=hyperliquid.direct_ingest.distributed_duplicate_unverified
reasonCode=DISTRIBUTED_DUPLICATE_PAYLOAD_UNVERIFIED
decision=NOOP expected=false shouldAlert=true copyImpact=NO_ORDER_SENT
```

Metricas:

```text
distributed_duplicate_total{result="duplicate"}
distributed_duplicate_total{result="payload_conflict"}
```

`payload_conflict` debe ser cero. Un `duplicate` sano no crea una segunda
`copy_dispatch_intent` ni una segunda orden.

`payload_unverified` tampoco envia orden, pero requiere revisar/backfillear la
fila; no debe contabilizarse como duplicado sano.

## 8. Cola SHADOW y DLQ

Metricas principales:

```text
shadow_queue_delay
shadow_processing_duration{stage="total"}
shadow_db_persist_duration
signals.copy.shadow.async.queue.depth
signals.copy.shadow.async.deferred.total
shadow_deadletter_replay_total
```

Evento lento:

```text
event=shadow.processing.slow reasonCode=SHADOW_PROCESSING_SLOW
```

Cuando la cola se llena, el orden correcto es:

1. dispatch LIVE terminado;
2. `shadow_dead_letter_recorded`;
3. ingest distribuido marcado como procesado;
4. `shadow.deadletter.replay.completed`.

Backlog read-only:

```sql
select status, count(*) as events,
       min(first_failed_at) as oldest,
       max(attempt_count) as max_attempts
from futuros_operaciones.shadow_event_dead_letter
group by status
order by status;
```

`RECOVERABLE` puede crecer durante una rafaga, pero debe volver a cero. Un
`REPLAYING` mas antiguo que el lease de 60 segundos se reclama nuevamente.
No se descartan eventos por maximo de intentos.

Solo el evento pendiente mas antiguo de cada `position_key` puede estar en
`REPLAYING`. Detectar una violacion:

```sql
select position_key, count(*)
from futuros_operaciones.shadow_event_dead_letter
where status = 'REPLAYING'
group by position_key
having count(*) > 1;
```

La consulta debe devolver cero filas. Posiciones diferentes pueden reproducirse
en paralelo.

## 9. MICRO_LIVE

Para OPEN/INCREASE deben observarse:

```text
executionMode=MICRO_LIVE
maxWalletMarginUsd=100
targetLeverage=5
fixedPerOperation=absent
userMaxConcurrentPositions=<null-or-configured-value>
```

Reasons de rechazo permitidos:

```text
MICRO_LIVE_TOTAL_MARGIN_EXCEEDED
SKIPPED_USER_POSITION_LIMIT
MICRO_LIVE_INSUFFICIENT_AVAILABLE_BALANCE
MICRO_LIVE_DUPLICATE_INTENT
MICRO_LIVE_GUARD_BLOCKED
MICRO_LIVE_SYMBOL_NOT_ALLOWED
MICRO_LIVE_MIN_NOTIONAL_NOT_REACHED
```

No aceptar que se reemplacen por `COPY_DISPATCH_ALREADY_REJECTED` en un intent
nuevo.

Para salidas:

```text
budgetCheck=SKIPPED_FOR_REDUCE_OR_CLOSE
decision=ALLOW
reasonCode=MICRO_LIVE_EXIT_ALWAYS_ALLOWED
microLiveBudgetLockAcquired=false
```

## 10. LIVE

Cada intent LIVE debe declarar:

```text
budgetMode=LIVE_UNRESTRICTED_BY_MICRO_LIMITS
microLiveBudgetLockAcquired=false
```

La separacion SHADOW/LIVE se confirma con:

```text
event=hyperliquid.direct_ingest.hot_path_completed
liveImpact=LIVE_DISPATCH_NOT_BLOCKED_BY_SHADOW
```

No usar una orden real para esta validacion. Confirmar sobre trafico canary
legitimo y reconciliar por `clientOrderId`/`binanceOrderId`.

## 11. Performance

Objetivos iniciales de canary, comparados con el baseline de la misma carga:

- cero regresion p95 en dispatch LIVE;
- `shadow_queue_delay` p95 decreciente y sin crecimiento sostenido;
- `shadow_db_persist_duration` p95 menor al baseline de 600-800 ms;
- cero deadlocks agotados;
- cero payload conflicts;
- backlog DLQ drenado;
- cero replays concurrentes para el mismo `position_key`;
- sin aumento de conexiones activas hasta el limite de Hikari.

Revisar el plan del aggregate despues de crear el indice:

```sql
explain (analyze, buffers)
select count(*) filter (where decision='SIMULATED'),
       count(*) filter (where decision='RECORDED'),
       count(*) filter (where decision='SKIPPED'),
       count(*) filter (where decision='DUPLICATE'),
       count(*) filter (where decision='ERROR')
from futuros_operaciones.shadow_copy_operation_event
where wallet_profile_id = :profile_id
  and decision in ('SIMULATED','RECORDED','SKIPPED','DUPLICATE','ERROR');
```

## 12. QA de escritura

Ejecutar solo contra la base dedicada `copy_trading_test`:

```powershell
./mvnw.cmd "-Dtest=CopyTradingPostgresConcurrencyTest" `
  "-Dcopy.postgres.test.jdbc-url=$env:COPY_TEST_DB_URL" `
  "-Dcopy.postgres.test.username=$env:COPY_TEST_DB_USER" `
  "-Dcopy.postgres.test.password=$env:COPY_TEST_DB_PASSWORD" test
```

La rafaga durable de 100, 1.000 y 10.000 eventos se ejecuta tres veces en QA;
reportar la mediana de throughput, p50/p95/p99, queue delay, db persist,
deadlocks, DLQ y filas perdidas. No ejecutar esa rafaga sobre produccion.

## 13. Rollback

1. Detener expansion del canary.
2. Conservar evidencia de logs, metricas e intents ambiguos.
3. Reconciliar intents `RECONCILING`, `PERSISTENCE_PENDING` y `MANUAL_REVIEW`.
4. Volver a la imagen anterior de `ms-signals-orc`.
5. Mantener columna e indices nuevos; son compatibles y su eliminacion agrega
   riesgo sin restaurar comportamiento.
6. No reenviar una orden ambigua. Consultar Binance por el mismo
   `clientOrderId`.
7. Si el replay causa presion extrema, pausarlo temporalmente solo despues de
   confirmar que los rows siguen en `RECOVERABLE`; nunca borrar la DLQ.

El rollback queda completo cuando LIVE opera con la imagen previa, no quedan
intents ambiguos sin clasificar y el backlog SHADOW permanece durable.
