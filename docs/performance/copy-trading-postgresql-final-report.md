# Auditoria final PostgreSQL MICRO_LIVE/LIVE

Fecha: 2026-07-10
Commit auditado: `9b0a831` (`feature/1.4.27`)
Clasificacion final: **AUDIT_ONLY**

## 1. Veredicto

La conexion QA se cargo desde `.env.audit`, el rol efectivo fue `copy_audit`,
no es superusuario y todas las sesiones quedaron read-only con timeout y
rollback. No hubo escrituras QA ni acciones Binance.

La base tiene la migration de intent y la extension `MANUAL_REVIEW` aplicadas y
validas. Los contratos de idempotencia/budget/reconciliation estan bien
orientados y los 16 escenarios de concurrencia/fallo pasaron en PostgreSQL
local.

No se declara canary-ready porque las tres tablas LIVE estan vacias en QA, falta
la ejecucion multi-replica real en PostgreSQL 16/CI, no hay metricas canary del
flujo y existe un hallazgo medido de fan-out por particiones antes del dispatch.

## 2. Evidencia principal

| Tema | Resultado |
|---|---|
| PostgreSQL | 16.10 |
| Base/schema | 94 GB / 93 GB |
| Schema | 84 tablas, 742 indices |
| LIVE rows | intent=0, operation=0, event=0 |
| Source rows | dedupe ~8,8 M; junio ~5,8 M; julio ~1,35 M |
| Indices invalidos/not-ready | 0 / 0 |
| Duplicados exactos | 4 pares |
| Locks actuales | 0 bloqueados; muestra transitoria posterior de 4, resuelta al reintentar |
| Historical DB | 2.178 deadlocks, 105 TB temp, 4,37 M temp files |
| `pg_stat_statements` | no disponible |
| Migration `202607100002` | aplicada por Flyway, success, constraint valid |
| Tests locales | 14 tests, 0 fallos, 0 skips; 16 casos cubiertos |

## 3. Lo que esta correcto

- Claim durable antes de Binance con unique idempotency.
- Payload mismatch bloqueado y replay sin resend.
- Budget MICRO serializado por user+allocation+mode.
- Pending ambiguo conserva budget; rechazo definitivo libera.
- Operation/event enlazados antes de `PERSISTED`.
- Ledger cumulative progress tiene lock e indice unique compatibles.
- Reconciliation usa lease, `SKIP LOCKED` y `MANUAL_REVIEW`.
- HTTP Binance no ocurre dentro de la transaccion de claim del batch.
- Allocations/users/metricas se resuelven desde snapshot en el evento.
- Migration de `MANUAL_REVIEW` e idempotencia legacy esta aplicada en QA.

## 4. Hallazgos

### P0: latest movement fan-out

El lookup de previous movement toca 38 particiones, o 28 con upper bound. Una
muestra real ejecuto en ~0,9 ms, pero el planning frio fue 22-25 ms y reviso mas
de 16 mil buffers de catalogo/indices. El JDBC preparado quedo p95 2,0 ms; SQL
no preparado p95 6,461 ms con catalogo caliente.

QA ademas difiere del migration versionado: tiene
`(position_key,event_time DESC)` y no el trailing `date_creation DESC`. Debe
resolverse el drift y, sobre todo, el fan-out antes de atribuir la demora a
Binance.

### P1: observabilidad PostgreSQL insuficiente

El cache hit global es 73,908%, hay 105 TB temp y 2.178 deadlocks desde restart,
pero faltan `pg_stat_statements`, `track_io_timing`, logs de lock wait y
application names por servicio. Tambien se observo fugazmente un grupo de 4
bloqueados que ya no existia en la consulta de detalle. Son sintomas reales sin
attribution.

### P1: user snapshot N+1 e indices ausentes

El refresh hace `1 + 2N` queries. No existe indice por `detail_user.id_users` ni
por `user_api_keys.user_id`; la segunda tabla tampoco presenta PK/unique en
catalogo. Hoy N=1, pero el contrato no escala ni asegura cardinalidad.

### P1: tablas LIVE vacias

Los planes por dispatch operation/event usan seq scan porque el heap esta vacio.
Los indices existen, pero no hay prueba de p95 write, WAL, bloat, reconciliation
backlog ni selectividad con volumen.

### P2: indice/pool write cost

Operation y event tienen 23 indices cada una; allocation 17. Hay cuatro pares
exactamente duplicados en el schema. Se requiere workload real antes de retirar
solapamientos. Signals permite 40 conexiones por replica: con dos usa 80% de
`max_connections`; con tres excede el limite.

## 5. Planes y latencias

| Camino | Evidencia | Resultado |
|---|---|---|
| Idempotency | unique index; JDBC p95 1,318 ms | ACCEPTABLE, tabla vacia |
| Budget | pending index; JDBC p95 1,447 ms | ACCEPTABLE, ramas vacias |
| Allocation snapshot | 2 rows; JDBC p95 1,552 ms | GOOD actual |
| Movement dedupe | 8,8 M; index-only; p95 1,302 ms | GOOD |
| Latest position | 38 partitions; p95 prepared 2,0 ms; cold plan 22 ms | NEEDS_ATTENTION |
| Reconciliation | seq+sort con 0 rows; p95 1,293 ms | NO CONCLUYENTE |
| Connection handshake | p50 16,383 ms; p99 37,905 ms | pool obligatorio |

Estas latencias miden solo PostgreSQL/JDBC. No incluyen CPU de sizing, Kafka,
HTTP interno, Binance ni persistencia write post-ack.

## 6. Migracion

`V202607100002__copy_dispatch_manual_review_integrity.sql`:

- aparece en Flyway como success;
- el check de status incluye `MANUAL_REVIEW` y esta validado;
- `ix_copy_dispatch_intent_manual_review` esta valid/ready;
- `ux_copy_operation_event_legacy_client_order_id` esta valid/ready;
- tiempo Flyway registrado: 11 ms con tablas LIVE vacias.

Estado: `CODE_VALIDATED` y `DATABASE_MIGRATION_APPLIED_QA`.

Para produccion, el lock estimado no puede derivarse de 11 ms QA si event ya
tiene volumen. El `DROP/ADD CHECK` toma lock de tabla y la validacion lee rows;
la creacion de unique index no es concurrente. Planificar ventana y precheck de
duplicados. Rollback logico: conservar el schema aditivo y revertir aplicacion;
no retirar `MANUAL_REVIEW` mientras existan rows en ese estado.

## 7. Medido, proyectado y no medido

### Medido

- identidad/permisos/read-only;
- version, uptime, sizes, settings, connections, locks y stats;
- table/index stats, validity, sizes y autovacuum;
- EXPLAIN/ANALYZE SELECT seguros;
- percentiles JDBC SELECT;
- migration QA;
- concurrencia/fallos en PostgreSQL local.

### Proyectado

- pool maximo por numero de replicas;
- write amplification por cantidad de indices;
- riesgo de reconciliation con OR/sort;
- impacto de N+1 al aumentar usuarios;
- opciones de indice/current-state para latest position.

### No medido

- p95/p99 INSERT/UPDATE/WAL en PostgreSQL 16 con volumen;
- dos procesos reales de `ms-signals-orc`;
- crash/fault injection dentro del servicio y red;
- HTTP ORC -> Binance engine;
- latencia/errores/rate limit Binance;
- throughput canary y exposure financiero real;
- bloat exacto;
- origen de deadlocks/temp I/O historicos.

## 8. Requisitos siguientes

### Requiere Testcontainers/CI

1. Repetir los 14 tests con imagen PostgreSQL 16.
2. Agregar dataset de al menos 1 M intents/eventos sinteticos.
3. Comparar reconciliation actual vs partial indexes candidatos.
4. Medir write p50/p95/p99, WAL, index growth y autovacuum.
5. Fault injection real entre reservation, send, ack, ledger y persisted.

### Requiere deploy

1. Application name/pool name por servicio y replica.
2. `pg_stat_statements` e I/O timing en una ventana controlada.
3. Resolver drift/fan-out de previous movement mediante migration revisada.
4. Eliminar N+1 y definir indices/constraints de usuario/API.
5. Consolidar duplicados exactos solo despues de observar workload.

### Requiere canary

1. Una replica MICRO_LIVE con budget minimo y allowlist.
2. Alertas de pending, manual review, reconciliation age, duplicate conflict,
   DB acquire y pre-network duration.
3. Cero resend ambiguo, cero budget breach y cero persisted sin ledger.
4. Soak suficiente para autovacuum, locks y restart recovery.
5. Promover a LIVE solo despues de un canary MICRO_LIVE exitoso.

## 9. Clasificacion

**AUDIT_ONLY**

No se cumplen aun los requisitos para `POSTGRES_CANARY_READY`,
`MICRO_LIVE_CANARY_READY` ni `LIVE_CANARY_READY`, aunque migration e invariantes
locales hayan pasado.
