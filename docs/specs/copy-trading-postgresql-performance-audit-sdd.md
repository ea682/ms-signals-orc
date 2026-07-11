# SDD: auditoria PostgreSQL del copy trading real

Estado: especificado antes de abrir QA; ciclo ejecutado y reportado
Fecha: 2026-07-10
Servicio principal: `ms-signals-orc`
Servicio relacionado: `ms-binance-engine`
Prioridad: integridad financiera > latencia > throughput
Clasificacion resultante: `AUDIT_ONLY`

Evidencia resultante:

- identidad/read-only del rol confirmados;
- PostgreSQL QA 16.10, 94 GB, 84 tablas y 742 indices;
- migration `202607100002` aplicada y validada en QA;
- EXPLAIN y percentiles JDBC SELECT capturados;
- 14 tests locales ejecutados, 16 escenarios cubiertos;
- tablas LIVE QA vacias, por lo que canary readiness no queda demostrada.

## 1. Objetivo

Obtener evidencia reproducible y exclusivamente read-only sobre la capacidad de
PostgreSQL QA para sostener el camino MICRO_LIVE/LIVE: resolucion runtime,
idempotencia durable, budget/reservas, persistencia de operation/event y
reconciliacion. La auditoria separa evidencia medida de inferencias y no declara
performance Binance a partir de SQL o CPU local.

El ciclo de esta etapa es:

```text
INVESTIGATE -> SPECIFY -> BASELINE -> EXPLAIN -> ANALYZE
-> RECOMMEND -> VALIDATE -> REPORT
```

## 2. Alcance

Incluye:

- identidad y permisos del rol QA;
- inventario de servidor, schema, tablas e indices;
- estadisticas de tablas, indices, columnas y autovacuum;
- configuracion PostgreSQL relacionada con performance;
- sesiones, locks, waits y transacciones largas;
- `pg_stat_statements` cuando este instalado y accesible;
- Hikari y capacidad teorica por replicas;
- trazado SQL desde repositorios/servicios Java;
- EXPLAIN seguro y benchmarks repetidos de SELECT;
- comparacion de migration pendiente contra QA;
- plan de pruebas de escritura en PostgreSQL local/Testcontainers.

No incluye ejecucion real de Binance, DDL/DML QA, migration, VACUUM, ANALYZE,
reset de stats, cancelacion de sesiones ni reparacion de datos.

## 3. Restricciones read-only

La conexion se carga localmente desde `.env.audit`, ignorado por Git. El rol
esperado es `copy_audit`. No se documentan password, URL completa, API keys ni
valores de tablas de credenciales.

Cada sesion debe usar:

```sql
SET application_name = 'copy_performance_audit';
SET default_transaction_read_only = on;
SET statement_timeout = '15s';
SET lock_timeout = '2s';
SET idle_in_transaction_session_timeout = '30s';
SET search_path = futuros_operaciones, public;
BEGIN READ ONLY;
-- SELECT / SHOW / catalog / safe EXPLAIN only
ROLLBACK;
```

Antes de medir se verifica identidad, database, schema, flags no privilegiados,
`default_transaction_read_only=on` y `transaction_read_only=on`. Cualquier
desviacion bloquea la auditoria con `AUDIT_BLOCKED_PERMISSIONS`; no existe
fallback a otro usuario.

## 4. Ambiente QA

La fuente de configuracion es `.env.audit`; la conexion es una instancia QA en
la red local y el schema esperado es `futuros_operaciones`. Version, uptime,
tamanos, settings, extensiones y cardinalidades se consideran desconocidos hasta
medirlos.

La auditoria no asume que QA replica volumen, traffic mix, cache residency,
hardware o concurrencia productiva. Cada plan se etiqueta con esa limitacion.

## 5. Flujo MICRO_LIVE

```text
source event
-> users/allocation snapshots (read out of band)
-> sizing MICRO_LIVE
-> transaction acquire
-> advisory xact lock(user, allocation, MICRO_LIVE)
-> INSERT intent ON CONFLICT DO NOTHING
-> lookup idempotency key
-> aggregate active margin/open + pending margin/reserved
-> validate max order/budget/positions
-> DISPATCHING + reservation PENDING
-> HTTP Binance engine
-> ack cumulative
-> required ledger event + operation links
-> PERSISTED/partial/reconciliation
```

El balance de cuenta no reemplaza el budget de allocation. Pending y ambiguos
consumen budget. El lock y las escrituras se prueban solo en local/Testcontainers;
QA mide exclusivamente los SELECT equivalentes.

## 6. Flujo LIVE

LIVE comparte identidad, intent, ledger y recovery, pero usa exposure sizing y
no hereda 20/100/5. El hot path no consulta metricas remotas: snapshots se
refrescan fuera del evento. PostgreSQL participa en source state, intent,
operation/event y recovery.

La auditoria debe distinguir una query requerida por integridad de una query
evitable en el hot path. No se propone retirar claim, constraints, reservation o
ledger para reducir latencia.

## 7. Tablas del hot path

Nombres confirmados en codigo/migrations, sujetos a existencia QA:

| Tabla | Uso |
|---|---|
| `copy_dispatch_intent` | claim, idempotencia, reservation, ack y recovery |
| `copy_operation` | posicion copiada activa y margen usado |
| `copy_operation_event` | ledger acumulativo y PnL/auditoria |
| `user_copy_allocation` | allocations activas y snapshot runtime |
| `futures_position` o equivalente mapeado | source position/origin state |
| `detail_user` / entidades de usuario | capital/config y snapshot users |
| `operation_movement_event` si existe | movimiento source/dedupe |
| outbox/jobs relacionados | persistencia asincrona y workers |

Las tablas reales adicionales se descubren primero mediante
`information_schema` y `pg_catalog`. Las tablas de API key solo se inventarian
por estructura y stats; nunca se seleccionan secretos.

## 8. Queries del hot path

Queries confirmadas antes de QA:

| Query | Fuente Java | Frecuencia esperada | Indice esperado |
|---|---|---|---|
| insert intent if absent | `CopyDispatchIntentRepository.insertIfAbsent` | cada first dispatch | unique idempotency |
| lookup idempotency | derived repository method | cada acquire/replay | unique idempotency |
| budget aggregate | `loadBudgetSnapshot` | first MICRO dispatch | allocation+mode+reservation + active operation |
| runtime allocations | `findAllActiveRuntimeAllocations` | job cada 5 s | predicado active/real |
| event progress | `findDispatchProgress` | cada cumulative progress | unique dispatch+event+qty/result |
| clientOrderId intents | `findAllByClientOrderId` | persistence fallback | client order index |
| reconciliation batch | `findReconciliationIdsForUpdateSkipLocked` | worker cada 5 s | status+next/updated |
| intent by PK | JPA `findById` | mutacion/recovery | PK |
| operation by dispatch | JPA/index migration | persist/recovery | unique dispatch intent |
| event by dispatch | ledger/recovery | persist/recovery | dispatch progress/index |

Los INSERT, UPDATE, advisory lock y `FOR UPDATE SKIP LOCKED` solo se explican
desde codigo/migration en QA. Su comportamiento concurrente se valida localmente.

## 9. Invariantes financieras

1. Una source identity produce como maximo un send por allocation.
2. Allocations distintas no comparten idempotency key ni budget.
3. Intent durable precede al HTTP Binance.
4. MICRO reservation precede a la autorizacion de send.
5. `activeMargin + pendingMargin <= allocationBudget` bajo concurrencia.
6. `openPositions + reservedPositions` respeta el limite MICRO.
7. Ambiguo conserva reservation PENDING.
8. Rechazo definitivo con qty cero libera; con qty positiva conserva efecto.
9. `PERSISTED` exige order, operation, event y progreso enlazados.
10. Partial aplica solo `max(0, cumulative - persisted)`.
11. `MANUAL_REVIEW` nunca autoriza send ni libera ambiguo automaticamente.
12. Reconciliador reclama una vez por lease y nunca llama create order.
13. Terminales no vuelven a DISPATCHING.
14. Indices/constraints no se debilitan por performance.

## 10. Hipotesis de performance

| ID | Hipotesis | Evidencia QA |
|---|---|---|
| H1 | unique idempotency resuelve replay con index lookup | EXPLAIN + repeated latency |
| H2 | budget pending filtra por allocation/mode sin scan global | EXPLAIN + buffers |
| H3 | active operation margin usa indice parcial correcto | plan + predicate match |
| H4 | reconciliation order puede usar status/time index | plan preliminar y table stats |
| H5 | dispatch/event links evitan scans | plans por UUID/client/order |
| H6 | runtime allocation refresh es pequeno y cacheable | rows, plan, p95 |
| H7 | tables vacias ocultan planes futuros | row counts/stats y clasificacion cauta |
| H8 | pool 40 por replica puede exceder capacidad agregada | max_connections + topology input |
| H9 | autovacuum puede requerir tuning en tablas append/update hot | dead tuples + timestamps/counts |
| H10 | indices solapados agregan write cost | definitions/sizes/scans, sin drop automatico |

## 11. Plan de medicion

1. Handshake/identidad y settings de rol.
2. Primera conexion y primera SELECT trivial.
3. Inventario servidor/schema/relations/extensions/settings.
4. Tabla stats y sizes sin `COUNT(*)` indiscriminado.
5. Index definitions, validity, usage e IO.
6. Column statistics sin valores sensibles.
7. Sessions/locks/waits/blocking chains.
8. `pg_stat_statements` filtrado, si disponible.
9. EXPLAIN sin ANALYZE de queries acotadas.
10. EXPLAIN ANALYZE solo de SELECT revisados.
11. Repeated SELECT benchmark con warmup y percentiles cliente.
12. Comparacion migration pendiente vs catalog.

Cada bloque usa una sesion corta, transaction READ ONLY y ROLLBACK. PGPASSWORD
y PGOPTIONS son variables temporales y se eliminan al terminar.

## 12. Plan de EXPLAIN

Queries prioritarias:

- PK, idempotency, clientOrderId y Binance orderId de intent;
- operation/event por dispatch intent;
- state/backlog y terminal inconsistencies;
- budget active+pending y position counts;
- allocation activa por wallet/user y snapshot completo;
- partial progress;
- reconciliation/recovery ordenado por antiguedad.

Primero:

```sql
EXPLAIN (VERBOSE, SETTINGS, FORMAT TEXT) SELECT ...;
```

Solo con plan acotado:

```sql
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, FORMAT TEXT) SELECT ...;
```

No se usa ANALYZE sobre CTE/funciones desconocidas, lock queries, DML ni
`FOR UPDATE`. Se registran estimates/actuals, loops, filters, buffers, temp,
sort, planning y execution time.

## 13. Riesgos

- QA puede estar vacio o tener stats reiniciadas recientemente.
- `pg_stat_statements` puede no estar instalado/preloaded.
- Rol read-only puede no ver todas las vistas de stats pese al grant esperado.
- EXPLAIN ANALYZE altera cache y consume recursos aunque sea SELECT.
- `pg_stat_user_tables` es estimado y puede estar stale.
- Indice con cero scans no implica indice inutil.
- Schema/code pueden estar en distinta migration version.
- Pool configurado no revela replicas efectivas sin inventario de deployment.
- Column stats pueden contener valores sensibles; no se imprimen MCV/histograms
  para tablas de usuario/API keys.
- Un plan rapido con cero rows no es prueba de escala.

## 14. Criterios de aceptacion

- Identidad exacta y rol no privilegiado verificados.
- Todas las sesiones read-only con timeout y rollback.
- Cero DDL/DML y cero sesiones abandonadas.
- Inventario, table/index stats, locks y settings capturados.
- Queries criticas mapeadas a codigo e indice esperado.
- Plans clasificados GOOD/ACCEPTABLE/NEEDS_ATTENTION/CRITICAL con contexto.
- Percentiles SELECT reproducibles, sin mezclarlos con HTTP/Binance.
- Migration pendiente comparada sin aplicarla.
- Gaps de Testcontainers/deploy/canary explicitos.
- Reportes no contienen secretos.

## 15. Limitaciones del entorno

QA permite medir lectura y catalogo. No permite demostrar:

- INSERT ON CONFLICT race;
- advisory lock contention;
- reservation atomicity;
- SKIP LOCKED entre workers;
- deadlock/lock timeout/pool exhaustion;
- rollback tras ledger/crash;
- WAL/write amplification;
- latency de HTTP interno o Binance;
- comportamiento con cardinalidad no presente en QA.

Esas pruebas pertenecen a PostgreSQL local/Testcontainers o a canary autorizado.

## 16. Evidencia necesaria

Artifacts requeridos:

- baseline servidor/schema/tables;
- index report con validity/overlap/context;
- query report con code trace/plans/latency;
- lock report;
- final report medido vs proyectado;
- runbook repeatable;
- SQL read-only sin credenciales;
- outputs resumidos de identidad, settings, plans y percentiles;
- lista de pruebas Testcontainers pendientes;
- post-deploy validation para migration.

## 17. Separacion de latencias

| Dominio | Medicion de esta etapa | No inferir |
|---|---|---|
| CPU local Java | benchmark existente, solo contexto | DB/network latency |
| PostgreSQL | connect, first/warm SELECT, plans y buffers | write concurrency desde SELECT |
| HTTP ORC -> Binance engine | fuera de esta auditoria | a partir de SQL |
| Red Binance | prohibida | a partir de CPU/QA |
| Persistencia post-ack | plans SELECT y diseno transaccional | p95 write sin Testcontainers/canary |

La clasificacion maxima sin migration aplicada, multi-replica tests y canary es
`AUDIT_ONLY`, aunque todos los SELECT de QA sean rapidos.
