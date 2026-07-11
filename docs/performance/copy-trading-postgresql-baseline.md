# Baseline PostgreSQL QA para copy trading

Fecha: 2026-07-10
Clasificacion de este artifact: evidencia read-only
Fuente de conexion: `.env.audit` local, ignorado por Git
Rol: `copy_audit`
Schema: `futuros_operaciones`

## 1. Gate de seguridad

| Control | Resultado |
|---|---|
| Usuario efectivo | `copy_audit` |
| Base | `trading_futuros` |
| Superusuario | no |
| CREATEDB / CREATEROLE / REPLICATION | no / no / no |
| `default_transaction_read_only` | `on` |
| `transaction_read_only` | `on` dentro de cada bloque |
| `statement_timeout` | 15 s |
| `lock_timeout` | 2 s |
| `idle_in_transaction_session_timeout` | 30 s |
| SELECT en tablas actuales | 84/84 |
| SELECT en secuencias actuales | 11/11 |
| `pg_read_all_stats` | si |
| Sesiones de auditoria abandonadas | 0 |

Todas las consultas QA se ejecutaron con `BEGIN READ ONLY` y `ROLLBACK`.
No se aplico DDL, DML, migration, VACUUM, ANALYZE ni una orden Binance.

## 2. Servidor y capacidad

| Metrica | Medido |
|---|---:|
| PostgreSQL | 16.10 (Debian) |
| Inicio del postmaster | 2026-06-29 02:15:55 UTC |
| Uptime al medir | 11 dias 16 h 24 min |
| Base | 94 GB (100.492.808.675 bytes) |
| Schema | 93 GB (100.191.682.560 bytes) |
| Tablas | 84 |
| Indices | 742 |
| Materialized views | 0 |
| Secuencias | 11 |
| Extensiones | `pgcrypto`, `plpgsql`, `uuid-ossp` |
| `pg_stat_statements` | no instalado |

## 3. Settings relevantes

| Setting | Valor | Lectura |
|---|---:|---|
| `max_connections` | 100 | limite compartido por todos los servicios |
| `shared_buffers` | 128 MB | muy pequeno frente a 94 GB; requiere evidencia host antes de cambiar |
| `effective_cache_size` | 4 GB | estimacion conservadora para el planner |
| `work_mem` | 4 MB | por nodo/sort, no por conexion |
| `maintenance_work_mem` | 64 MB | default |
| `random_page_cost` | 4 | default, revisar segun storage real |
| `effective_io_concurrency` | 1 | default conservador |
| `checkpoint_timeout` | 5 min | default |
| `max_wal_size` | 1 GB | configurado |
| `wal_compression` | off | no cambiar sin medir CPU/WAL |
| `autovacuum` | on | 3 workers, naptime 60 s |
| `track_io_timing` | off | impide atribuir tiempo fisico de I/O |
| `max_parallel_workers_per_gather` | 0 | paralelismo de query deshabilitado |
| `log_lock_waits` | off | limita diagnostico historico de waits |

No se recomienda cambiar estos valores solo con esta captura. El host, RAM,
storage, WAL y carga mixta no fueron inventariados con una cuenta del sistema.

## 4. Conexiones, waits y actividad

Fotografia inicial:

| Metrica | Valor |
|---|---:|
| Sesiones observadas | 39 |
| Client backends | 34 |
| Activas | 1 (la auditoria) |
| Idle | 33 |
| Idle in transaction | 0 |
| Bloqueadas | 0 |
| Transacciones mayores a 1 min | 0 |
| Advisory locks | 0 |

Habia 22 sesiones idle identificadas solo como `PostgreSQL JDBC Driver` y
varias conexiones pgAdmin. Al no existir `ApplicationName` por servicio no se
puede atribuir el consumo de pool a un microservicio especifico.

Fotografia final: 0 sesiones de auditoria remanentes, 0 bloqueados y 0
transacciones mayores a un minuto.

## 5. Estadisticas globales

Stats desde el restart indicado:

| Metrica | Valor |
|---|---:|
| Commits | 83.896.725 |
| Rollbacks | 2.750 |
| Cache hit global | 73,908% |
| Temp files | 4.371.646 |
| Temp bytes acumulados | 105 TB |
| Deadlocks acumulados | 2.178 |
| Sessions abandoned | 22 |
| Conflicts | 0 |

Estos valores son senales globales, no una atribucion al copy trading. Sin
`pg_stat_statements`, `track_io_timing`, application names y ventanas de carga
no existe evidencia para identificar la query o servicio responsable.

## 6. Cardinalidad y tamanos del camino real

| Relacion | Rows estimadas / exactas seguras | Heap | Indices | Total |
|---|---:|---:|---:|---:|
| `copy_dispatch_intent` | exactas 0 | 0 B | 64 kB | 72 kB |
| `copy_operation` | exactas 0 | 0 B | 184 kB | 192 kB |
| `copy_operation_event` | exactas 0 | 0 B | 184 kB | 192 kB |
| `user_copy_allocation` | exactas 2 | 8 kB | 960 kB al primer muestreo | 1008 kB |
| `users` | exactas 1 | 8 kB | 48 kB | 56 kB aprox. |
| `detail_user` | exactas 1 | 64 kB | 32 kB | 136 kB |
| `user_api_keys` | exactas 1 | 8 kB | 0 B | 16 kB aprox. |
| `futures_position` | estimadas 212.913 | 325 MB | 969 MB | 2.701 MB |
| `operation_movement_event_dedupe` | estimadas 8.635.537 | 3.475 MB | 1.258 MB | 4.734 MB |
| `operation_movement_event_2026_06` | estimadas 5.737.445 | 6.333 MB | 7.383 MB | 25 GB |
| `operation_movement_event_2026_07` | estimadas 1.323.126 | 1.625 MB | 1.844 MB | 7.140 MB |

Los tres ledgers LIVE estan fisicamente vacios. Sus planes prueban compatibilidad
de predicados/indices, no performance con cardinalidad productiva.

## 7. Autovacuum, stats y wraparound

| Relacion | Live | Dead | Dead % | Observacion |
|---|---:|---:|---:|---|
| `operation_movement_event_2026_06` | 5.827.352 | 416.350 | 6,67% | sin autovacuum desde restart; bajo threshold default de 20% |
| `operation_movement_event_2026_07` | 1.345.445 | 82.211 | 5,76% | autovacuum 1.663 veces |
| `futures_position` | 212.940 | 26.040 | 10,90% | tabla update-hot; autovacuum activo |
| `user_copy_allocation` | 2 | 20 | 90,91% | porcentaje alto pero volumen absoluto minimo |
| `copy_dispatch_intent` | 0 | 0 | n/a | sin evidencia de churn LIVE |
| `copy_operation` | 0 | 0 | n/a | stats de columnas antiguas pese a heap vacio |
| `copy_operation_event` | 0 | 0 | n/a | stats de columnas antiguas pese a heap vacio |

Edad de la base: 79.433.479 XID y 7 MXID, sin riesgo inmediato de wraparound.
No esta instalada una extension de bloat; el bloat real no fue medido. La
discrepancia entre heap vacio y stats de columnas en tablas LIVE exige ANALYZE
post-carga por el proceso operativo, no por esta auditoria.

## 8. Latencia JDBC read-only

Cliente Java 21 + pgjdbc 42.7.11, una conexion reutilizada salvo donde se
indica. Todos son SELECT. El tiempo incluye red cliente/servidor JDBC.

| Benchmark | n | p50 ms | p75 | p90 | p95 | p99 | max |
|---|---:|---:|---:|---:|---:|---:|---:|
| Handshake de conexion | 30 | 16,383 | 17,237 | 19,787 | 20,195 | 37,905 | 37,905 |
| Primer `SELECT 1` en conexion nueva | 30 | 1,573 | 1,682 | 1,810 | 1,882 | 1,893 | 1,893 |
| `SELECT 1` warm | 1000 | 1,208 | 1,250 | 1,301 | 1,341 | 1,483 | 8,000 |
| Intent por idempotency, miss | 500 | 1,218 | 1,247 | 1,287 | 1,318 | 1,377 | 1,461 |
| Budget aggregate vacio | 500 | 1,278 | 1,324 | 1,383 | 1,447 | 2,725 | 9,882 |
| Snapshot allocations, 2 rows | 500 | 1,394 | 1,442 | 1,504 | 1,552 | 1,721 | 9,033 |
| Allocation por wallet | 500 | 1,391 | 1,434 | 1,469 | 1,525 | 1,643 | 7,883 |
| Operation por dispatch, miss | 500 | 1,203 | 1,235 | 1,274 | 1,301 | 1,334 | 1,555 |
| Event por dispatch, miss | 500 | 1,221 | 1,259 | 1,298 | 1,334 | 1,414 | 9,294 |
| Reconciliation filter vacio | 500 | 1,201 | 1,223 | 1,257 | 1,293 | 1,350 | 9,662 |
| Movement dedupe, hit real | 1000 | 1,204 | 1,229 | 1,271 | 1,302 | 1,404 | 7,054 |
| Latest position preparado | 500 | 1,761 | 1,845 | 1,911 | 2,000 | 2,578 | 6,795 |
| Latest position SQL no preparado | 30 | 4,666 | 5,198 | 6,275 | 6,461 | 6,927 | 6,927 |
| Root snapshot usuarios | 500 | 1,274 | 1,319 | 1,368 | 1,410 | 2,147 | 7,802 |
| Detail user lookup | 500 | 1,330 | 1,371 | 1,454 | 1,590 | 2,481 | 9,318 |
| API key lookup | 500 | 1,340 | 1,415 | 1,652 | 2,087 | 3,359 | 9,506 |

La diferencia handshake/warm confirma que Hikari evita unos 16-38 ms por
operacion. No hay evidencia de saturacion del pool durante esta captura.

## 9. Limites del baseline

- QA no tiene rows LIVE en intent/operation/event.
- Los percentiles de miss no representan heap/index con millones de intents.
- No se midieron INSERT/UPDATE/WAL en QA.
- No se midio HTTP ORC -> Binance engine ni red Binance.
- `pg_stat_statements` e I/O timing no estan disponibles.
- El benchmark local de concurrencia uso PostgreSQL 18, no QA 16.10.
