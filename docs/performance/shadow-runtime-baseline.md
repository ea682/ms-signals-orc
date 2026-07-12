# Baseline del runtime SHADOW

Fecha: 2026-07-11

Commit: `27bc2057d456495ee5945666714a4b81533e73a2`

Estado: baseline previo a correcciones de
`copy-trading-shadow-runtime-reliability-sdd.md`.

Ordenes reales Binance: ninguna.

## 1. Entorno

### 1.1 Cliente de pruebas

| Campo | Valor |
|---|---|
| OS | Windows 11 Home Single Language 10.0.26200 |
| CPU | AMD Ryzen 7 5800H |
| Procesadores logicos | 16 |
| Memoria | 31,4 GB |
| Java | Temurin 21.0.11 LTS |
| Build | Maven Wrapper |

### 1.2 PostgreSQL observado

| Campo | Valor |
|---|---|
| Version | PostgreSQL 16.10 |
| Plataforma | Debian x86_64 |
| Inicio del servidor | 2026-06-29 02:15:55 UTC |
| Modo de medicion | queries read-only con statement timeout |
| `pg_stat_statements` | no instalado |

No se modificaron parametros, tablas ni datos durante la captura.

## 2. Datos al momento de la captura

Medicion: `2026-07-11 21:04:50 UTC`.

| Entidad | Filas |
|---|---:|
| `shadow_copy_operation_event` | 519.743 |
| `shadow_position_state` | 2.228 |
| `shadow_copy_operation` | 2.228 |
| `copy_wallet_profile` | 80 |
| allocations SHADOW activas | 35 |
| allocations MICRO_LIVE/LIVE activas | 2 |

Tres perfiles concentraban la mayor parte del ledger observado:

| profileId | strategy | eventos aproximados | principal resultado |
|---:|---|---:|---|
| 78 | MOVEMENT_ALL | 208.000+ | mayormente SKIPPED |
| 77 | SHORT_ONLY | 128.000+ | mayormente SKIPPED |
| 79 | LONG_ONLY | 116.000+ | mayormente SKIPPED |

Principales reasons: `RESIZE_WITHOUT_SHADOW_OPEN`,
`RESIZE_AFTER_SHADOW_CLOSED`, `FLIP_WITHOUT_SHADOW_OPEN` y
`SHADOW_NEW_EXPOSURE_BLOCKED_BY_RANKING_EXIT`.

## 3. Latencia observada

La tabla no contiene el monotonic enqueue timestamp. Por eso esta medicion es:

```text
sourceToPersistMs = date_creation - event_time
```

Incluye transporte, direct-ingest queue, SHADOW queue, locks, processing,
persistencia y commit. No representa queue delay puro.

Ventana de diez minutos previa a la captura:

| muestras | strategy events/s persistidos | p50 ms | p95 ms | p99 ms | max ms |
|---:|---:|---:|---:|---:|---:|
| 630 | 1,050 | 3.854,2 | 63.932,6 | 73.266,8 | 81.189,1 |

Una captura anterior de los tres perfiles mas cargados mostro:

| profileId | p50 ms | p95 ms | p99 ms | max ms |
|---:|---:|---:|---:|---:|
| 77 | 6.409,8 | 51.684,4 | 74.547,1 | 81.036,2 |
| 78 | 9.471,3 | 58.190,7 | 75.142,1 | 81.701,0 |
| 79 | 13.264,7 | 61.296,6 | 73.639,0 | 82.214,3 |

## 4. Query baseline

### 4.1 Conteo actual por decision

Query equivalente a uno de los cinco calls ejecutados por profile:

```sql
SELECT count(*)
FROM futuros_operaciones.shadow_copy_operation_event
WHERE wallet_profile_id = 78
  AND decision = 'SIMULATED';
```

`EXPLAIN (ANALYZE, BUFFERS)`:

| Campo | Resultado |
|---|---:|
| plan | sequential scan |
| filas retornadas al aggregate | 48.416 |
| filas descartadas | 467.375 |
| execution time | 140,180 ms |
| shared buffers read | 26.793 |

No existe indice por `(wallet_profile_id, decision)`. El indice de lookup
actual comienza por profile, pero continua con origin, event type, side y time,
por lo que el planner prefirio scan completo para este conteo.

### 4.2 Aggregate combinado de decisiones

Una query read-only con cinco `count(*) FILTER` en un solo scan demoro
`141,679 ms`.

La implementacion actual ejecuta cinco calls separados. Su costo secuencial
esperado para el perfil grande es cercano a `5 x 140 ms`, consistente con los
logs productivos de `dbPersistMs=600-800`.

Esta comparacion solo demuestra oportunidad. El after debe volver a medir con
el codigo real, cache comparable y tres corridas.

### 4.3 Interacciones SQL estaticas

Por estrategia no duplicada, el flujo actual incluye:

- advisory profile lock;
- advisory idempotency lock;
- exists de evento;
- uno o dos lookups de position;
- lookup adicional de ultimo CLOSED para resize sin OPEN;
- lookup y save de shadow operation cuando hay mutacion;
- save de position y event;
- save de allocation;
- lookup y save de wallet profile;
- lookup de validation;
- nueve aggregates de validation;
- save de validation;
- flush/commit.

El rango estatico es aproximadamente 18 a 25 interacciones SQL por estrategia,
segun lifecycle. El conteo exacto requiere Hibernate statistics o datasource
proxy en el harness.

## 5. Locks y concurrencia

El worker consulta representantes y adquiere locks de todos los perfiles de la
wallet antes de filtrar estrategias. Los locks son ordenados y
`pg_advisory_xact_lock`, por lo que se liberan al commit.

Consecuencias del baseline:

- posiciones distintas de una wallet compiten por los mismos profile locks;
- ocho workers no implican ocho eventos concurrentes para una wallet caliente;
- wallets distintas pueden avanzar en paralelo;
- LONG y SHORT usan positionKeys distintos y pueden llegar desde lanes
  diferentes; el profile lock serializa, pero no ordena por source timestamp.

## 6. Pool, errores y deadlocks

Estadistica acumulada de PostgreSQL:

| Campo | Valor |
|---|---:|
| commits | 92.937.058 aproximadamente |
| rollbacks | 3.399 |
| deadlocks | 2.683 |

`stats_reset` no tenia fecha. Los deadlocks son globales y acumulados desde el
arranque; no se atribuyen a SHADOW o distribution sin evidencia adicional.

No existe timer de transaction unit ni `pg_stat_statements`, por lo que el
baseline no puede informar honestamente:

- transaction p50/p95 de `syncDistribution`;
- SQL exacto responsable de cada deadlock;
- lock wait real por profile;
- conexiones activas historicas durante las rafagas.

Esos datos deben agregarse en la instrumentacion y el harness.

## 7. Metricas existentes

- `signals.copy.shadow.async.queue.depth`;
- `signals.copy.shadow.async.queue.high_water_mark`;
- `signals.copy.shadow.async.workers.active`;
- `signals.copy.shadow.async.enqueued.total`;
- `signals.copy.shadow.async.dropped.total`;
- `signals.copy.shadow.async.enqueue.duration`;
- `signals.copy.shadow.async.worker.duration`;
- `copy.deadlock.total`;
- `copy.deadlock.retry.total`;
- `copy.lock.wait`.

Faltan stage timers, queue delay puro, db persist puro, commit, coverage y
transaction unit.

## 8. Tests baseline

Comando:

```powershell
$env:JAVA_HOME='C:\Users\erika\.jdks\temurin-21.0.11'
.\mvnw.cmd "-Dtest=PostgresShadowCoverageQueryServiceTest,ShadowCoverageCalculatorTest,CopyIdempotencyKeyFactoryTest,MicroLiveBudgetPolicyTest" test
```

Resultado:

```text
Tests run: 37, Failures: 0, Errors: 0, Skipped: 0
```

La prueba de coverage usa un proxy que devuelve `OffsetDateTime`; no reproduce
el tipo `Instant` de Hibernate y por eso el baseline verde no detecta la
regresion productiva.

## 9. Benchmark faltante en baseline

El commit investigado no contiene harness SHADOW para 100, 1.000 y 10.000
eventos. No se inventan percentiles locales.

La fase RED debe crear un harness que mida:

- una wallet y multiples perfiles;
- multiples wallets;
- OPEN, INCREASE, REDUCE, CLOSE y NOOP;
- queries por strategy event;
- p50/p95/p99 de cada stage;
- throughput;
- pool y deadlocks;
- eventos persistidos, duplicados, recuperables y perdidos.

El before del harness se ejecutara contra el commit previo o una worktree
aislada. El after usara el mismo entorno, dataset y tres corridas. Se reportara
la mediana.

## 10. Medicion after del aggregate de validacion

Medicion read-only realizada el 2026-07-11 sobre PostgreSQL 16.10. El perfil
mas cargado tenia 212.228 eventos de un total aproximado de 522.288 filas.

El before reproduce los cinco `countByWalletProfileIdAndDecision` secuenciales.
El after ejecuta un solo `count(*) filter` con los cinco resultados.

| corrida | before: 5 queries ms | after: 1 aggregate ms |
|---:|---:|---:|
| 1 | 664,422 | 127,880 |
| 2 | 646,761 | 124,008 |
| 3 | 645,642 | 121,274 |
| mediana | 646,761 | 124,008 |

Resultado mediano: 5,22 veces mas rapido y 80,8% menos tiempo SQL para los
contadores de decision. No hubo escrituras ni cambios de configuracion.

El plan after previo al nuevo indice seguia siendo sequential scan:

| Campo | Resultado |
|---|---:|
| filas del perfil | 212.228 |
| filas descartadas | 310.060 |
| buffers | 30.105 |
| execution time | 126,321 ms |

Se agrega `ix_shadow_event_profile_decision_aggregate` como indice parcial
cubriente, construido `CONCURRENTLY`. Su ganancia adicional debe medirse en
canary despues de aplicar la migracion; no se atribuye una mejora no observada.

## 11. Limite del benchmark de rafaga

No se ejecuto una rafaga durable de 100, 1.000 y 10.000 eventos contra la base
configurada porque no es la base dedicada `copy_trading_test` y el entorno no
tiene Docker disponible. Las pruebas PostgreSQL 16 de proyeccion y concurrencia
son read-only. El runbook deja el comando para ejecutar la rafaga de escritura
en QA antes del canary; este limite impide declarar evidencia de throughput
end-to-end productivo desde esta maquina.

## 12. Optimizaciones aplicadas y bordes medidos

- cinco conteos de decision fueron reemplazados por un aggregate;
- V004 agrega un indice parcial cubriente construido `CONCURRENTLY`;
- perfiles no aplicables se filtran antes del advisory lock;
- SKIPPED/NOOP sin cambio de posicion no recalcula validation;
- `queueDelay`, processing total y persistencia DB tienen timers separados;
- `lastAcceptedEventAt` impide que RESIZE/CLOSE/FLIP atrasados muten estado;
- la DLQ reclama un solo evento por `positionKey` en orden de
  `first_failed_at`, sin serializar posiciones independientes.

La mejora SQL mediana de 80,8% corresponde exclusivamente a los contadores de
decision. No se extrapola como throughput end-to-end hasta ejecutar el harness
de escritura QA de tres corridas.
