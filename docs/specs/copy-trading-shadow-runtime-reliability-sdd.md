# SDD - Reliability del runtime SHADOW, MICRO_LIVE y LIVE

Fecha de investigacion: 2026-07-11

Commit investigado de `ms-signals-orc`:
`27bc2057d456495ee5945666714a4b81533e73a2`

Estado SDD al crear esta especificacion: `INVESTIGATION_ONLY`.

Estado de implementacion local: correcciones aplicadas; la clasificacion final
depende de la verificacion descrita en la seccion 18.

## 1. Objetivo

Corregir y verificar seis problemas del flujo productivo:

1. incompatibilidad temporal de la consulta rolling SHADOW;
2. transaccion excesivamente grande de `syncDistribution`;
3. diagnostico incorrecto del duplicado distribuido sano;
4. atraso y persistencia costosa de SHADOW;
5. contrato ambiguo cuando varias estrategias coinciden;
6. clasificacion insuficiente de `RESIZE_AFTER_SHADOW_CLOSED`.

La correccion debe conservar los gates, la idempotencia, el accounting y la
independencia del hot path LIVE.

## 2. Alcance

El alcance principal es `ms-signals-orc`.

`ms-binance-engine` solo se modifica si una prueba demuestra que el contrato
de la orden o su respuesta causa uno de los problemas. La investigacion no
encontro esa dependencia para los seis problemas actuales.

`ms-sentinel-hyperliquid` se inspecciona para entender la clasificacion del
delta de origen. No se cambia desde esta especificacion sin abrir una SDD
propia o ampliar explicitamente el alcance.

Fuera de alcance:

- modificar el sizing LIVE;
- relajar copy guard, readiness o promociones;
- deduplicar globalmente por wallet;
- cambiar la alternativa de solapamiento sin decision de negocio;
- ejecutar ordenes reales;
- cambiar variables productivas durante tests;
- registrar secretos o payloads privados.

## 3. Invariantes productivos

### 3.1 Unidad de estrategia

La identidad funcional es:

```text
wallet_id + strategy_code + scope_type + scope_value
```

Una allocation real agrega `user_id + user_copy_allocation_id`.

Las metricas, estados SHADOW, activity, exposure, coverage, guard, promotion,
dispatch intent y accounting no se mezclan entre unidades de estrategia.

### 3.2 MICRO_LIVE

- maximo `100 USDC` de margen por `user + wallet`;
- presupuesto compartido entre estrategias de la wallet;
- maximo `20 USDC` de margen por nueva operacion;
- maximo cinco posiciones abiertas o reservas pendientes;
- OPEN e INCREASE consumen presupuesto;
- REDUCE y CLOSE omiten el budget gate y siguen permitidos;
- reservas ambiguas siguen consumiendo capacidad hasta reconciliacion;
- el lock de presupuesto es exclusivo de MICRO_LIVE;
- el calculo usa margen, no notional apalancado.

### 3.3 LIVE

- conserva sizing y gates actuales;
- no hereda limites `100/20/5`;
- no adquiere el advisory lock MICRO_LIVE;
- no espera procesamiento SHADOW;
- un resultado ambiguo se reconcilia y no se reenvia a ciegas.

### 3.4 SHADOW

- no emite ordenes Binance;
- conserva orden por `positionKey` dentro de una unidad;
- persiste un solo evento por identidad SHADOW;
- no mezcla posiciones entre perfiles;
- no sacrifica accounting por throughput;
- un fallo SHADOW no bloquea LIVE;
- ningun evento aceptado como durable puede perderse silenciosamente.

## 4. Flujo de referencia

```text
Hyperliquid trade
  -> ms-sentinel-hyperliquid detecta OPEN/RESIZE/CLOSE/FLIP
  -> POST direct delta
  -> mapper de ms-signals-orc
  -> dedupe local y distribuido
  -> direct-ingest lane(positionKey)
  -> bind de lifecycle origin
  -> enqueue SHADOW
  -> dispatch LIVE/MICRO_LIVE por usuario
     -> contexts por allocation aplicable
     -> dispatch intent durable
     -> budget MICRO_LIVE cuando corresponde
     -> ms-binance-engine
     -> persistencia/reconciliacion

SHADOW worker
  -> dequeue
  -> perfiles aplicables
  -> profile lock deterministico
  -> dedupe de evento
  -> estado de posicion y accounting
  -> operacion y ledger
  -> activity y validation
  -> commit

Promotion worker
  -> candidatos SHADOW
  -> rolling coverage batch
  -> gates restantes
  -> promotion transaction por profileKey
```

## 5. Evidencia de investigacion

### 5.1 Temporal rolling

PostgreSQL 16 declara `event_time timestamptz NOT NULL`. La query nativa usa:

```sql
min(event_time) AS "oldestEventTime",
max(event_time) AS "newestEventTime"
```

Hibernate 6 materializa ambos agregados como `java.time.Instant`. La
proyeccion actual exige `OffsetDateTime`, por lo que Spring Data lanza:

```text
Cannot project java.time.Instant to java.time.OffsetDateTime
```

### 5.2 Distribucion

`syncDistribution` abre una transaccion antes de cargar usuarios y la mantiene
durante todos los usuarios, perfiles, allocations, flush y relink. Un catalogo
de simbolos vacio puede provocar HTTP dentro de esa transaccion.

El retry `40P01` envuelve el job completo. El fallo de una unidad revierte las
unidades ya procesadas. Promotion no usa la misma transaccion ni el mismo lock.

### 5.3 Dedupe

El claim distribuido funciona para PROCESSING, PROCESSED, lease vencido,
FAILED y REJECTED. El reason code sano no existe en `CopyLogAdvice` y cae en
el default REVIEW con alerta.

### 5.4 Costo SHADOW

En PostgreSQL real existian aproximadamente 510.000 eventos. Una sola consulta
`count(decision='SIMULATED')` para un perfil grande hizo sequential scan de la
tabla y demoro aproximadamente 140 ms. El runtime ejecuta cinco conteos
separados por cada estrategia. Una consulta con cinco `FILTER` demoro cerca de
142 ms en la misma sesion read-only.

El refresh de validation tambien ejecuta cuatro agregaciones de posiciones,
una lectura de validation y un save. Los perfiles se bloquean antes de filtrar
si aplican al evento.

La implementacion corregida usa una sola proyeccion aggregate para los cinco
contadores, no refresca validation cuando el evento no muta posicion y decide
el routing antes de adquirir locks. Un evento LONG, por ejemplo, no toma el
lock de `SHORT_ONLY`.

### 5.5 Solapamiento

El resolver deduplica usuarios. El motor vuelve a expandir allocations y
genera una orden por allocation aplicable. La idempotency key incluye
allocation, estrategia, scope, source event e intent.

El budget MICRO_LIVE es comun a `user + wallet + mode`. Si solo queda un cupo,
una sola estrategia reserva, pero el orden ganador no esta especificado.

### 5.6 Lifecycle posterior al cierre

El codigo actual busca cualquier ultimo CLOSED de `profile + symbol + side`.
No compara `incomingEventAt`, `closedAt`, ultimo evento aceptado ni source
version. Un RESIZE posterior siempre se clasifica igual.

Se observo la secuencia:

```text
REDUCE 2.40 -> 1.84
CLOSE  1.84 -> 0
RESIZE 0 -> 0.82, 67 ms despues
FLIP posterior
```

El RESIZE y el FLIP fueron omitidos, y el perfil permanecio cerrado mientras
siguieron llegando ajustes.

## 6. Contrato temporal rolling

### 6.1 Tipos

- PostgreSQL/JDBC/Hibernate boundary: `Instant` para agregados nativos.
- Dominio y contratos existentes: `OffsetDateTime` en UTC.
- Conversion permitida:

```java
OffsetDateTime.ofInstant(value, ZoneOffset.UTC)
```

- No se registra converter global.
- No se usa zona horaria del host.
- `event_time` no permite null. Una allocation sin filas no produce proyeccion.

### 6.2 Resultado

La query devuelve cero o una fila por allocation. El service mapea cada
`Instant` a UTC y crea `ShadowCoverageCounts`.

En `AUDIT`, un fallo rolling deja la decision efectiva historica y marca el
audit rolling como fallido. En `ROLLING`, un fallo bloquea promotion. En
`LEGACY`, no se ejecuta la query.

### 6.3 Observabilidad

Se implementan los eventos:

- `shadow.coverage.query.started`;
- `shadow.coverage.query.succeeded`;
- `shadow.coverage.query.failed`;
- `shadow.coverage.fallback.used` cuando la fuente efectiva es historica por
  degradacion permitida.

El fallo identifica projection, alias, tipos source/target, ventana, cantidad,
fallback e impacto. No se infiere el tipo source desde el texto de excepcion:
se registra el contrato conocido de la proyeccion.

Metricas:

- `shadow_coverage_query_total{result,source}`;
- `shadow_coverage_query_duration`;
- `shadow_coverage_fallback_total{reason}`.

## 7. Contrato transaccional de distribution

### 7.1 Preparacion fuera de transaccion

El coordinador no tiene `@Transactional`. Fuera de transaccion debe:

1. cargar candidatos y usuarios;
2. resolver catalogo/simbolos;
3. calcular elegibilidad, target mode y porcentajes;
4. construir unidades inmutables;
5. identificar altas, updates, pausas y cierres.

No se permite HTTP dentro del executor transaccional.

### 7.2 Unidad consistente

La unidad es:

```text
userId + walletId + profileKey
```

Cada unidad usa una transaccion independiente por medio de otro bean Spring o
`TransactionOperations`. No se usa self-invocation para crear boundaries.

### 7.3 Orden de locks y writes

Orden canonico:

```text
1. construir profileKey canonica sin sanitizar
2. advisory lock shadow-profile:{profileKey}
3. releer la unidad dentro de la transaccion
4. aplicar writes propios de esa unidad
5. flush y commit
```

Todo flujo que muta el mismo profileKey, incluido runtime SHADOW y promotion,
usa el mismo namespace. `syncDistribution` y promotion procesan una sola clave
por transaccion. SHADOW puede tomar varias claves aplicables al mismo evento,
pero siempre las ordena antes de adquirirlas. Ningun flujo cooperante toma
primero una fila para despues pedir el profile lock.

La clave usada por PostgreSQL conserva caracteres funcionales como `<=5`.
Solo la copia destinada al log se sanitiza; usar `<_5` para el lock crearia un
namespace distinto y esta prohibido.

### 7.4 Retry y recuperacion

- solo SQLState transitorio explicitamente permitido, inicialmente `40P01`;
- maximo tres intentos;
- backoff con jitter acotado;
- retry por unidad, no por job;
- errores de negocio no se reintentan;
- el agotamiento persiste una unidad recuperable o DLQ durable;
- las demas unidades continuan;
- un replay conserva la misma identidad y es idempotente.

Eventos:

- `copy.distribution.unit.started`;
- `copy.distribution.unit.completed`;
- `copy.distribution.unit.failed`;
- `postgres.deadlock.retry`;
- `postgres.deadlock.exhausted`.

Metricas:

- `copy_distribution_unit_total{result}`;
- `copy_distribution_transaction_duration`;
- `copy_deadlock_total{operation}`;
- `copy_deadlock_retry_total{result}`.

## 8. Contrato de duplicado distribuido

### 8.1 Duplicado esperado

```text
event=hyperliquid.direct_ingest.distributed_duplicate
reasonCode=DISTRIBUTED_DUPLICATE_SUPPRESSED
reasonAlias=duplicate_claimed_by_other_instance
diagnosticArea=idempotency
diagnosticSeverity=INFO
decision=NOOP
expected=true
shouldAlert=false
copyImpact=NO_DUPLICATE_ORDER
```

### 8.2 Casos no equivalentes

- misma key y mismo fingerprint: duplicado esperado;
- misma key y fingerprint distinto: `IDEMPOTENCY_KEY_PAYLOAD_CONFLICT`, BLOCK,
  alerta;
- storage error fail-closed: rechazado y retryable segun error;
- storage error fail-open: permitido con `duplicateRisk=true` y alerta;
- lease vencido: reacquire esperado, con reason propio;
- dispatch duplicado posterior a Binance: lo resuelve dispatch intent, no este
  catalogo.

Una fila creada antes de V003 puede tener `payload_fingerprint=NULL`. Ese caso
no se declara duplicado sano ni puede reacquirir un lease: se responde NOOP
fail-closed con `DISTRIBUTED_DUPLICATE_PAYLOAD_UNVERIFIED`, `shouldAlert=true`
y ninguna orden. La remediacion es revisar o backfillear la fila legacy; nunca
aprender un fingerprint entrante como si probara el payload historico.

Se agrega fingerprint estable o comparacion de los campos canonicos antes de
clasificar como duplicado sano.

Metrica:

`distributed_duplicate_total{result}` con resultados de baja cardinalidad.

## 9. Contrato de rendimiento SHADOW

### 9.1 Medicion correcta

- `queueDelayMs`: enqueue monotonic timestamp hasta dequeue, calculado antes
  del procesamiento;
- `lockWaitMs`: tiempo esperando advisory lock;
- `dbPersistMs`: SQL de mutacion y flush, sin queue ni accounting;
- `commitMs`: retorno del boundary transaccional si puede medirse separado;
- `totalElapsedMs`: dequeue hasta commit;
- `sourceToPersistMs`: event timestamp hasta commit, separado de queue delay.

Stages minimos:

```text
DEQUEUE
PROFILE_LOOKUP
PROFILE_FILTER
LOCK_WAIT
DEDUPE
POSITION_LOAD
CLASSIFICATION
ACCOUNTING
OPERATION_PERSIST
EVENT_PERSIST
ACTIVITY_PERSIST
VALIDATION_REFRESH
FLUSH_COMMIT
```

### 9.2 Reduccion de queries

Primera implementacion permitida:

- reemplazar los cinco conteos por una sola query aggregate;
- reemplazar, si el plan lo justifica, las cuatro agregaciones de posiciones
  por una proyeccion aggregate;
- agregar indice solo despues de `EXPLAIN ANALYZE` antes/despues;
- filtrar estrategias antes de pedir locks;
- evitar reads duplicados de profile/allocation dentro de la misma unidad.
- no recalcular validation para SKIPPED/NOOP sin mutacion de posicion; el
  ledger durable sigue contando el evento y el siguiente cambio valido o sync
  actualiza la materializacion.

Counters incrementales quedan fuera de la primera correccion salvo que tests
concurrentes prueben ausencia de lost updates y exista reconciliacion desde el
ledger como fuente de verdad.

### 9.3 Orden y aislamiento

- FIFO por `positionKey` y profile;
- perfiles distintos pueden avanzar en paralelo;
- eventos de lados opuestos de un FLIP requieren una barrera comun por
  `wallet + symbol + profileKey`, no solo dos lanes por side;
- una wallet caliente no debe bloquear wallets independientes;
- no se aumenta threads sin medir lock contention y pool.

### 9.4 Durabilidad

La cola en memoria por si sola no satisface cero perdida. La especificacion de
implementacion debe cubrir:

- queue full;
- shutdown/restart;
- worker failure;
- retry agotado;
- replay ordenado;
- dedupe durante replay.

El `shadow_event_dead_letter` existente puede guardar el payload, pero no es
suficiente sin producer para queue-full y consumer de replay. El replay nunca
autoriza dispatch Binance.

El producer durable cubre queue-full, interrupcion y fallo de worker. El claim
de replay conserva `first_failed_at` y solo permite un evento `REPLAYING` por
`positionKey`; eventos posteriores de esa posicion esperan, mientras posiciones
distintas avanzan en paralelo. Un lease abandonado vuelve a ser reclamable.

Eventos/metricas:

- `shadow.processing.slow`;
- `shadow_processing_duration{stage,result}`;
- `shadow_queue_delay`;
- `shadow_db_persist_duration`.

## 10. Contrato de estrategias solapadas

Se conserva la alternativa A:

```text
una copia por allocation/estrategia aplicable
```

Por lo tanto, MOVEMENT_ALL y SHORT_ONLY pueden enviar una orden cada una para
el mismo source event. Cada una tiene idempotency key y clientOrderId propios.

El budget MICRO_LIVE sigue siendo conjunto. Si no hay capacidad para la
segunda, esta queda rechazada con su reason de presupuesto durable.

Alternativas no implementadas:

- B: una copia por `user + wallet + sourceEvent`;
- C: una estrategia ganadora por prioridad.

El orden actual no es contrato. Mientras negocio no elija C, no se promete que
una estrategia especifica gane el ultimo cupo.

Las metricas y guard del runtime deben resolverse por allocation key completa,
no solo por `strategyCode`. Se agrega RED para dos SYMBOL_SPECIALIST con scope
distinto antes de cambiar este lookup.

## 11. Contrato lifecycle SHADOW

### 11.1 Estados

```text
NONE -> OPEN
OPEN -> INCREASE
OPEN -> REDUCE
OPEN -> CLOSE
CLOSED -> explicit OPEN = REOPEN
OPEN side A -> FLIP -> CLOSED side A + OPEN side B
```

### 11.2 RESIZE con estado CLOSED

Clasificacion requerida:

| Condicion | Decision | expected | alert |
|---|---|---:|---:|
| idempotency key ya vista | DUPLICATE_NOOP | true | false |
| incomingAt <= lastAcceptedEventAt | STALE_AFTER_CLOSE_NOOP | true | false |
| explicit OPEN posterior | REOPEN | true | false |
| explicit FLIP con contexto consistente | APPLY_FLIP | true | false |
| RESIZE positivo posterior sin OPEN | LIFECYCLE_GAP_RECOVERABLE | false | por umbral |
| profile sin historia durante warmup | WARMUP_NO_VALID_POSITION | true | false |

No se convierte automaticamente todo RESIZE positivo en OPEN. El caso
`LIFECYCLE_GAP_RECOVERABLE` conserva el evento, registra contexto y solicita
reconciliacion con estado autoritativo o correccion del productor.

El log `shadow.resize.after_closed` incluye estado previo, ultimo evento,
closedAt, incomingAt, ordering decision, origin/source IDs y accion.

Metrica:

`resize_after_shadow_closed_total{decision}`.

Cada posicion conserva `lastAcceptedEventAt`. Para una posicion OPEN, un
RESIZE, CLOSE o FLIP estrictamente anterior queda en NOOP con
`STALE_RESIZE_NOOP`, `STALE_CLOSE_NOOP` o `STALE_FLIP_NOOP`. Timestamps iguales
se aceptan en orden FIFO porque varias fuentes pueden tener precision limitada.
La metrica `shadow_stale_event_total{event_type}` no usa IDs como tags.

### 11.3 Estrategias independientes

Una estrategia puede estar CLOSED y otra OPEN. La decision de una no se copia
a la otra. Cada log incluye profileKey y strategyCode para explicar la
diferencia.

## 12. Logs de MICRO_LIVE y LIVE

Los logs existentes de dispatch no se eliminan. Se completan sin tags de alta
cardinalidad en metricas.

OPEN/INCREASE MICRO_LIVE registra:

- limites 100/20/5;
- used, pending, requested, remaining y projected margin;
- open y pending positions;
- lock wait;
- allocation, strategy, source event, idempotency y clientOrderId;
- decision y reason durable.

REDUCE/CLOSE registra:

```text
budgetCheck=SKIPPED_FOR_REDUCE_OR_CLOSE
decision=ALLOW
reasonCode=MICRO_LIVE_EXIT_ALWAYS_ALLOWED
```

LIVE registra:

```text
budgetMode=LIVE_UNRESTRICTED_BY_MICRO_LIMITS
microLiveBudgetLockAcquired=false
```

Los reason codes de budget se normalizan sin perder el error original. Un
retry de un intent rechazado conserva `lastErrorCode`; no se degrada a un
generico si el reason durable existe.

## 13. Test plan RED

### 13.1 Coverage

- mapper/projection recibe Instant;
- repository PostgreSQL 16 con fracciones y fronteras UTC;
- allocation sin filas;
- query no activa fallback por incompatibilidad;
- rolling calculator usa counts correctos;
- logs y metricas success/failure/fallback.

### 13.2 Distribution

- misma unidad en dos procesos;
- unidades distintas en paralelo;
- Shadow vs distribution;
- promotion vs distribution;
- 40P01 recuperado y agotado;
- rollback de una unidad no revierte otra;
- catalog lookup no ejecuta HTTP dentro de transaccion;
- una unidad lenta no bloquea otra wallet.

### 13.3 Dedupe

- replay normal entre replicas;
- same key/same fingerprint;
- same key/different fingerprint;
- lease vencido;
- storage fail-open y fail-closed;
- fila legacy sin fingerprint queda fail-closed y no se etiqueta healthy;
- severity, expected, decision y shouldAlert.

### 13.4 SHADOW

- OPEN, INCREASE, REDUCE, CLOSE y NOOP;
- 100, 1.000 y 10.000 eventos;
- wallet caliente con multiples estrategias;
- wallets independientes;
- conteo de queries;
- queue, lock, persist y commit separados;
- queue full, restart y replay sin perdida;
- replay single-flight y ordenado por posicion;
- perfil no aplicable no adquiere advisory lock;
- evento atrasado no modifica una posicion mas nueva;
- tres corridas y mediana.

### 13.5 Solapamiento

- MOVEMENT_ALL + SHORT_ONLY generan dos intents;
- keys/clientOrderIds diferentes;
- mismo budget 100;
- solo una reserva cuando quedan 20;
- retry no duplica ninguna;
- dos scopes de una estrategia usan su propia metrica y guard.

### 13.6 Lifecycle

- close legitimo seguido de resize stale;
- resize duplicado;
- explicit reopen;
- gap RESIZE sin OPEN;
- FLIP LONG a SHORT y SHORT a LONG;
- reduce parcial y close final;
- lados opuestos concurrentes;
- estrategias con estados diferentes.

## 14. Baseline y performance gates

El baseline se guarda en:

`docs/performance/shadow-runtime-baseline.md`

Debe registrar commit, maquina, Java, PostgreSQL, datos, p50/p95/p99,
throughput, queries, queue, lock, persist, deadlocks, errores y fallback.

Acceptance de performance:

- no mas queries de validation por evento que el nuevo contrato;
- ninguna regresion mayor a 5% en hot path LIVE p95;
- ninguna regresion mayor a 10% en hot path LIVE p99;
- mejora SHADOW demostrada por mediana de tres corridas;
- cero eventos perdidos en el harness durable;
- cero deadlocks no recuperados en escenarios esperados;
- pool estable y sin espera LIVE causada por SHADOW.

## 15. Rollout

1. desplegar logs/metricas y fix temporal con coverage en AUDIT;
2. validar consulta rolling sin fallback inesperado;
3. desplegar transaction units con worker habilitado en canary;
4. observar deadlocks, retries y unidades recuperables;
5. desplegar aggregate SHADOW e indice validado;
6. comparar queue delay y db persist;
7. habilitar replay SHADOW con lote pequeno;
8. validar MICRO_LIVE 100/20/5 y exits;
9. ampliar canary;
10. evaluar activacion ROLLING separadamente.

## 16. Rollback

- coverage: volver a `AUDIT` o `LEGACY`, nunca promover manualmente;
- distribution: desactivar nuevo coordinator y volver al job anterior solo si
  no existen unidades a mitad de replay;
- SHADOW aggregate: volver al recompute anterior manteniendo ledger e indice;
- replay: detener claims, conservar DLQ;
- logs/metricas son compatibles y no requieren rollback;
- no borrar intents, events, positions ni recovery records.

## 17. Criterios de aceptacion

- cero incompatibilidades Instant/OffsetDateTime;
- rolling PostgreSQL 16 verde y fallback normal cero;
- transaccion por unidad acotada y sin HTTP;
- lock order comun entre distribution, SHADOW y promotion;
- retries observables y agotamiento durable;
- duplicado sano INFO, expected y sin alerta;
- colision de key bloqueada;
- queue/persist/lock medidos correctamente;
- validation sin cinco scans separados;
- contrato A de solapamiento preservado;
- MICRO_LIVE 100/20/5 demostrado;
- exits MICRO_LIVE y LIVE sin budget lock;
- lifecycle posterior al cierre clasificado por contexto;
- suite, PostgreSQL, concurrencia, benchmark y package verdes;
- `git diff --check` limpio;
- migraciones presentes en el jar;
- runbook productivo actualizado.

No se puede declarar `PRODUCTION_CANARY_READY` si cualquiera de estos puntos
queda sin evidencia.

## 18. Implementacion aplicada

Migraciones aditivas:

- V003: fingerprint de payload para dedupe distribuido;
- V004: indice concurrente para aggregate de decisiones SHADOW;
- V005: `last_accepted_event_at` y backfill UTC por posicion;
- V006: indice concurrente para replay ordenado por `positionKey`.

Decisiones y llamadas externas se calculan antes de las transacciones unitarias.
Distribution, sync SHADOW y promotion comparten `CopyDistributionUnitExecutor`,
transaccion `REQUIRES_NEW`, timeout acotado, advisory lock canonico y retry solo
para SQLState `40P01`.

La evidencia local no incluye una rafaga durable de 100/1.000/10.000 writes en
una base QA dedicada. Por esa razon, incluso con suite, PostgreSQL 16 read-only,
package y migraciones verdes, el dictamen maximo local es `QA_READY`.

## 19. Evidencia de verificacion final

Fecha: 2026-07-11. Java: Temurin 21.0.11.

- suite completa `ms-signals-orc`: 490 tests, 0 failures, 0 errors, 2 skipped;
- los skips corresponden al benchmark concurrente opt-in y al test PostgreSQL
  que exige URL externa cuando la suite corre aislada;
- PostgreSQL 16.10 real, read-only: 9 tests, 0 failures, 0 errors, 0 skipped;
- coverage nativo proyecto `Instant`, conserva fracciones UTC y fronteras;
- concurrencia prueba misma clave, claves distintas, rollback y `40P01`;
- PostgreSQL acepta el plan del claim DLQ single-flight mediante `EXPLAIN`;
- benchmark SQL, tres corridas: mediana 646,761 ms antes y 124,008 ms despues,
  mejora 5,22x y reduccion de 80,8% en los contadores de decision;
- `mvnw -DskipTests package`: success;
- JAR verificado con V003, V004 + conf, V005 y V006 + conf;
- `git diff --check`: exit 0;
- cero espacios finales y cero marcadores de merge en los 49 paths cambiados;
- sin coincidencias de `Cannot project java.time.Instant to
  java.time.OffsetDateTime` ni `Caso revisable no clasificado` en reportes.

Dictamen: `QA_READY`.

Bloqueador para `PRODUCTION_CANARY_READY`: ejecutar tres rafagas durables de
100, 1.000 y 10.000 eventos en `copy_trading_test`, aplicar V004/V006 en QA y
comparar throughput, p50/p95/p99, pool, deadlocks, backlog y filas perdidas.
