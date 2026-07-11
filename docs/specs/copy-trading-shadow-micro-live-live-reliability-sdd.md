# SDD - Reliability SHADOW, MICRO_LIVE y LIVE

Estado: implementado y validado localmente (`FIXED_LOCAL_ONLY`)
Fecha: 2026-07-11
Repositorios inspeccionados:

- `ms-signals-orc`: branch `feature/1.4.27`, commit `9b0a83126d5c45df3ee65ffdc0ab290786dbaaa6`.
- `ms-binance-engine`: branch `feature/1.0.16`, commit `e5daceaa1c0fecad2f19fd4ac5e9d6175943585a`.

## 1. Problema

Existen allocations `MICRO_LIVE` activas, pero la evidencia QA observada no contiene una orden aceptada. En paralelo se observaron deadlocks PostgreSQL `40P01`, estados de guard summary/full aparentemente contradictorios, progreso de readiness basado en tiempo con evidencia real insuficiente y movimientos `RESIZE`/`CLOSE` sin una apertura copiada.

El cambio debe preservar el camino LIVE: sin consultas por usuario nuevas, sin transacciones durante HTTP, sin sleeps, sin locks globales y sin reenvio automatico ante un resultado ambiguo.

## 2. Evidencia confirmada

La consulta QA se ejecuto con rol read-only, `transaction_read_only=on`, timeouts acotados y `ROLLBACK`. No se escribio en QA ni se llamo a Binance real.

### 2.1 Cero ordenes MICRO_LIVE

- Existen dos allocations activas para el mismo usuario y wallet: IDs `505` (`MOVEMENT_ALL`) y `506` (`SHORT_ONLY`), ambas `MICRO_LIVE`, vinculadas a Shadow y con promocion persistida.
- `allocation_pct=0.000001` no impidio la elegibilidad del evento investigado.
- El `OPEN` del origen `601dd5b6-838b-440f-8efb-c59e96209015` registro `copy_eligible_users=1` y `copy_submitted_tasks=1`.
- El `copy_execution_job` de ese origen termino `DEAD` despues de 12 intentos con `TRANSIENT`: `Respuesta invalida de ms-binance: orderId/campos de ejecucion null`.
- Los posteriores `resize_without_open_copy` y `close_without_open_copy` son consecuencia de no haber materializado la apertura copiada.
- `copy_dispatch_intent` tiene cero filas en QA porque el evento ocurrio por el camino legacy, antes del commit actual de intent durable.

Conclusion confirmada: para el evento trazado, la causa de cero ordenes materializadas no fue `eligibleUsers=0` ni `allocation_pct`; fue una respuesta de ejecucion invalida agotando el retry legacy.

### 2.2 Comportamiento del codigo actual

- El coordinador actual crea/recupera un intent durable antes de HTTP.
- Una respuesta sin identidad de orden queda `RECONCILING` y nunca se reenvia automaticamente.
- `ms-binance-engine` normaliza `avgPrice=null` como precio pendiente si existen `orderId`, estado y cantidad validos.
- Una respuesta sin `orderId` queda `AMBIGUOUS`, incluso si dice `FILLED`.
- Los switches `copy.new-dispatch.enabled`, `copy.micro-live.enabled` y `copy.live.enabled` son fail-closed por defecto. El diagnostico existente no expone `copy.new-dispatch.enabled`.

### 2.3 Sizing y aclaracion de contrato

- MICRO_LIVE ignora `allocation_pct`. Cada combinacion canonica `userId + walletId` dispone de un unico presupuesto rigido y compartido de `100 USDC`, aunque esa wallet tenga varias estrategias/allocations activas. El margen maximo es `20 USDC` por operacion y el maximo es 5 posiciones. Si no quedan 20 completos puede usar solo el remanente sin superar 100; al agotar margen o cantidad de posiciones se bloquean nuevas entradas.
- Aclaracion posterior del contrato: los limites `100/20/5` son exclusivos de MICRO_LIVE. LIVE conserva el sizing ponderado existente y no se limita artificialmente a 100.
- Una cuenta con menos de 100 USDC disponibles no reduce silenciosamente el presupuesto MICRO_LIVE: se rechaza con reasonCode explicito.
- LIVE no hereda los topes de MICRO_LIVE. Conserva el contrato actual `accountCapital * allocation_pct` y sizing por exposicion fuente.
- En LIVE, saldo `805` y porcentaje `0.000001` producen capital asignado `0.000805`. Ese resultado debe llegar a filtros de sizing y ser aceptado o rechazado explicitamente por `MIN_NOTIONAL`/precision; no alterarse a 100.
- El leverage se aplica una vez: margen objetivo -> notional por leverage -> quantity por precio y filtros del simbolo.

### 2.4 Locks

Orden observado:

| Flujo | Orden actual relevante |
|---|---|
| sync Shadow | `copy_wallet_profile` -> validation -> `shadow_copy_allocation` |
| evento Shadow | position/operation/event Shadow -> `shadow_copy_allocation` -> `copy_wallet_profile` |
| sync allocation usuario | sync Shadow -> `user_copy_allocation` -> link Shadow |
| promocion MICRO_LIVE | `user_copy_allocation` -> audit |
| dispatch real | `copy_dispatch_intent` -> HTTP sin transaccion -> persistencia/reconciliacion |

La inversion profile/allocation explica los ciclos `40P01`. Ademas, `syncDistribution` mantiene una transaccion para todos los usuarios, ampliando el tiempo de retencion de locks.

### 2.5 Recuperacion de migracion concurrente

En el primer despliegue de `V202607110002`, el backfill termino pero el primer
`CREATE INDEX CONCURRENTLY` excedio `lock_timeout=5s`. PostgreSQL dejo
`ix_copy_dispatch_intent_micro_wallet_budget` con `indisready=true` e
`indisvalid=false`; el segundo indice no fue creado y Flyway no registro la
migracion como exitosa.

Un segundo intento con `lock_timeout=60s` permitio confirmar el bloqueador:
Flyway mantenia una conexion `idle in transaction` con el advisory lock
transaccional y un snapshot de `flyway_schema_history`; la conexion que
ejecutaba `CREATE INDEX CONCURRENTLY` esperaba el `virtualxid` de esa misma
conexion. Por tanto, aumentar timeouts no resuelve el ciclo. PostgreSQL debe usar
el advisory lock de sesion soportado por Flyway mediante
`spring.flyway.postgresql.transactional-lock=false`.

### 2.6 Contrato de reserva de posiciones

El arranque posterior a las migraciones detecto que
`copy_dispatch_intent.reserved_position_count` es `smallint`, como define
`V202607090001`, mientras la entidad lo declaraba como `int`. El esquema es el
contrato correcto: cada intent reserva 0 o 1 posicion y MICRO_LIVE admite como
maximo 5. La entidad debe mapear el campo como `short`; no se amplia la columna
ni se crea una migracion de datos innecesaria.

Como la migracion usa `executeInTransaction=false`, cada sentencia anterior al
fallo puede quedar confirmada. El reintento debe eliminar de forma concurrente
los dos indices nuevos por nombre y recrearlos; no puede usar `IF NOT EXISTS`,
porque PostgreSQL aceptaria un indice existente pero invalido. La validacion
posterior debe reportar tanto indices invalidos como indices ausentes.

## 3. Flujo actual

```text
Hyperliquid delta
 -> normalizacion e idempotencia de origen
 -> matching de estrategia
 -> snapshot cacheado de allocations
 -> guard cacheado
 -> usuarios candidatos
 -> lifecycle guard
 -> executor de copia
 -> sizing y filtros Binance
 -> gate MICRO_LIVE/LIVE
 -> claim de copy_dispatch_intent
 -> HTTP a ms-binance-engine (sin transaccion abierta)
 -> normalizacion ACK/AMBIGUOUS/REJECTED
 -> persistencia de copy_operation/copy_operation_event
 -> reconciliacion por orderId/clientOrderId
```

SHADOW se encola en lanes independientes y no forma parte del despacho LIVE. El lookup latest-position se ejecuta en el almacenamiento del estado de origen, una vez por evento, no una vez por allocation.

## 4. Flujo esperado

1. Resolver allocation y guard desde snapshots cacheados.
2. Emitir un resumen estructurado de cada exclusion, no solo `eligibleUsers=0`.
3. Dimensionar MICRO_LIVE desde exactamente 100 USDC por `userId + walletId`, compartidos entre estrategias, maximo 20 USDC de margen por operacion y capacidad total acotada.
4. Mantener LIVE con sizing proporcional actual y rechazos explicitos si el resultado queda bajo minimos Binance.
5. Crear el intent durable antes de llamar al adaptador Binance.
6. Tratar timeout/respuesta incompleta como ambigua y reconciliar, nunca reenviar a ciegas.
7. Mantener `RESIZE`/`CLOSE` huerfanos como no-op seguro con causa consultable.
8. Serializar mutaciones Shadow solo por `profileKey`, con claves ordenadas.
9. Reintentar exclusivamente `40P01`, con rollback completo, backoff, jitter y limite.
10. Persistir el evento Shadow agotado como recuperable.
11. Promover MICRO_LIVE a LIVE solo con evidencia desglosada de envio, ACK, fill, cierres y reconciliacion.

## 5. Invariantes

- `targetCapitalUSDC` de cada `userId + walletId` MICRO_LIVE es exactamente `100`.
- `maxMarginPerOperationUSDC` de MICRO_LIVE es `20` y la suma abierta/reservada de todas las estrategias de esa misma `userId + walletId` nunca supera 100.
- El advisory lock y el snapshot de presupuesto MICRO_LIVE usan la misma clave canonica `userId + normalizedWalletId + MICRO_LIVE`; `allocationId` conserva identidad/idempotencia de estrategia, pero no crea un presupuesto independiente.
- El pre-sizing cacheado de MICRO_LIVE suma margen y posiciones activas de toda la `userId + walletId`; no filtra por estrategia. El snapshot PostgreSQL repite el mismo alcance como barrera atomica antes del dispatch.
- La aplicacion falla al iniciar si los limites efectivos MICRO_LIVE no son exactamente `100/20/5`; una variable de entorno no puede cambiar silenciosamente este contrato.
- MICRO_LIVE bloquea nuevas entradas al alcanzar margen total o cantidad maxima; reductions/closes siguen permitidos.
- `allocation_pct` no dimensiona MICRO_LIVE, pero conserva su semantica actual en LIVE.
- LIVE no queda limitado a 100/20 por este cambio.
- Una misma idempotency key no puede producir dos ordenes.
- Un timeout ambiguo no puede provocar un segundo envio automatico.
- Ninguna reduccion o cierre puede abrir una posicion accidentalmente.
- Una allocation sin permiso de nuevas entradas puede reducir/cerrar una posicion existente.
- Una decision full vigente gana sobre summary no final, aunque summary sea mas nuevo.
- El guard runtime informa fuente, version, `computedAt`, `expiresAt`, `decisionFinal` y `materializationStatus`.
- Un deadlock no pierde silenciosamente el evento: se reintenta o queda `RECOVERABLE`.
- Cada retry es idempotente y acotado.
- Readiness no puede aprobar LIVE con cero ordenes enviadas, reconocidas o llenadas.
- Mutaciones Shadow no serializan globalmente LIVE.
- Wallets con profile keys distintas pueden progresar en paralelo.
- `V202607110002` es reintentable despues de una ejecucion parcial: nunca puede
  terminar exitosamente conservando un indice `indisvalid=false` o sin alguno
  de los dos indices esperados.
- Los locks DDL de la migracion tienen espera acotada a 60 segundos y la
  migracion se ejecuta fuera de maxima carga; un timeout conserva el servicio
  anterior y exige diagnostico/reintento controlado.
- Flyway conserva exclusion mutua entre replicas con advisory lock de sesion;
  no mantiene un advisory lock transaccional que pueda bloquear su propio
  `CREATE INDEX CONCURRENTLY`.
- `CopyDispatchIntentEntity.reservedPositionCount` usa `short` y coincide con
  `reserved_position_count smallint`; Hibernate `ddl-auto=validate` debe poder
  validar este contrato al arrancar.

## 6. Estados y transiciones

### Dispatch intent

```text
CREATED -> DISPATCHING -> ACKNOWLEDGED/NEW/PARTIALLY_FILLED/FILLED
                       -> REJECTED
                       -> RECONCILING -> ACKNOWLEDGED/FILLED/REJECTED/MANUAL_REVIEW
ACKNOWLEDGED/FILLED -> PERSISTENCE_PENDING -> PERSISTED
```

`RECONCILING`, `PERSISTENCE_PENDING` y estados ambiguos nunca autorizan un nuevo send con la misma idempotency key.

### Allocation

```text
SHADOW -> MICRO_LIVE -> LIVE
                 \-> permanece MICRO_LIVE si falta cualquier evidencia
ACTIVE -> EXIT_ONLY/PAUSED (sin nuevas entradas; reductions/closes permitidos)
ACTIVE -> CLOSED
```

No existe promocion directa a LIVE cuando la politica exige MICRO_LIVE.

### Guard

Precedencia, de mayor a menor:

1. riesgo real vigente (`DISABLED`, `MANUAL_PAUSE`, riesgo critico);
2. full decision final materializada y no expirada asociada a la promocion;
3. snapshot summary vigente;
4. fail-closed por ausencia/staleness segun configuracion.

Un summary `SHADOW_ONLY` no degrada silenciosamente una allocation promovida mediante full, pero un riesgo real posterior si puede bloquear nuevas entradas.

## 7. Estrategia de concurrencia

- Advisory transaction lock por `shadow-profile:<profileKey>`.
- Todas las claves necesarias se deduplican y ordenan antes del primer write.
- El lock se usa solo en mutaciones Shadow/distribucion, nunca en el hot path LIVE.
- `syncDistribution` conserva por ahora su transaccion de job existente; el orden canonico de advisory locks y el retry de `40P01` reducen el riesgo, pero dividirla por usuario queda como mejora posterior que requiere una refactorizacion transaccional aislada.
- El dispatch mantiene sus locks/claims locales y `SKIP LOCKED`; no comparte el advisory lock Shadow.
- No se usa `synchronized` global.

## 8. Estrategia de retries

- Detectar SQLState exacto `40P01` recorriendo la cadena de causas SQL.
- Maximo configurable, por defecto 3 intentos.
- Backoff exponencial acotado con jitter.
- Cada intento Shadow invoca nuevamente el proxy transaccional; cada retry de sync abre una transaccion nueva para el job completo y revierte integralmente el intento anterior.
- No reintentar errores de negocio, constraints definitivos ni timeouts Binance.
- Tras agotar Shadow, persistir payload y metadatos en DLQ `RECOVERABLE` con upsert idempotente.
- Tras agotar sync, emitir estado recuperable para el siguiente ciclo programado.

## 9. Presupuesto de performance

- Regresion maxima inicial: p95 <= 5%, p99 <= 10% frente al baseline.
- Diferencia absoluta se reporta cuando la etapa local esta por debajo de 1 ms.
- Cero queries nuevas por allocation en eligibility/sizing.
- Readiness usa una agregacion batch fuera del hot path.
- Cero transacciones durante HTTP.
- Cero sleeps en LIVE.
- El lock Shadow no se adquiere en dispatch.
- No aumentar Hikari sin presupuesto global.

## 10. Plan de pruebas

### Unitarias

- MICRO_LIVE usa 100 USDC y maximo 20 por operacion aun con `allocation_pct=0.000001`.
- LIVE conserva sizing ponderado con `allocation_pct=0.000001` y produce rechazo explicito si queda bajo minimos.
- Capital menor a 100 rechaza con reasonCode explicito.
- Leverage se aplica una vez.
- Guard full promovido gana sobre summary no final; riesgo real bloquea.
- Resumen de elegibilidad cuenta cada motivo.
- Readiness con cero enviados/ACK/fills/cierres bloquea.
- Reconciliacion ambigua no reenvia.
- Normalizador Binance acepta fill con `avgPrice=null` y rechaza identidad ausente como ambigua.

### PostgreSQL 16 Testcontainers

- Reproducir la inversion profile -> allocation / allocation -> profile.
- Mismo profileKey se serializa sin perder efectos.
- Profile keys distintas progresan independientemente.
- Dos replicas logicas reclaman una sola idempotency key.
- Dispatch/reconciliation concurrentes no duplican.
- Retry `40P01` exitoso y agotado.
- DLQ hace upsert idempotente.
- Fallo parcial de `V202607110002`: indice invalido previo se elimina y ambos
  indices terminan `indisready=true` e `indisvalid=true`.
- La validacion post-migracion falla si cualquiera de los indices esta ausente.

### Performance

- 1, 10, 50, 100, 300 y 1000 allocations/tareas locales.
- JDBC de intent, budget e idempotencia.
- latest-position preparado y frio.
- Comparacion before/after con mismo hardware/JVM.

## 11. Rollback

1. Desactivar promociones con `COPY_LIVE_PROMOTION_ENABLED=false`.
2. Mantener `COPY_LIVE_ENABLED=false` y `COPY_LIVE_DRY_RUN=true`.
3. Mantener `COPY_NEW_DISPATCH_ENABLED=false` si el canary de intent falla.
4. Desactivar retry Shadow por configuracion (`max-attempts=1`) sin retirar idempotencia.
5. La tabla DLQ es aditiva y backward-compatible; no necesita borrarse al volver codigo.
6. Revertir clases/configuracion sin editar migraciones aplicadas.

## 12. Riesgos

- Un advisory lock mal normalizado podria no representar la misma unidad en todos los flujos.
- Una futura division transaccional por usuario aumentaria commits del job, aunque reduciria contencion y blast radius; requiere una decision explicita de atomicidad.
- La transaccion actual de `syncDistribution` todavia puede retener locks de varias wallets hasta completar el job; es un riesgo residual documentado, no una afirmacion de eliminacion total.
- La evidencia de slippage depende de `reference_price` y `average_price`; muestras pendientes no deben contarse como cero.
- Un evento historico legacy no crea intent durable retroactivamente.
- Activar switches sin un canary controlado puede transformar una correccion local en orden real.

## 13. Cambios descartados

- Cambiar `allocation_pct` a `1.0`: mezcla distribucion con capital objetivo y no corrige la causa demostrada.
- Aplicar los topes MICRO_LIVE de 100/20 al modo LIVE.
- Retry ciego de HTTP/orden Binance.
- Lock global Java o advisory lock global.
- Consultar full decision dentro del loop LIVE.
- Crear current-state/latest-position adicional sin beneficio medido.
- Aumentar Hikari de forma aislada.

## 14. NO CONFIRMADO

### Switches efectivos del despliegue

- Hipotesis: `COPY_NEW_DISPATCH_ENABLED` o `COPY_MICRO_LIVE_ENABLED` podria estar desactivado en el entorno efectivo.
- Evidencia disponible: defaults fail-closed y diagnostico incompleto.
- Evidencia faltante: environment efectivo del container posterior al ultimo despliegue.
- Prueba necesaria: endpoint diagnostico/readiness y un canary fake/controlado, sin orden real.

### Correccion actual ante respuesta Binance invalida

- Hipotesis: intent durable + reconciliacion evita repetir el fallo legacy.
- Evidencia disponible: codigo y tests locales del coordinador/normalizadores.
- Evidencia faltante: evento MICRO_LIVE posterior a los commits actuales.
- Prueba necesaria: canary MICRO_LIVE con adaptador controlado o cuenta de canary aprobada externamente.

### Guard full persistido

- Hipotesis: metadata de promocion demuestra que existio una full decision valida, pero no conserva todo su fingerprint.
- Evidencia disponible: `linkedShadowAllocationId`, `promotedFromShadowAt`, source ranking version y auditoria de promocion.
- Evidencia faltante: hash/version completa del payload full en una fuente durable unica.
- Prueba necesaria: contrato versionado extremo a extremo con `ms-metrica-cuenta` antes de declarar CAS completo.

### Presupuesto global de conexiones

- Hipotesis: 40 conexiones por replica no escala a tres replicas con `max_connections` cercano a 100.
- Evidencia disponible: configuracion de signals y valor QA observado.
- Evidencia faltante: suma efectiva de pools de todos los servicios y conexiones reservadas.
- Prueba necesaria: inventario de deployments y `pg_stat_activity` por `application_name` durante carga.
