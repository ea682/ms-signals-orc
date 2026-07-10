# SDD: Idempotencia y reconciliacion de Copy Execution

Estado: aprobado para implementacion
Fecha: 2026-07-09
Servicios: `ms-signals-orc`, `ms-binance-engine`

## 1. Incidente y diagnostico

Una orden MARKET real podia quedar `FILLED` en Binance y no quedar registrada en
`copy_operation`/`copy_operation_event`. El validador local
`BinanceEngineServiceImpl.isValidOrderResponse` exigia, ademas de `orderId` y
cantidad ejecutada, precio ejecutable y `updateTime`. Una respuesta valida con
`orderId`, `status=FILLED`, `executedQty>0` y `avgPrice=null` era rechazada.

El fallo salia por el pipeline compartido de MICRO_LIVE y LIVE. El pending se
mantenia solo en `ActiveCopyOperationCache`, podia eliminarse o perderse al
reiniciar, y `CopyExecutionJobWorker` clasificaba el error como reintentable. El
retry volvia a enviar la orden. Binance no garantiza deduplicacion permanente de
un `clientOrderId` una vez que una orden ya termino, por lo que pudo crear varios
`orderId` reales para la misma intencion.

## 2. Unidad exacta de ejecucion

Una intencion unica se identifica por:

- `id_user`
- `user_copy_allocation_id` (o un scope legacy explicito)
- `execution_mode`
- `strategy_code`, `scope_type`, `scope_value`
- identidad inmutable del evento/delta fuente
- `copy_intent` (`OPEN`, `INCREASE`, `REDUCE`, `CLOSE`, `FLIP_CLOSE`,
  `FLIP_OPEN`, `REOPEN`)

La identidad fuente preferida es el `idempotencyKey` recibido de Hyperliquid. Se
propaga en `OperacionEvent.sourceEventId` y sobrevive en el payload durable del
job. Para eventos legacy, el fallback es el `clientOrderId` determinista ya
construido a partir del trigger, origin, target qty, usuario y allocation. No se
usa solo `id_order_origin`: una misma posicion puede emitir OPEN, varios RESIZE y
CLOSE con ese origin.

Clave canonica:

```text
sha256(v1|user|allocation|mode|strategy|scopeType|scopeValue|
       exactSourceEventIdentity|copyIntent)
```

El `clientOrderId` es determinista desde esa clave, maximo 36 caracteres y solo
`[A-Za-z0-9._-]`. Es una ayuda de consulta, no la fuente de idempotencia.

Antes de crear el intent real, el coordinador debe rechazar sin I/O:

- allocation ausente o con modo distinto de `MICRO_LIVE`/`LIVE`;
- cantidad nula, cero, negativa o no numerica;
- ausencia simultanea de `sourceEventId`, `clientOrderId` y `idOrderOrigin`.

Allocations diferentes producen claves diferentes. Por lo tanto, una allocation
MOVEMENT_ALL y otra SHORT_ONLY/LONG_ONLY pueden copiar el mismo delta una vez
cada una. No existe deduplicacion global por wallet u origin.

## 3. Reconocimiento de una orden

El normalizador clasifica la respuesta sin depender de datos secundarios:

- `FILLED` con `orderId != null` y `executedQty > 0`: aceptada y ejecutada.
- `PARTIALLY_FILLED` con `orderId != null` y `executedQty > 0`: aceptada,
  persistible y reconciliable.
- `NEW` con `orderId != null`: aceptada, todavia no ejecutada.
- respuesta sin `orderId`, status desconocido o parse incompleto: ambigua.
- rechazo explicito 4xx/Binance: rechazo definitivo.

`avgPrice`, `price`, `cumQuote` y `updateTime` no determinan si Binance acepto la
orden. El precio se resuelve en este orden:

1. `avgPrice > 0`.
2. `price > 0`.
3. `cumQuote / executedQty`.
4. lookup por `orderId`.
5. lookup por `clientOrderId`.
6. fills/trades por `orderId`, enriquecidos por ms-binance dentro del lookup
   read-only y fuera del hot path.

Si sigue ausente, se persiste `average_price_status=PENDING_RESOLUTION`. Para una
apertura, `copy_operation.price_entry` puede usar el precio de referencia ya
validado para sizing, marcado como provisional; no se presenta como fill final.
Nunca se reenvia solo por faltar precio promedio.

## 4. Maquina de estados durable

La tabla `futuros_operaciones.copy_dispatch_intent` existe antes de invocar HTTP:

```text
CREATED -> DISPATCHING -> NEW/PARTIALLY_FILLED/FILLED -> PERSISTED
                    \-> RECONCILING -> ...
                    \-> REJECTED/FAILED_FINAL
FILLED/PARTIALLY_FILLED -> PERSISTENCE_PENDING -> PERSISTED
```

Reglas:

- `idempotency_key` tiene unique constraint.
- `INSERT ... ON CONFLICT DO NOTHING` crea la intencion.
- `UPDATE CREATED -> DISPATCHING` concede a una sola replica derecho de envio.
- un duplicado en `DISPATCHING/RECONCILING` no envia y deja actuar al reconciler.
- un duplicado ya reconocido reutiliza el snapshot persistido para reintentar
  solo cuando `copy_operation_id` sigue ausente o el estado es
  `PERSISTENCE_PENDING`.
- un intent con `copy_operation_id` ya aplicado responde NOOP; no vuelve a
  ejecutar la matematica local de OPEN/INCREASE/REDUCE/CLOSE.
- `REJECTED/FAILED_FINAL/CANCELLED` no se reenvian automaticamente.
- ninguna transicion ambigua libera presupuesto.

Cada intent guarda un `request_hash` canonico de simbolo, side, positionSide,
tipo, qty, leverage, margen, notional, precio de referencia, reduceOnly y
clientOrderId. Reutilizar la misma idempotency key con otro payload es un
conflicto fail-closed: no altera el intent original y no llama Binance.

`PARTIALLY_FILLED` no es terminal mientras Binance mantenga la orden abierta.
Cada persistencia usa `delta = executedQty acumulada - persistedExecutedQty` para
no sumar/restar dos veces el fill ya aplicado. Un `CANCELED/EXPIRED` con cantidad
ejecutada se trata como fill parcial terminal y conserva esa ejecucion; sin
cantidad es rechazo definitivo. Un intent `PERSISTED` con precio provisional
sigue elegible para resolver precio. NEW, partial y precio pendiente tienen
maximo de intentos y terminan fail-closed/manual, nunca en loop infinito.

El progreso del ledger es unico por
`dispatch_intent_id + event_type + qty_executed + resulting_qty`. Antes del
insert se consulta esa misma clave para que una reconciliacion secuencial
repetida sea NOOP sin provocar rollback; el indice unico mantiene la defensa
final ante una carrera concurrente.

## 5. Clasificacion de errores

### A. Definitivamente no enviado

Validacion local, simbolo/cantidad invalida o presupuesto rechazado antes de HTTP.
Estado final y reserva liberada. No requiere lookup.

### B. Rechazo definitivo

Binance confirma que no creo la orden. Estado `REJECTED`, reserva liberada. No hay
retry automatico.

### C. Resultado ambiguo

Timeout, conexion cortada, 5xx con envio posible, respuesta incompleta, parse
fallido o error de persistencia despues del ack. Estado `RECONCILING` o
`PERSISTENCE_PENDING`, reserva retenida y cero reenvios ciegos. Solo una
confirmacion inequívoca de no existencia podria habilitar una politica futura de
reenvio; esta version queda fail-closed y nunca reenvia automaticamente.

## 6. Presupuesto MICRO_LIVE

El limite es por `(id_user, user_copy_allocation_id, MICRO_LIVE)`:

- margen maximo por nueva orden: 20 USDC.
- margen total usado o reservado: 100 USDC.
- posiciones abiertas o slots reservados: 5.

Formula:

```text
usedMargin + reservedPendingMargin + requestedMargin <= 100
openPositions + reservedNewPositions <= 5
requestedMargin <= 20
```

El leverage cambia notional, no el margen permitido. El saldo global de Binance
no sustituye el presupuesto de la allocation. La reserva se serializa mediante
advisory transaction lock de Postgres por usuario/allocation/modo. Una intencion
duplicada reutiliza la misma reserva. `RECONCILING` y `PERSISTENCE_PENDING`
conservan reserva; rechazo definitivo la libera; `PERSISTED` la confirma y el
margen pasa a provenir de `copy_operation` activa.

LIVE comparte idempotencia, claim, reconciliacion y reservas pending, pero no los
limites fijos 20/100/5. Su sizing sigue siendo exposure-based y se calcula antes
de dispatch con el capital propio de usuario/allocation.

## 7. Persistencia y crash recovery

Orden obligatorio:

1. Crear intent y reservar.
2. Claim atomico.
3. Enviar a Binance.
4. Normalizar y persistir inmediatamente `orderId/status/qty`.
5. Persistir `copy_operation_event`.
6. Persistir/upsert `copy_operation`.
7. Actualizar cache.
8. Marcar intent `PERSISTED` y confirmar reserva.

El ledger requerido no puede degradarse silenciosamente: si faltan sus campos
minimos o falla su insercion, la transaccion de persistencia falla y el intent
permanece `PERSISTENCE_PENDING`. Los eventos best-effort legacy no cambian.

Si 5-7 fallan, el intent queda `PERSISTENCE_PENDING`; no se ejecuta panic-close
ni otro envio. Al reiniciar, `CopyOrderReconciliationWorker` toma batches con
`FOR UPDATE SKIP LOCKED`, consulta Binance primero por `binance_order_id` cuando
ya fue reconocido y usa `client_order_id` como fallback cuando aquella identidad
no existe o no aparece. Ninguno de esos lookups envia una orden. Luego reconstruye
idempotentemente el estado local y termina el intent. Un item fallido no detiene
el batch. El claim deja un lease en `next_reconciliation_at` de al menos
`dispatch-stale-after`, para que otra replica no procese el mismo intent mientras
la primera sigue consultando/persistiendo. Hay backoff, maximo de intentos y
`next_reconciliation_at`.

## 8. Operaciones soportadas

- OPEN/REOPEN: reserva margen y slot; crea/upserta posicion activa.
- INCREASE: reserva margen, no un slot nuevo; actualiza qty/precio promedio.
- REDUCE: no reserva margen/slot de nueva exposicion; mantiene reserva del intent
  mientras el resultado sea ambiguo y reduce qty idempotentemente.
- CLOSE: reduce-only; cierra localmente una vez confirmado el fill.
- FLIP: dos intents independientes (`FLIP_CLOSE`, `FLIP_OPEN`). El segundo solo
  se evalua con la regla de negocio existente y nunca comparte clave con el
  cierre.

## 9. Hot path y performance

La ingesta/candidate resolution sigue usando caches y enqueue. No consulta
metricas ni ejecuta simulacion full. La escritura/claim durable vive en el
dispatch worker, no antes del enqueue. Coste esperado por primer dispatch: una
transaccion corta con advisory lock, insert/select y claim. Duplicado: lookup o
conflict + select, sin HTTP.

Indices: unique `idempotency_key`, status/next retry, allocation/mode/reservation,
`client_order_id`, `binance_order_id`, `source_event_id`.

Objetivos: pre-enqueue p95 < 20 ms; candidate p95 < 5 ms; guard p95 < 2 ms;
reserva p95 < 5 ms en DB local; dedupe p95 < 10 ms. La reconciliacion corre fuera
del hot path.
El snapshot MICRO_LIVE de margen usado, margen pending, posiciones abiertas y
slots reservados se obtiene con una sola consulta agregada bajo el advisory lock,
no con cuatro round trips secuenciales.

El resolver de candidatos depende de `CopyStrategyGuardRuntimeCache`, no de
`MetricWalletService`. La lectura runtime solo puede usar `getIfPresent` y el
ultimo snapshot valido ya publicado; nunca puede disparar el loader Caffeine,
HTTP de `/joyas`, copy-guard windows, DB ni simulacion. Un cache miss aplica la
politica fail-open/fail-closed configurada y se observa con reason code, mientras
el job programado refresca snapshots fuera del hot path.

## 10. Observabilidad y seguridad

Se emiten eventos estructurados `copy.dispatch.*`, `copy.budget.*` y
`copy.reconciliation.*` con intent/allocation/modo. Los snapshots se sanitizan;
no se guardan ni loguean API key, secret o firmas.

Tests y validaciones usan mocks. No envian, cierran ni cancelan ordenes reales.
La herramienta de incidente es dry-run por defecto y solo propone backfill.
Su modo apply solo puede enriquecer un intent existente cuando el CSV contiene
allocation, source event y exactamente una orden Binance para esa intencion.
Duplicados o mappings incompletos quedan fuera de apply y requieren revision
manual; nunca se elige un orderId arbitrario.

## 11. Auditoria LIVE

LIVE usa `BinanceEngineServiceImpl`, `ProcesBinanceServiceImpl`,
`CopyExecutionJobWorker`, `CopyTradingMapper` y el mismo cliente de Binance que
MICRO_LIVE. Antes del cambio, OPEN, INCREASE, REDUCE, CLOSE y ambas piernas de
FLIP estaban expuestas a reintento tras respuesta/persistencia ambigua; dos
replicas dependian de locks/cache no durables. El fix se aplica en el coordinador
comun. El sizing LIVE y sus guards no se reemplazan por limites MICRO_LIVE.

## 12. Despliegue y rollback

1. Deshabilitar nuevas entradas MICRO_LIVE; mantener reductions/closes.
2. Ejecutar el preflight de duplicados y desplegar la migracion aditiva. Crear la
   tabla de intents y agregar columnas nullable tiene lock breve; reemplazar el
   indice activo de `copy_operation` escanea esa tabla y su lock depende de la
   cardinalidad. Debe programarse en ventana de mantenimiento si la tabla es
   grande. La migracion falla, sin borrar filas, si existen duplicados que violen
   la nueva unicidad por allocation exacta.
3. Desplegar ms-binance-engine y luego ms-signals-orc.
4. Ejecutar reconciliacion dry-run y validar orfanas.
5. Habilitar una allocation canary con capital minimo.

Rollback de aplicacion: volver binarios manteniendo tabla/columnas aditivas. No se
elimina informacion de intents durante rollback. El DDL inverso solo debe usarse
despues de confirmar que no quedan intents pendientes.
