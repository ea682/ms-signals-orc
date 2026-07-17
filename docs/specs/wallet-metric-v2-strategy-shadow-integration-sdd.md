# SDD: Integracion strategy-level Wallet Metric V2 en Signals

Estado: APROBADO PARA IMPLEMENTACION  
Fecha: 2026-07-13  
Repositorio: `ms-signals-orc`  
Metodo: Specification-Driven Development

## 1. Problema actual

`MetricWalletServiceImpl` consume `/operaciones/metrica/joyas` en summary y un endpoint
separado de copy guard. Mantiene dos caches Caffeine, dos last-known-good y fallbacks
por wallet. Summary puede alimentar seleccion y SHADOW aunque no sea decision final.
Los indices secundarios `byWalletId` y `byWalletStrategy` pueden devolver otra unidad
de una wallet. Las claves actuales normalizan scopes de forma incompatible con el
contrato V2 (`strategy/default` o lowercase).

El hot path correctamente evita HTTP, pero su snapshot no conoce `generationId`, no
invalida atomically summary/full/guard y puede reutilizar datos V1 vencidos. Existe un
flag `fallback-all-users-on-empty-allocation`; su default es false, pero V2 debe impedir
ese comportamiento de forma estructural.

## 2. Inventario real y decisiones

| Elemento | Uso actual | Decision | Evidencia | Accion |
|---|---|---|---|---|
| `MetricWalletsInfoClient` | Cliente unico para history/joyas/guard/decision | ADAPT | Consumido por `MetricWalletServiceImpl` | Mantener un cliente, agregar contrato V2 canonico |
| `MetricWalletServiceImpl` | Discovery, caches, distribucion y guard | CONSOLIDATE | Mezcla fetch, cache, fallback y policy | Extraer store V2 generation-aware y conservar facade |
| `MetricCopyDecisionGateway` | Decision exacta para promociones | ADAPT | Consumidor real de `/copy-decision` | V1 conserva DTO rollback; V2 consume contrato canonico y adapta fail-closed sin habilitar dinero |
| `MetricaWalletDto` | Contrato V1 grande | KEEP_TEMPORARY | V1 rollback y muchos consumidores | No mapear V2 dentro de este DTO |
| `CopyDecisionDto` | Full V1 exacto | CONSOLIDATE | Semantica solapada | Reemplazar consumo operativo por contrato canonico V2 |
| `CopyGuardWindowSnapshotDto` | Guard V1 | ADAPT | Runtime existente | Alinear encabezado V2 y strategyKey |
| `CopyStrategyRuntimeRouter` | Routing por estrategia | REUSE/ADAPT | Hot path y asignaciones | Canonicalizar identidad uppercase |
| `CopyStrategyGuardRuntimeCache` | Puerto hot path | ADAPT | Candidate resolver | Resolver por action e identidad completa |
| `CopyRuntimeGuardPolicy` | Guard de nueva entrada | ADAPT | Candidate resolver | Fail closed en V2; distinguir exposure/exit |
| `HyperliquidCopyCandidateResolver` | Usuarios para OPEN/CLOSE/RESIZE/FLIP | ADAPT | Hot path real | Summary nunca autoriza; CLOSE/REDUCE no se bloquean |
| `ShadowCopyTradingServiceImpl` | Enrolamiento y contabilidad SHADOW | ADAPT | Ya persiste strategyKey | Enrolar solo decision FULL vigente |
| `UserCopyAllocationEntity` | Allocation LIVE/MICRO/SHADOW | ADAPT | Tiene scope y strategyKey | Canonicalizar defaults y backfill aditivo |
| `ShadowCopyAllocationEntity` | Allocation SHADOW strategy-level | ADAPT | Tiene scope y strategyKey | Asociar decision/generation V2 |
| `CopyWalletProfileEntity` | Perfil de wallet/estrategia | ADAPT | Certificacion/promocion | Mantener identidad completa |
| `ShadowCopyOperation*` y `ShadowPositionState` | Evidencia SHADOW | REUSE | Claves incluyen allocation | No duplicar tablas |
| `CopyDispatchIntentEntity` | Idempotencia de dispatch | ADAPT | Ya guarda strategy/scope | Incluir strategyKey en fingerprint/key donde falte |
| `MetricWalletDistributionJobWorker` | Refresh/distribucion | ADAPT | Llama facade | Orquestar summary -> finalists full -> guard |
| `lastKnownGoodHistory` V1 | Fallback de red | KEEP_TEMPORARY | Rollback V1 | Inaccesible en modo V2 |
| `byWalletId` / `byWalletStrategy` fallback | Busqueda laxa | DELETE del flujo V2 | Puede cruzar scopes | V2 solo exact strategyKey+generation |
| `fallbackAllUsersOnEmptyAllocation` | Opcion peligrosa | DEPRECATE | Campo inyectado en resolver | Forzar false en V2 y documentar retiro |

## 3. Contrato canonico

Signals usa un solo modelo `MetricStrategySnapshot` para summary/full/guard con:

- `metricVersion=2`, `sourceVersion`;
- `generationId`, `generationActivatedAt`, `computedAt`, `dataAsOf`;
- `walletId`, `strategyCode`, `scopeType`, `scopeValue`, `strategyKey`;
- `certificationStatus`, `degradationState`;
- `allowNewEntries`, `decisionFinal`;
- `qualityFlags`, `reasonCodes`;
- `completeCycles`, `historyDays`, `dataFreshnessSeconds`;
- `coverage`, `unknownEconomicFields`, `evaluationMode`.

Campos requeridos ausentes hacen el objeto no operable. UNKNOWN se preserva como null.
Los fixtures compartidos bajo `src/test/resources/contracts` prueban compatibilidad con
las respuestas Nest.

Los endpoints estables conservan `limit` solo para consumidores V1/rollback. En el
contrato V2, `limitWallet` expresa la cantidad maxima de unidades strategy-level que
se retornan y `minOperations` expresa el minimo de operaciones. Signals debe enviar
`limitWallet` en discovery, full y copy guard; no debe depender del alias legacy
`limit`, aunque Metrics lo acepte temporalmente por compatibilidad. Una prueba sobre
el HTTP real protege tanto el nombre del query param como la deserializacion Nest/Java.

## 4. Identidad

`strategyKey = lower(walletId)|upper(strategyCode)|upper(scopeType)|upper(scopeValue)`.

Toda persistencia, cache, comparacion, idempotencia y log usa esa clave. Nunca se busca
por wallet como fallback. `MOVEMENT_ALL` antiguo se normaliza a `ALL|ALL`; LONG a
`DIRECTION|LONG`; SHORT a `DIRECTION|SHORT`; SYMBOL conserva `SYMBOL|<SYMBOL>`.

## 5. Read mode

`METRIC_WALLET_READ_MODE=V1|COMPARE|V2` se resuelve en un componente unico.

- V1: facade y caches antiguos, solo rollback.
- COMPARE: refresca V1 y V2, compara por strategyKey, pero runtime continua con la
  fuente configurada y una respuesta compare nunca autoriza nuevas exposiciones V2.
- V2: solo endpoints/contratos V2; sin V1 cache, sin last-known-good V1 y sin fallback.

Produccion conserva default COMPARE/V1 hasta cutover manual. El modo se registra al
arranque con `fallbackEnabled=false`.

## 6. Flujo de refresh

### Discovery

El scheduler consulta summary V2 en batch, valida contrato y reemplaza atomicamente el
ranking. Summary solo identifica finalistas: siempre `decisionFinal=false` y no crea
ni activa SHADOW.

### Full

Para cada finalista se obtiene full fuera del hot path, con concurrencia acotada y
singleflight por `strategyKey+generationId`. Solo un FULL vigente con
`eligibleForShadow`, `decisionFinal=true` y `allowNewEntries=true` puede alimentar
enrolamiento SHADOW. La decision se persiste/cacha con razones y expiracion.

### Copy guard

Guard se refresca en batch para la misma generacion y se publica junto a full. No se
publica un snapshot si summary/full/guard tienen generaciones distintas.
FULL debe ser subconjunto exacto por `strategyKey` de SUMMARY y copy guard debe tener
exactamente las mismas keys que FULL. La igualdad de generation por wallet no basta,
porque dos scopes distintos pueden compartir generationId.

## 7. Cache generation-aware

Un aggregate inmutable contiene summary, full y guard de una generacion. Clave exacta:
`strategyKey+generationId+metricVersion`. Metadata: fetchedAt, dataAsOf, expiresAt,
decisionFinal, allowNewEntries y reasonCodes.

Al detectar una nueva generacion se construye el aggregate completo y luego se hace un
swap atomico. El aggregate anterior no se combina con ventanas nuevas. Singleflight
evita refresh concurrente duplicado. TTL:

- summary `PT2M`;
- full `PT10M`;
- guard `PT2M`;
- max staleness `PT10M`.

Timestamps anteriores al TTL o adelantados mas de un minuto respecto del reloj local
se consideran invalidos para abrir exposicion; el margen de un minuto tolera clock skew
normal sin aceptar snapshots fechados arbitrariamente en el futuro.

El reemplazo persistido se serializa tambien entre replicas mediante un advisory lock
transaccional PostgreSQL. Al finalizar siempre existen cero filas o un aggregate completo
de una sola publicacion; nunca una mezcla de dos generaciones concurrentes.

Un cambio del conjunto de `strategyKey` en SUMMARY fuerza refresh coordinado de FULL y
guard aun cuando la wallet conserve el mismo generationId. Cambios solo de ranking no
invalidan FULL antes de su TTL.

## 8. Seguridad por accion

La decision runtime recibe `OPEN`, `INCREASE`, `REDUCE`, `CLOSE` o `FLIP`:

| Estado metric | OPEN | INCREASE | REDUCE con posicion | CLOSE con posicion | FLIP |
|---|---|---|---|---|---|
| FULL+guard vigentes y permitidos | permite | permite | permite | permite | cierra y abre |
| Summary solamente | bloquea | bloquea | permite | permite | cierra, bloquea apertura |
| Caida/timeout | bloquea | bloquea | permite | permite | cierra, bloquea apertura |
| Stale/UNKNOWN/contrato invalido | bloquea | bloquea | permite | permite | cierra, bloquea apertura |
| Guard negativo | bloquea | bloquea | permite | permite | cierra, bloquea apertura |

Permitir un exit exige una posicion activa conocida; no se inventa una posicion.
Ninguna falla de metricas convierte respuesta vacia en todos los usuarios.

## 9. SHADOW independiente

Una misma wallet puede tener cuatro o mas allocations SHADOW, una por strategyKey.
Ranking no deduplica wallet. Pausar un scope no pausa otro. Enrolamiento automatico
permanece apagado por default (`METRIC_WALLET_V2_SHADOW_AUTO_ENROLL_ENABLED=false`).
Cuando se habilite explicitamente, la sincronizacion V2 solo acepta la fuente
`metric_v2_full_cache`; summary, cache vacia, datos stale o cualquier fuente V1 no
pueden enrolar SHADOW por ese camino. El interruptor V2 no cambia el comportamiento
del rollback V1 y debe estar enlazado a una property real con pruebas para ambos valores.
Una publicacion V2 vacia se trata como refresh no disponible y no invalida en masa las
asignaciones persistidas; la expiracion/guard bloquea nuevas exposiciones por separado.

No se habilita MICRO_LIVE ni LIVE en este trabajo. Promocion y certificacion existentes
continuan leyendo evidencia persistida, pero no se ejecutan en pruebas integradas.
Su gateway exacto en V2 valida contrato, identity y generation contra el guard cacheado;
expone `canShadow` solo con FULL+guard y fuerza `canMicroLive=false/canLive=false` con
`V2_MONEY_PROMOTION_DISABLED`. COMPARE conserva la fuente V1 y nunca opera con V2.

## 10. Persistencia y migracion

Se inspeccionaron las entidades solicitadas. Allocation y shadow allocation ya poseen
scope y strategyKey, por lo que no se crea una tabla nueva. Se agrega una migracion
aditiva posterior a `V202607130006` para:

- normalizar valores faltantes/default a la identidad canonica;
- reconstruir `strategy_key` deterministamente;
- reforzar indices unicos strategy-level donde la base aun use wallet+strategy;
- agregar metadata de metricVersion/generation/decision solo si no existe;
- preservar filas historicas y evitar duplicados.

No se edita ninguna migracion aplicada ni se eliminan columnas.

## 11. COMPARE

El comparador registra presencia, ciclos, historia, PnL, win rate, PF, expectancy,
drawdown, ranking, eligibility y guard por strategyKey. `event=metric_v1_v2.compare`.
No mezcla DTO ni habilita operaciones. Queda aislado para poder retirarlo.

## 12. Observabilidad

Micrometer:

- `signals.metric_v2.summary.refresh.total`;
- `signals.metric_v2.full.refresh.total`;
- `signals.metric_v2.copy_guard.refresh.total`;
- `signals.metric_v2.cache.hit.total`;
- `signals.metric_v2.cache.miss.total`;
- `signals.metric_v2.generation.change.total`;
- `signals.metric_v2.open.blocked.total`;
- `signals.metric_v2.close.allowed_on_metric_failure.total`;
- `signals.metric_v2.contract.error.total`.

Logs incluyen event, identidad completa, generationId, version, decision, reason y
elapsedMs. Tags Micrometer son acotados; strategyKey no se usa como tag.

## 13. Fallos y disponibilidad

HTTP timeout, 5xx, JSON invalido, ACTIVE ausente, generationId ausente, contrato stale
o mismatch de strategyKey mantienen el ultimo aggregate V2 solo para diagnostico. Si
esta vencido no autoriza exposicion. V1 no se consulta en modo V2.

CLOSE/REDUCE usan cache de posiciones y no esperan HTTP. Full nunca se solicita en el
procesamiento de un delta.

## 14. Pruebas RED/GREEN

Se cubren los 35 casos solicitados: fixtures summary/full/guard, identidad, scopes,
generation cache e invalidacion, summary no SHADOW, full allow/block, seguridad por
accion, FLIP split, guard, ausencia fallback, stale/UNKNOWN, idempotencia, allocations
antiguas, Spring context, PostgreSQL y build Maven.

Ademas se prueba que COMPARE no opera, que faltan campos requeridos bloquean y que
dos simbolos de la misma wallet conservan objetos distintos.

## 15. Configuracion

- `METRIC_WALLET_READ_MODE=COMPARE`;
- refresh summary/full/guard;
- max staleness;
- fail-open new exposure=false;
- allow close on failure=true;
- shadow auto-enroll=false;
- default execution mode SHADOW.

YAML y env examples contienen defaults; `.env.prod` no se modifica. Cada property
tiene binding test o uso directo demostrado.

## 16. Performance

Refresh no ocurre en hot path. Summary y guard son batch; full usa concurrencia
acotada y singleflight. Se mide latencia, calls y cache ratio. El guard hot path debe
ser lookup in-memory sin HTTP/DB. No se promete una latencia no medida.

## 17. Cutover y rollback

Cutover: Metrics COMPARE, validacion, Signals COMPARE con runtime V1, Metrics V2,
bloqueo temporal de nuevas exposiciones, Signals V2 y solo SHADOW.

Rollback: Signals a COMPARE/V1 y Metrics a COMPARE/V1. No se cierran posiciones por
cambio de modo y los exits continuan. La condicion para retirar V1 es una ventana de
operacion V2 estable, comparacion sin diferencias bloqueantes y rollback verificado.

## 18. Criterios de aceptacion

Contrato compartido verificado por fixtures; cuatro strategyKey independientes;
summary no autoriza; full y guard de una misma ACTIVE si autorizan; falla/stale bloquea
OPEN/INCREASE pero no exits existentes; FLIP no abre sin decision; cache atomicamente
generation-aware; no fallback V1/todos los usuarios; migracion aditiva; tests, Spring,
PostgreSQL, Maven, benchmark y diff final registrados; cero ordenes reales.
