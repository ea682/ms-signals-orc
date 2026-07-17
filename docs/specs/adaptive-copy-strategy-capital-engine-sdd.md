# SDD: Adaptive Copy Strategy Capital Engine

Estado: APROBADO PARA IMPLEMENTACION  
Fecha: 2026-07-14  
Repositorio: `ms-signals-orc`

## 1. Objetivo

Consumir evaluaciones strategy-level y convertirlas en una decision operativa segura,
sin habilitar dinero automaticamente. Identidad canonica:

`lower(walletId)|upper(strategyCode)|upper(scopeType)|upper(scopeValue)`.

Una wallet puede tener varias estrategias/estados/capitales. Solo el portafolio agrega
su riesgo conjunto.

## 2. Inventario y decisiones

| Componente | Decision | Accion |
|---|---|---|
| modulo interno `copy-target-core` | REUSE/ADAPT | Unica matematica de sizing |
| `MetricV2SnapshotStore` | ADAPT | Cache por strategy+generation+versions |
| `MetricCopyDecisionGateway` | ADAPT | Validar scores/decision y fail-closed |
| `CopyRuntimeGuardPolicy` | ADAPT | Gate por tipo de accion |
| `CopyBudgetResolver` | REUSE/ADAPT | MICRO 100 USDC x5; LIVE proporcional |
| `PostgresCopyDispatchIntentStore` | REUSE | Idempotencia/reserva/exit always allowed |
| promotion/readiness services | CONSOLIDATE | Lifecycle unico y promociones disabled |
| allocation/distribution | ADAPT | CapitalMultiplier/capacity/portfolio |
| fallback laxo por wallet | DELETE de V2 | Nunca cruzar scopes |

## 3. Inputs y validacion

Se requieren generationId, strategyKey, decisionFinal, scores con disponibilidad,
operational state, capitalMultiplier, capacity, reasonCodes y versions. Summary solo
descubre. Full solo habilita SHADOW. Cualquier mismatch de identidad/generacion/version,
staleness o campo obligatorio UNKNOWN bloquea nuevas exposiciones.

FULL debe transportar `simulationMatrix` con la misma `strategyKey` y `generationId`.
Una matriz ejecutable exige `available=true`, status COMPLETE, exactamente 44 escenarios
y las 44 combinaciones capital x leverage canonicas. Ausencia, strategyKey distinta o
generationId distinta bloquean toda exposicion nueva. UNKNOWN e INCOMPLETE se preservan
con sus reasonCodes y permiten solo SHADOW para recolectar evidencia; bloquean MICRO_LIVE,
LIVE y la seleccion de una banda real. Nunca bloquean CLOSE ni REDUCE.

## 4. Lifecycle y escalera

Estados: CANDIDATE -> SHADOW -> PROBATION -> MICRO_LIVE -> ACTIVE; degradacion a WATCH,
PAUSED o RETIRED. Estado, executionMode, multiplier y guard son campos separados.

Multipliers: 0, .25, .50, .75, 1. Escalera LIVE: 25/50/75/100%. La degradacion normal
solo afecta exposicion futura; no fuerza resize de posiciones abiertas. Emergencia puede
solicitar REDUCE/CLOSE solo si el flag de reduccion activa se habilita manualmente; default
false. Reingreso exige varias evaluaciones sanas y sube un escalon por vez.

MICRO_LIVE usa exactamente capital total 100 USDC y x5 por user+wallet, sin cuota fija
por operacion ni maximo global fijo. El presupuesto compartido bloquea OPEN/INCREASE al
agotarse; CLOSE/REDUCE siempre pasan. MICRO/LIVE y promociones quedan disabled por default.

## 5. Acciones y copy guard

- OPEN/INCREASE: FULL vigente, allowNewEntries, guard ALLOW, equity fresca, capacity y
  portfolio disponibles.
- CLOSE/REDUCE: se permiten aun con score/metricas/equity stale o estrategia pausada.
- FLIP: cerrar lado actual de forma idempotente; reevaluar el nuevo OPEN desde cero.
- WARNING: permite nueva exposicion con trazabilidad.
- REDUCE_CAPITAL: aplica multiplier menor solo a nuevas exposiciones.
- PAUSE_OPEN/RETIRED: bloquea OPEN/INCREASE, conserva exits.

Observabilidad Micrometer obligatoria en el punto central de guard:

- `signals.copy_guard.warning.total` para WARNING.
- `signals.copy_guard.reduce.total` para REDUCE_CAPITAL.
- `signals.copy_guard.pause.total` para cualquier decision que bloquee exposicion.
- `signals.copy_guard.recovery.total` solo para una decision ALLOW con reason de
  recuperacion explicito; un ALLOW normal no cuenta como recovery.
- `signals.portfolio.correlation_limit.total` y
  `signals.portfolio.capacity_limit.total` cuando el guard/budget contiene esos reason
  codes. UNKNOWN no se cuenta como limite superado.
- `signals.metric_generation.change.total` al publicar atomicamente una generation
  distinta, manteniendo temporalmente el contador legacy como alias de dashboard.

## 6. Capital, leverage y capacidad

Sizing canonico:

`ratio = abs(sourceNotional)/sourceEquity`  
`targetNotional = allocatedCapital * ratio * capitalMultiplier`  
`requiredMargin = targetNotional/targetLeverage`.

Se redondea hacia abajo, no se eleva a minNotional, se aplica common scaling y FLIP
close-first. La banda se limita por normal/stress/emergency capacity, available margin,
symbol rules, liquidation survival y portfolio allocation. Leverage se selecciona por
robustness, nunca para inflar notional.

## 7. Portafolio

El allocator considera beta, concentracion wallet/symbol/side, correlacion normal/stress,
common drawdown, margin stress y follower notional agregado. Una correlacion alta reduce
allocation/multiplier, no elimina evidencia ni borra la estrategia. UNKNOWN de correlacion
o hedging baja Evidence y bloquea escalamiento agresivo.

Rotacion exige beneficio esperado neto mayor que costes de cerrar/reabrir, impacto,
impuestos no modelados y MinimumPromotionAdvantage. La histeresis del Top10 se respeta.

## 8. BNB policy

Solo forecast, sin conversion real:

`requiredBnbFeeBuffer = clamp(expectedFees * safetyFactor, minAbsolute, maxAbsolute)`.

Expone capital actual/requerido, ahorro esperado y risk status. UNKNOWN de precio BNB
no se convierte a cero. Auto conversion y llamadas privadas permanecen disabled.

## 9. Cache, idempotencia y observabilidad

Cache key: strategyKey+generationId+calculatorVersion+policyVersion+capital+leverage+mode.
El cambio de generation invalida atomicamente summary/full/windows/matrix/decision.
Idempotencia incluye sourceEventId, allocation, strategyKey, versions y action. Duplicate
FLIP no duplica close ni new side.

Metricas/logs cubren guard warning/reduce/pause/recovery, capacity/correlation limits,
generation change y decisiones con seis scores, multiplier, reasons y elapsedMs.

## 10. Pruebas y aceptacion

Tests: identidad, cache, state transitions, reentry lento, action gates, FLIP, MICRO 100/x5,
capacity, portfolio, rotation, BNB forecast/no conversion, generation invalidation e
idempotencia. Spring context y contratos Nest/Java deben pasar. Ninguna prueba envia orden
real; MICRO/LIVE permanecen disabled en defaults y ejemplos productivos.

## 11. Cutover y rollback

COMPARE -> V2 cache sin exposures -> SHADOW -> evidencia -> evaluacion manual MICRO.
Rollback vuelve COMPARE/V1, conserva decisiones/evidencia, no cambia ACTIVE y siempre
mantiene CLOSE/REDUCE.
