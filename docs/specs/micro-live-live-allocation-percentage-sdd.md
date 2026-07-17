# SDD - Semantica de allocation_pct en SHADOW, MICRO_LIVE y LIVE

> Nota de precedencia (2026-07-13):
> `copy-trading-proportional-portfolio-v3-sdd.md` gobierna el sizing economico
> V3. MICRO_LIVE usa 100 USDC totales a 5x, sin 20 USDC fijos ni maximo global
> de cinco posiciones; los limites anteriores quedan solo como historia.

## 1. Estado del documento

- Estado: especificacion aprobada para implementacion local.
- Metodo: Specification-Driven Development.
- Servicio propietario del runtime: `ms-signals-orc`.
- Servicio observado, no modificado por esta especificacion: `ms-binance-engine`.
- Fecha de investigacion: 2026-07-11.
- Rama investigada: `feature/1.4.27`.
- Commit investigado: `60b6f43e55d9c076e6169e38bbfac27977c08bc9`.

## 2. Problema

`allocation_pct` tiene hoy dos significados incompatibles:

1. porcentaje economico LIVE;
2. marcador positivo de actividad para MICRO_LIVE.

La promocion SHADOW usa `0.000001` cuando no existe porcentaje real. El valor
se persiste en `user_copy_allocation` y `user_wallet_copy_plan`. La promocion
MICRO_LIVE -> LIVE copia posteriormente el mismo valor. LIVE multiplica el
capital del cliente por ese porcentaje, por lo que el marcador se convierte en
una asignacion economica casi nula.

## 3. Evidencia comprobada

### 3.1 Escritura

- `ShadowCopyTradingServiceImpl` copia `capitalShare` a la allocation SHADOW y
  a `target_live_allocation_pct` cuando el perfil es promovible.
- `ShadowPromotionServiceImpl` usa actualmente el primer valor positivo entre
  `target_live_allocation_pct`, `shadow.allocation_pct` y `0.000001`.
- La misma promocion escribe ese valor en `user_copy_allocation` y en
  `user_wallet_copy_plan`.
- `MicroLivePromotionServiceImpl` crea LIVE copiando
  `micro.getAllocationPct()`.
- `UserCopyAllocationServiceImpl` escribe la distribucion calculada por perfil.
- `ms-wallet-metric-etl` contiene otro allocator wallet-level capaz de escribir
  `user_wallet_copy_plan` y `user_copy_allocation`, pero esta deshabilitado en
  el entorno investigado.

### 3.2 Lectura

- Cuatro consultas de `UserCopyAllocationRepository` exigen
  `coalesce(allocation_pct, 0) > 0` para promocion o diagnostico.
- `MetricWalletServiceImpl.getCandidatesUser` y el camino cached-only eliminan
  cualquier allocation cuyo porcentaje efectivo sea cero.
- El hot path de Binance dentro de este servicio necesita esa metrica para
  construir `AllocationCopyContext`; por ello MICRO_LIVE con porcentaje nulo no
  llega hoy a `CopyBudgetResolver`.
- `CopyBudgetResolver` ignora correctamente el porcentaje para MICRO_LIVE y usa
  100/20/5.
- `CopyBudgetResolver` usa `accountCapital * allocationPct` para LIVE.

### 3.3 Esquema y datos

- PostgreSQL 16.10 confirma `allocation_pct NOT NULL` en
  `user_copy_allocation` y `user_wallet_copy_plan`.
- Ambos campos tienen escala 6 y rango 0..1.
- Las allocations 505 y 506 son MICRO_LIVE activas con `0.000001`, enlazadas a
  SHADOW con porcentaje cero y target LIVE nulo.
- Un unico plan wallet-level tiene `allocation_pct=0.000001` y
  `allocated_capital_usd=100`.
- No existe LIVE activo sospechoso en el snapshot investigado.
- Las dos filas MICRO y el plan cumplen los criterios combinados de sentinel
  legacy seguro; no hay filas ambiguas en ese snapshot.

## 4. Causa raiz

La causa no es el sizing MICRO_LIVE. Es un contrato roto entre cinco capas:

1. columnas `NOT NULL`;
2. queries que usan porcentaje como bandera de actividad;
3. fallback `0.000001` sensible al origen, pero no al destino;
4. promocion MICRO_LIVE -> LIVE que hereda el porcentaje;
5. tests y documentacion que fijaron el sentinel como comportamiento valido.

Los intentos anteriores corrigieron el consumidor MICRO_LIVE, pero conservaron
la fuente, las queries, el esquema y la herencia hacia LIVE.

## 5. Semantica unica y unidad

`allocation_pct` significa exclusivamente porcentaje economico real de capital.

- `1.000000` = 100%.
- `0.100000` = 10%.
- rango persistible: 0..1 para historia y SHADOW;
- rango ejecutable LIVE: mayor que 0 y menor o igual que 1;
- escala persistida: 6, redondeo `HALF_UP`;
- `0.000001` puede ser un valor economico real solo si una distribucion vigente
  lo produjo y su fuente lo demuestra. El valor por si solo no prueba sentinel.

La unidad queda demostrada por el allocator, sus limites 0..1 y el calculo
LIVE `capital * allocationPct`.

## 6. Contrato por modo

| Modo | allocation_pct | allocated_capital_usd | Sizing |
| --- | --- | --- | --- |
| SHADOW | opcional, no ejecutable | no aplica | simulacion |
| MICRO_LIVE | NULL para filas nuevas | 100 | fijo 100/20/5 |
| LIVE | porcentaje real obligatorio | derivado o snapshot | porcentaje vigente |

### 6.1 MICRO_LIVE

- `sizing_mode=FIXED_CAPITAL`.
- `allocation_pct=NULL` en toda fila nueva.
- `allocation_pct_source=FIXED_MICRO_BUDGET`.
- Sigue activa por modo, estado, vigencia, usuario, API key, capital y guards.
- Nunca depende de `allocation_pct > 0`.
- Los porcentajes legacy nulo, `0.000001` y `0.25` se ignoran para sizing.
- Presupuesto total 100 USDC, margen maximo 20 USDC y cinco posiciones o
  reservas por usuario+wallet.

### 6.2 LIVE

- `sizing_mode=PERCENTAGE`.
- `allocation_pct` es obligatorio, positivo y <= 1.
- Debe tener fuente, source id, fecha de calculo y vigencia.
- No hereda ningun campo economico de MICRO_LIVE.
- Si no existe distribucion valida, MICRO_LIVE permanece activa.

## 7. Fuente canonica LIVE

### 7.1 Hallazgo

Hay dos allocators historicos:

- `ms-wallet-metric-etl`: wallet-level, durable, actualmente deshabilitado y sin
  detalles desde 2026-06-07;
- `ms-signals-orc`: allocator efectivo habilitado, alimentado por `/joyas`, con
  seleccion por usuario y unidad `wallet+strategy+scope`, pero sin snapshot
  durable.

Por tanto, el plan wallet-level no es hoy una fuente vigente utilizable por la
promocion y no se debe fingir que lo es.

### 7.2 Decision

La fuente economica canonica sera el resultado actual del allocator efectivo de
`ms-signals-orc`, despues de aplicar seleccion por usuario, `maxWallet`,
`maxProfilesPerWallet`, guards y reescalado.

Se materializara, sin recalcular dentro de la promocion, en:

- `live_allocation_distribution_run`;
- `live_allocation_distribution_detail`.

El snapshot no inventa otro porcentaje: vuelve durable el resultado ya usado
por `UserCopyAllocationServiceImpl`.

Cada run contiene usuario, source, `calculated_at`, `valid_until` y total del
usuario. Cada detalle contiene wallet, estrategia, scope, porcentaje de la
estrategia y total agregado de la wallet.

Cada run nace `STAGED`; solo cambia a `COMPLETED` despues de persistir todas las
unidades runtime. Una falla lo cambia a `FAILED`. El resolver siempre mira el
run mas reciente y falla cerrado si esta `STAGED` o `FAILED`, por lo que nunca
retrocede silenciosamente a una distribucion anterior. Una distribucion vacia
publica un run completo sin detalles para invalidar el resultado anterior.

## 8. Identidad y solapamiento

La identidad economica exacta es:

```text
userId + walletId + strategyCode + scopeType + scopeValue
```

La distribucion canonica vigente es tipo B: porcentaje por estrategia/scope.
Ademas persiste `wallet_total_allocation_pct`.

Invariantes:

- suma de detalles del mismo usuario+wallet <= total de esa wallet;
- suma de wallets/perfiles del usuario <= 1;
- el mismo evento puede producir una orden por allocation aplicable;
- la cantidad de ordenes no multiplica el presupuesto asignado;
- si la politica selecciona un solo perfil por wallet, los otros perfiles no
  tienen detalle vigente y no pueden promocionar a LIVE.

## 9. Resolver

`LiveAllocationPercentageResolver` recibe:

- userId;
- walletId;
- strategyCode;
- scopeType;
- scopeValue;
- promotionTime.

Devuelve:

- percentage;
- strategyPercentage;
- walletTotalPercentage;
- source;
- sourceId/distributionId;
- calculatedAt;
- validUntil;
- reasonCode.

Reglas:

- usa solo el ultimo run COMPLETED del usuario;
- nunca retrocede a un run anterior si el ultimo no contiene el perfil;
- rechaza run vencido;
- exige identidad exacta;
- valida rango y sumas agregadas;
- no hace HTTP;
- puede releer PostgreSQL dentro de la transaccion corta para confirmar que el
  run sigue siendo el ultimo vigente.

Reason codes:

- `LIVE_ALLOCATION_PCT_RESOLVED`;
- `LIVE_ALLOCATION_PCT_MISSING`;
- `LIVE_ALLOCATION_PCT_INVALID`;
- `LIVE_ALLOCATION_PCT_STALE`;
- `LIVE_ALLOCATION_PCT_SCOPE_MISMATCH`;
- `LIVE_ALLOCATION_PCT_TOTAL_EXCEEDED`;
- `LIVE_DISTRIBUTION_NOT_AVAILABLE`;
- `LIVE_DISTRIBUTION_AMBIGUOUS`.

## 10. Promociones

### 10.1 SHADOW -> MICRO_LIVE

- No resuelve porcentaje LIVE.
- Crea allocation con porcentaje nulo y fuente de presupuesto fijo.
- Crea o conserva plan wallet-level con porcentaje nulo y capital 100, salvo que
  el mismo plan ya represente una distribucion LIVE de la wallet.

### 10.2 MICRO_LIVE -> LIVE

1. Evalua readiness, evidencia y FULL fuera de la transaccion.
2. Resuelve distribucion vigente fuera de la transaccion.
3. Si falla, guarda reason durable y mantiene MICRO_LIVE operativa.
4. Adquiere el advisory lock canonico del perfil.
5. En transaccion corta relee MICRO_LIVE, comprueba idempotencia y relee el
   snapshot PostgreSQL vigente.
6. Cierra MICRO, crea LIVE con porcentaje y metadata reales, actualiza el plan y
   escribe auditoria atomicamente.
7. Commit y log estructurado.

### 10.3 SHADOW -> LIVE directo

Usa exactamente el mismo resolver. Sin porcentaje vigente queda bloqueado. El
default productivo sigue exigiendo MICRO_LIVE.

## 11. Plan wallet-level

`user_wallet_copy_plan` es una vista operativa agregada, no la fuente por
estrategia.

- Solo MICRO_LIVE en la wallet: porcentaje nulo, capital 100 y FIXED_CAPITAL.
- Al menos una LIVE: porcentaje igual al total LIVE de la wallet, capital
  derivado y PERCENTAGE.
- En mezcla LIVE+MICRO, el plan describe la parte LIVE; MICRO sigue usando el
  presupuesto fijo independiente y no lee el plan para sizing.
- Una nueva promocion MICRO nunca degrada un plan PERCENTAGE existente.

## 12. Migracion legacy

La siguiente version disponible despues de `V202607110006` es
`V202607110007`.

La migracion debe:

1. permitir NULL en ambos campos `allocation_pct` necesarios;
2. agregar metadata de sizing y fuente;
3. crear tablas de snapshot;
4. convertir a NULL solo MICRO_LIVE con evidencia combinada segura;
5. convertir el plan relacionado solo si tiene shape sentinel+100 y no existe
   LIVE activo para esa wallet;
6. conservar porcentajes MICRO ambiguos como legacy ignorado;
7. pausar un LIVE activo con `0.000001` solo si la procedencia combinada prueba
   que heredo el sentinel MICRO; un porcentaje minimo trazado por una
   distribucion actual no se reclasifica;
8. agregar constraints de rango y contrato LIVE;
9. ser idempotente ante reejecucion logica y despliegue interrumpido.

## 13. Atomicidad, locks e idempotencia

- El calculo remoto y FULL permanecen fuera de la transaccion.
- El snapshot STAGED y sus detalles se insertan atomicamente; su finalizacion o
  fallo se persisten en una transaccion corta separada.
- La promocion usa el mismo `CopyDistributionUnitExecutor`, advisory lock y
  retry 40P01 ya usados por distribucion/SHADOW.
- La unicidad activa por usuario+wallet+strategy+scope sigue siendo defensa
  final.
- Un replay devuelve NOOP si LIVE ya existe.
- Un fallo al insertar LIVE revierte cierre MICRO, plan y auditoria del intento.

## 14. Observabilidad

Logs requeridos:

- `copy.allocation.percentage.resolved` para MICRO fija;
- `copy.promotion.live.percentage.resolved` para LIVE;
- `copy.promotion.live.rejected` con KEEP_MICRO_LIVE;
- `copy.allocation.legacy_sentinel.detected` para legacy.

Metricas de baja cardinalidad:

- `copy_allocation_percentage_resolution_total`;
- `copy_live_promotion_total`;
- `copy_legacy_allocation_sentinel_total`;
- `copy_live_distribution_percentage`.

No se usan IDs como tags.

## 15. Escenarios y pruebas RED

Se implementan los catorce escenarios solicitados:

1. SHADOW -> MICRO con NULL;
2. MICRO NULL elegible;
3. sizing MICRO independiente de nulo/sentinel/0.25;
4. MICRO -> LIVE usa 0.12 vigente;
5. cambio 0.15 -> 0.07;
6. missing mantiene MICRO;
7. invalidos y sentinel marcado bloquean;
8. directo SHADOW -> LIVE;
9. dos estrategias no exceden total wallet;
10. queries mode-aware;
11. legacy interpretable y migrable;
12. concurrencia una sola LIVE;
13. rollback sin estado parcial;
14. replay idempotente.

## 16. Criterios de aceptacion

- No hay escritura nueva del sentinel.
- MICRO funciona y se ejecuta con porcentaje nulo.
- LIVE nunca hereda porcentaje MICRO.
- Toda promocion LIVE usa un snapshot exacto, vigente y auditable.
- Dos estrategias respetan el total wallet.
- El plan no mezcla sentinel con capital fijo.
- REDUCE y CLOSE conservan su comportamiento.
- LIVE no hereda locks ni limites de presupuesto MICRO.
- Migracion, PostgreSQL 16, concurrencia, suite y package quedan verdes.
- No se ejecuta ninguna orden Binance durante validacion.

## 17. Rollback y riesgos

- Rollback de aplicacion: el constraint LIVE impide crear LIVE sin porcentaje
  valido aun si un binario anterior intenta copiar NULL.
- Rollback de datos: las filas migradas se identifican por metadata legacy y
  pueden restaurarse desde preflight, sin tocar porcentajes ambiguos.
- Riesgo residual: `ms-wallet-metric-etl` conserva un publicador wallet-level
  incompatible con multiples estrategias. Debe permanecer deshabilitado hasta
  una especificacion de integracion; no es la fuente elegida aqui.
- Riesgo residual: un fallo prolongado del job de distribucion vence snapshots y
  bloquea promociones, pero no bloquea REDUCE/CLOSE ni desactiva MICRO_LIVE.
