# Reporte de indices PostgreSQL para copy trading

Fecha: 2026-07-10
Metodo: catalogo/stats QA read-only; ningun indice fue creado, eliminado o
reindexado.

## 1. Resumen

| Control | Resultado |
|---|---:|
| Indices del schema | 742 |
| Indices invalidos | 0 |
| Indices `indisready=false` | 0 |
| Duplicados exactos detectados | 4 pares |
| Indices de `copy_dispatch_intent` | 8 |
| Indices de `copy_operation` | 23 |
| Indices de `copy_operation_event` | 23 |
| Indices de `user_copy_allocation` | 17 |

Las tablas LIVE vacias ya cargan 54 indices. Esto protege multiples contratos,
pero el costo real de write amplification solo puede medirse con inserts/updates
locales y canary.

## 2. Indices del dispatch intent

| Indice | Uso esperado | Estado |
|---|---|---|
| `copy_dispatch_intent_pkey(id)` | PK/recovery | valid, ready |
| `ux_copy_dispatch_intent_idempotency(idempotency_key)` | claim/replay | valid, unique; usado por plan |
| `ix_copy_dispatch_intent_client_order(client_order_id)` | fallback de persistencia | valid; usado por plan |
| `ux_copy_dispatch_intent_user_binance_order(id_user, binance_order_id)` | correlacion user+order | valid, partial, unique |
| `ix_copy_dispatch_intent_allocation_budget(user, allocation, mode, reservation)` | pending budget | valid; usado por aggregate |
| `ix_copy_dispatch_intent_reconciliation(status, next, updated)` | worker/recovery | valid; no demostrable con tabla vacia |
| `ix_copy_dispatch_intent_manual_review(updated_at, id)` | cola humana | valid; index-only scan |
| `ix_copy_dispatch_intent_source_event(source_event_id, allocation)` | diagnostico/dedupe source | valid |

El lookup solo por `binance_order_id` puede recorrer el indice compuesto sin
una condicion sobre su primera columna. No existe una query productiva de
repository con ese filtro: el reconciliador consulta Binance por
`clientOrderId`. No se propone otro indice sin confirmar ese nuevo contrato.

## 3. Indices operation/event

- `ux_copy_operation_dispatch_intent` y
  `ix_copy_operation_event_dispatch_intent` existen, son validos y coinciden con
  recovery por intent.
- `ux_copy_operation_event_dispatch_progress` coincide con
  `(dispatch_intent_id,event_type,coalesce(qty),coalesce(resulting_qty))`.
- `ux_copy_operation_event_legacy_client_order_id` preserva idempotencia legacy
  solo cuando `dispatch_intent_id IS NULL`.
- `ux_copy_operation_allocation_origin_type_active` separa allocations y evita
  colision entre estrategias de una misma wallet.
- Los planes QA eligen seq scan en operation/event porque ambos heaps tienen 0
  rows. Esto no invalida los indices ni demuestra su plan a escala.

Hay solapamientos semanticos, no exactos, entre varios indices de
`copy_operation` por `origin/user/type/strategy` y entre indices de event por
`user/time`. No se debe eliminar ninguno antes de capturar workload real y
comparar las consultas JPA generadas.

## 4. Duplicados exactos

| Tabla | Indices equivalentes | Accion recomendada |
|---|---|---|
| `user_copy_allocation` | `idx_user_copy_allocation_user_wallet_policy_active`, `ix_user_copy_allocation_user_active_wallets` | elegir uno en una migration futura, despues de revisar dependencias |
| `shadow_copy_allocation` | `ux_shadow_copy_allocation_active_strategy`, `ux_shadow_copy_allocation_profile_key` | confirmar que ambos representan la misma constraint de negocio |
| `wallet_trade_fact` | `idx_wallet_trade_fact_wallet_closed_trade`, `ix_wtf_wallet_closed_open_trade` | consolidar solo con workload real |
| `wallet_trade_fact` | `ix_wallet_trade_fact_movement_hash`, `ix_wtf_movement_hash_not_null` | consolidar solo con workload real |

`idx_scan=0` no fue usado como criterio de drop. El uptime era menor a 12 dias y
varias funcionalidades LIVE aun no producen rows.

## 5. Indices faltantes demostrados

### 5.1 Resolucion de usuario

`UserDetailServiceImpl.findAllActive()` ejecuta una query root y dos lookups por
usuario. QA confirma:

- `detail_user` no tiene indice por `id_users`;
- `user_api_keys` no tiene ningun indice ni constraint visible, incluido
  `user_id` e `id_user_api_keys`;
- ambos lookups hacen seq scan.

Antes de agregar DDL se debe validar la cardinalidad esperada:

1. Si `detail_user` es uno-a-uno, proponer unique `(id_users)`.
2. Si hay una sola credencial activa por usuario/exchange, proponer una unique
   acorde a ese contrato; de lo contrario un indice no unique `(user_id)`.
3. Preferir ademas una query projection/fetch-join para eliminar el N+1.

Con un usuario QA el costo es pequeno; a N usuarios el patron actual puede
degradar a `1 + 2N` round trips y scans.

### 5.2 Latest movement por position

El codigo/migration versionado espera orden por:

```text
position_key, event_time DESC, date_creation DESC
```

QA posee `ix_ome_position_time(position_key,event_time DESC)` sin
`date_creation`. Es drift entre schema y repositorio y obliga a
`Incremental Sort`. Cada lookup sin fecha planifica 38 child indexes; con upper
bound removio 10 y mantuvo 28.

No basta con agregar la tercera columna: el costo principal observado es el
fan-out de particiones. Las opciones deben probarse con dataset local:

- restaurar la definicion versionada completa;
- mantener una tabla de current position state con una fila por `position_key`;
- acotar particiones con una ventana correcta y fallback explicito de cambio de
  mes;
- conservar un indice global equivalente mediante una tabla de lookup, ya que
  PostgreSQL no ofrece un indice global btree sobre particiones.

## 6. Reconciliation

El query real combina dos ramas con `OR` y ordena por
`coalesce(next_reconciliation_at,updated_at),created_at,id`. Con 0 intents el
planner uso seq scan + sort, por lo que no hay evidencia para declarar ausente
un indice.

En un fixture de escala se deben comparar, sin aplicar aun en QA:

- el indice actual;
- dos partial indexes separados para stale `DISPATCHING` y estados
  reconciliables;
- un orden que no requiera expresion `coalesce`, si el contrato lo permite.

## 7. Indices grandes y write cost

| Tabla | Indices | Tamano medido |
|---|---:|---:|
| `operation_movement_event_2026_06` | 16 | 7.383 MB |
| `operation_movement_event_2026_07` | 16 | 1.848 MB |
| `operation_movement_event_dedupe` | 1 | 1.259 MB |

Cada particion poblada mantiene tres variantes por `movement_key` y una variante
por `position_key`. En julio, el indice `movement_key,source` presenta trafico
alto y los otros dos cero scans en la ventana observada, pero no se recomienda
drop automatico: faltan `pg_stat_statements`, ciclo completo de features y una
ventana mayor desde restart.

## 8. Prioridad

| Prioridad | Accion | Tipo |
|---|---|---|
| P0 | Resolver drift/fan-out de latest position con benchmark local | performance hot path |
| P1 | Eliminar N+1 y definir constraints/indices de usuario/API | escalabilidad e integridad |
| P1 | Cargar cardinalidad realista de intents y probar reconciliation | readiness |
| P2 | Consolidar duplicados exactos mediante migration revisada | write cost |
| P2 | Auditar solapamientos de movement indexes con workload real | capacidad |
