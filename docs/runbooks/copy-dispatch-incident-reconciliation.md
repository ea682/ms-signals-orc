# Runbook: reconciliacion de ordenes MICRO_LIVE/LIVE

## Alcance y prohibiciones

Este procedimiento reconcilia ordenes reales que existen en Binance pero no en
`copy_operation`. No envia, cancela ni cierra ordenes. Tampoco crea un intent
sintetico cuando no existe informacion suficiente para asignarlo sin duda a
`wallet + user_copy_allocation_id + strategy/scope + source_event`.

El modo predeterminado es DRY RUN. `apply=1` solo completa intents durables ya
existentes y despierta al worker normal de reconciliacion. El worker vuelve a
consultar Binance y persiste de forma idempotente; nunca reenvia la orden.

## Preparacion segura

1. Bloquear solamente nuevas entradas MICRO_LIVE/LIVE mediante copy guard o
   `allowNewEntries=false`. Mantener workers y reductions/closes habilitados.
2. Guardar un respaldo de Postgres y registrar el rango UTC del incidente.
3. Exportar desde Binance todas las ordenes del rango, no solo las tres vistas
   inicialmente: BTCUSDC, ETHUSDC, SOLUSDC y cualquier otro simbolo detectado.
4. No incluir API key, secret, firma ni credenciales en el CSV.
5. Desplegar primero `ms-binance-engine`, ejecutar Flyway y luego desplegar
   `ms-signals-orc` con `COPY_RECONCILIATION_ENABLED=true`.

Formato CSV obligatorio:

```csv
user_id,user_copy_allocation_id,source_event_id,client_order_id,symbol,order_id,status,executed_qty,avg_price,cum_quote,leverage,order_time
user-uuid,505,source-event,cpO_xxx,BTCUSDC,123,FILLED,0.001,,65.25,5,2026-07-09T00:00:00Z
```

`avg_price` puede estar vacio. `cum_quote / executed_qty` se usa como precio
derivado. `source_event_id` y `user_copy_allocation_id` pueden quedar vacios en
un export legado, pero esas filas no se aplican automaticamente si no existe un
intent local exacto.

Antes de Flyway, verificar que la nueva unicidad activa no encuentre duplicados:

```sql
SELECT user_copy_allocation_id, id_order_origin, type_operation, COUNT(*)
FROM futuros_operaciones.copy_operation
WHERE is_active = true AND user_copy_allocation_id IS NOT NULL
GROUP BY user_copy_allocation_id, id_order_origin, type_operation
HAVING COUNT(*) > 1;

SELECT id_order_origin, id_user, type_operation,
       COALESCE(copy_strategy_code, 'MOVEMENT_ALL'), COUNT(*)
FROM futuros_operaciones.copy_operation
WHERE is_active = true AND user_copy_allocation_id IS NULL
GROUP BY id_order_origin, id_user, type_operation,
         COALESCE(copy_strategy_code, 'MOVEMENT_ALL')
HAVING COUNT(*) > 1;
```

No borrar ni fusionar esas filas automaticamente. Si aparecen resultados, detener
el despliegue y reconciliar cada posicion contra Binance. El indice sobre la tabla
existente requiere un scan y puede tomar lock proporcional a su tamano; usar una
ventana de mantenimiento cuando `copy_operation` sea grande.

## Ejecucion DRY RUN

```powershell
psql $env:DB_URL `
  -v binance_csv="'C:/secure/binance-orders.csv'" `
  -f scripts/reconcile-copy-dispatch-incident.sql
```

El reporte entrega:

- ordenes Binance sin `copy_operation`;
- repeticiones por usuario/allocation/source event/clientOrderId/simbolo;
- cantidad, notional, margen estimado y orderIds;
- intents existentes reparables;
- ordenes legadas sin intent que requieren mapeo manual;
- todos los simbolos, incluidos BTCUSDC, ETHUSDC y SOLUSDC cuando existan.

## Apply protegido

Revisar y archivar el DRY RUN antes de aplicar. El modo apply no crea posiciones,
no modifica Binance y no inventa allocation/strategy/scope.

```powershell
psql $env:DB_URL `
  -v binance_csv="'C:/secure/binance-orders.csv'" `
  -v apply=1 `
  -v confirm_apply="'I_UNDERSTAND_DB_BACKFILL_ONLY'" `
  -f scripts/reconcile-copy-dispatch-incident.sql
```

El update se limita por `id_user + client_order_id + allocation` y rechaza un
`binance_order_id` contradictorio. Ademas exige `allocation + source_event +
symbol`, exactamente una orden Binance en ese grupo y exactamente un intent local
coincidente. Un grupo con varios `orderId`, allocation/source ausente o mapping
ambiguo queda fuera de apply para revision humana; nunca se selecciona una fila
arbitraria.

El intent seguro queda `RECONCILING` con reserva `PENDING`; luego
`CopyOrderReconciliationWorker` confirma en Binance y hace el backfill. Una orden
sin intent queda fuera de apply para revision humana.

## Consultas de validacion

### Intents por source event

```sql
SELECT source_event_id, id_user, user_copy_allocation_id, strategy_code,
       scope_type, scope_value, copy_intent, status, client_order_id,
       binance_order_id, copy_operation_id
FROM futuros_operaciones.copy_dispatch_intent
WHERE source_event_id = :source_event_id
ORDER BY id_user, user_copy_allocation_id, copy_intent, created_at;
```

### Duplicados de la intencion exacta

```sql
SELECT idempotency_key, COUNT(*) AS intents, ARRAY_AGG(id) AS intent_ids
FROM futuros_operaciones.copy_dispatch_intent
GROUP BY idempotency_key
HAVING COUNT(*) > 1;
```

### Binance orderId duplicado localmente

```sql
SELECT id_user, binance_order_id, COUNT(*) AS intents, ARRAY_AGG(id) AS intent_ids
FROM futuros_operaciones.copy_dispatch_intent
WHERE binance_order_id IS NOT NULL
GROUP BY id_user, binance_order_id
HAVING COUNT(*) > 1;
```

### clientOrderId asociado a varios orderId

```sql
SELECT id_user, client_order_id, COUNT(DISTINCT binance_order_id) AS order_ids,
       ARRAY_AGG(DISTINCT binance_order_id) AS values
FROM futuros_operaciones.copy_dispatch_intent
WHERE binance_order_id IS NOT NULL
GROUP BY id_user, client_order_id
HAVING COUNT(DISTINCT binance_order_id) > 1;
```

### Operaciones reales sin intent

```sql
SELECT id_operation, id_user, user_copy_allocation_id, execution_mode,
       id_orden, client_order_id, id_order_origin, parsymbol, date_creation
FROM futuros_operaciones.copy_operation
WHERE COALESCE(is_shadow, false) = false
  AND execution_mode IN ('MICRO_LIVE', 'LIVE')
  AND dispatch_intent_id IS NULL
ORDER BY date_creation;
```

### Intents ejecutados sin copy_operation

```sql
SELECT id, id_user, user_copy_allocation_id, execution_mode, status,
       source_event_id, client_order_id, binance_order_id, executed_qty,
       average_price_status, reconciliation_attempts, next_reconciliation_at
FROM futuros_operaciones.copy_dispatch_intent
WHERE copy_operation_id IS NULL
  AND status IN ('FILLED', 'PARTIALLY_FILLED', 'RECONCILING',
                 'PERSISTENCE_PENDING', 'FAILED_FINAL')
ORDER BY updated_at;
```

### Reservas pendientes

```sql
SELECT id_user, user_copy_allocation_id, execution_mode,
       SUM(requested_margin_usd) AS reserved_margin_usd,
       SUM(reserved_position_count) AS reserved_positions,
       COUNT(*) AS intents
FROM futuros_operaciones.copy_dispatch_intent
WHERE reservation_status = 'PENDING'
GROUP BY id_user, user_copy_allocation_id, execution_mode
ORDER BY execution_mode, reserved_margin_usd DESC;
```

### Margen MICRO_LIVE usado y reservado

```sql
WITH used AS (
  SELECT id_user, user_copy_allocation_id,
         SUM(size_usd / NULLIF(leverage, 0)) AS used_margin_usd,
         COUNT(*) AS open_positions
  FROM futuros_operaciones.copy_operation
  WHERE execution_mode = 'MICRO_LIVE' AND is_active = true
    AND COALESCE(is_shadow, false) = false
  GROUP BY id_user, user_copy_allocation_id
), reserved AS (
  SELECT id_user, user_copy_allocation_id,
         SUM(requested_margin_usd) AS reserved_margin_usd,
         SUM(reserved_position_count) AS reserved_positions
  FROM futuros_operaciones.copy_dispatch_intent
  WHERE execution_mode = 'MICRO_LIVE' AND reservation_status = 'PENDING'
  GROUP BY id_user, user_copy_allocation_id
)
SELECT COALESCE(u.id_user, r.id_user) AS id_user,
       COALESCE(u.user_copy_allocation_id, r.user_copy_allocation_id) AS allocation_id,
       COALESCE(u.used_margin_usd, 0) AS used_margin_usd,
       COALESCE(r.reserved_margin_usd, 0) AS reserved_margin_usd,
       COALESCE(u.used_margin_usd, 0) + COALESCE(r.reserved_margin_usd, 0) AS total_margin_usd,
       COALESCE(u.open_positions, 0) AS open_positions,
       COALESCE(r.reserved_positions, 0) AS reserved_positions
FROM used u FULL JOIN reserved r USING (id_user, user_copy_allocation_id)
ORDER BY total_margin_usd DESC;
```

### MICRO_LIVE sobre 100 USDC o sobre el limite opcional del usuario

Usar la consulta anterior y filtrar:

```sql
WHERE total_margin_usd > 100
```

La consulta base no determina por si sola una violacion de cantidad de
posiciones. Para esa validacion, unir por `user_copy_allocation_id` con
`user_copy_allocation` y reportar solo cuando
`user_max_concurrent_positions IS NOT NULL` y el total abierto+reservado supera
ese valor. Una orden sobre 20 USDC o mas de cinco posiciones no son violaciones
V3 por si mismas.

### Reconciliaciones pendientes, incluidas LIVE

```sql
SELECT execution_mode, status, COUNT(*) AS intents,
       MIN(updated_at) AS oldest, MAX(reconciliation_attempts) AS max_attempts
FROM futuros_operaciones.copy_dispatch_intent
WHERE status IN ('DISPATCHING', 'RECONCILING', 'PERSISTENCE_PENDING',
                 'NEW', 'PARTIALLY_FILLED', 'FILLED', 'FAILED_FINAL')
GROUP BY execution_mode, status
ORDER BY execution_mode, status;
```

## Canary post-fix

1. Confirmar migracion e indices con Flyway.
2. Confirmar que no hay `FAILED_FINAL` sin ticket manual.
3. Habilitar una sola allocation MICRO_LIVE con capital minimo.
4. Observar un evento y comprobar un intent, una llamada Binance, un orderId,
   una `copy_operation` y su auditoria.
5. Inyectar mediante mock una respuesta FILLED con `avgPrice=null`; debe quedar
   `PENDING_RESOLUTION` sin segunda llamada.
6. Repetir LIVE solo en testnet/mock. No usar produccion para probar fallos.
7. Habilitar gradualmente y alertar por `copy.dispatch.ambiguous`,
   `copy.dispatch.duplicate`, `copy.reconciliation.failed` y reservas antiguas.

## Rollback

Volver binarios manteniendo tabla y columnas aditivas. No borrar
`copy_dispatch_intent` mientras existan estados no terminales. Si el worker se
detiene, las reservas quedan fail-closed y deben revisarse; nunca liberar ni
cerrar posiciones automaticamente para hacer cuadrar el estado local.
