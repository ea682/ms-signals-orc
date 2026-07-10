# Runbook de reconciliacion de ordenes copy trading

## 1. Principio

Un timeout, 5xx, conexion cortada o ack incompleto no prueba que Binance no haya
recibido la orden. El operador debe consultar y reconciliar; nunca reenviar a
ciegas. `MANUAL_REVIEW` no significa que no hubo fill.

Prioridad:

```text
preservar efecto economico
-> impedir segundo send
-> reconstruir operation/event/progress
-> resolver precio
-> liberar solo cuando no-fill sea definitivo
```

## 2. Accion inmediata

1. Poner `COPY_NEW_DISPATCH_ENABLED=false` y reiniciar de forma controlada.
2. Mantener `COPY_RECONCILIATION_ENABLED=true`.
3. No borrar, reabrir, cancelar ni cerrar automaticamente.
4. No liberar reservation `PENDING` de un ambiguo.
5. Capturar hora, servicio, replica, traceId e intent id.
6. Verificar salud de DB, Binance engine, pools y workers.

Si se sospecha duplicado real, detener tambien la expansion canary. No ejecutar
panic-close: puede multiplicar la perdida o cerrar la posicion equivocada.

## 3. Clasificacion

| Estado | Significado | Accion |
|---|---|---|
| DISPATCHING stale | crash antes/durante HTTP | lookup, no resend |
| RECONCILING | outcome desconocido | worker consulta Binance |
| NEW | orden aceptada sin fill | deferir y consultar |
| PARTIALLY_FILLED | fill acumulativo parcial | aplicar solo delta |
| FILLED | exchange confirma fill | completar ledger/operation |
| PERSISTENCE_PENDING | exchange conocido, DB local incompleta | backfill idempotente |
| PERSISTED + price pending | efecto local existe, precio provisional | lookup trades/price |
| REJECTED + RELEASED | no fill definitivo | no resend automatico |
| MANUAL_REVIEW + PENDING | resultado/exposure ambiguo | revision humana |
| MANUAL_REVIEW + CONFIRMED | efecto existe, precio/revision pendiente | no reservar doble |

## 4. Orden de lookup

1. Si existe `binance_order_id`, consultar por orderId.
2. Si no aparece o no existe id, consultar por clientOrderId estable.
3. Nunca llamar `newOrder`, `/position` o close endpoint desde el worker.
4. Un 404 temporal no prueba ausencia; aplicar backoff.
5. Al agotar intentos, `MANUAL_REVIEW`; no cambiar a REJECTED por inferencia.

## 5. Semantica de resultados

| Resultado Binance | Tratamiento |
|---|---|
| FILLED + orderId + qty, avgPrice null | efecto valido; price pending |
| FILLED con cumQuote/qty | derivar precio si ambos son positivos |
| PARTIALLY_FILLED | delta=max(0,cumulative-persisted) |
| mismo partial | NOOP |
| cumulative menor | NOOP y alerta de ordering |
| CANCELED/EXPIRED/REJECTED con qty>0 | conservar fill; no release ciego |
| CANCELED/EXPIRED/REJECTED con qty=0 confirmado | REJECTED/RELEASED |
| NEW | mantener reserva y reconsultar |
| response sin orderId | ambiguo por clientOrderId |

## 6. Inspeccion read-only

Usar el script completo:

```powershell
psql -X -v ON_ERROR_STOP=1 -f src/main/resources/db/validation/copy_dispatch_performance_validation.sql
```

Consultas focalizadas:

```sql
SELECT id, idempotency_key, execution_mode, status, reservation_status,
       client_order_id, binance_order_id, executed_qty,
       persisted_executed_qty, average_price_status,
       copy_operation_id, copy_operation_event_id,
       reconciliation_attempts, next_reconciliation_at,
       last_error_code, updated_at
FROM futuros_operaciones.copy_dispatch_intent
WHERE id = :intent_id;
```

```sql
SELECT id_event, dispatch_intent_id, event_type, qty_executed,
       resulting_qty, price, price_status, event_time
FROM futuros_operaciones.copy_operation_event
WHERE dispatch_intent_id = :intent_id
ORDER BY event_time, id_event;
```

```sql
SELECT id_operation, dispatch_intent_id, id_orden, client_order_id,
       is_active, size_par, size_usd, leverage, price_entry, price_close
FROM futuros_operaciones.copy_operation
WHERE dispatch_intent_id = :intent_id;
```

No pegar secrets, signatures ni headers en tickets.

## 7. Dry-run de incidente

Existe un script adicional:

```text
scripts/reconcile-copy-dispatch-incident.sql
```

Su default es dry-run. El modo apply exige la frase exacta documentada dentro
del script y solo corrige DB; no envia/cancela ordenes. Antes de cualquier apply:

- CSV anonimizado y unico por order/clientOrderId;
- mapping exacto a una allocation y source identity;
- cero filas ambiguas o duplicadas;
- peer review de dos personas;
- backup y ventana autorizada;
- ejecutar primero dry-run y conservar output.

CSV duplicado o mapping ambiguo siempre va a revision manual.

## 8. Recuperacion por tipo

### Intent durable sin send probado

No reenviar. El worker consulta Binance. Solo un rechazo/no-fill definitivo
permite release; una ausencia temporal mantiene PENDING.

### Fill sin copy_operation

Crear/upsert idempotentemente usando dispatch intent y cumulative executed qty.
Registrar required event, enlazar ambos IDs y luego marcar PERSISTED.

### Operation existe, event falta

Registrar required event idempotente por intent+eventType+progress, enlazarlo y
completar PERSISTED. No volver a aplicar qty a la operation.

### Event existe, operation falta

Reconstruir operation desde intent+ack; el event existente se reutiliza. Validar
resulting qty antes de PERSISTED.

### Price pending

Consultar order/trades. Si no se resuelve al maximo, conservar reference price,
reservation CONFIRMED y pasar a MANUAL_REVIEW. El fill no desaparece.

### Partial

Aplicar solo `max(0, executed_qty - persisted_executed_qty)`. Conservar la
reserva residual hasta terminal. Nunca aplicar el cumulative completo otra vez.

## 9. Validaciones antes de cerrar incidente

- Un intent por idempotency key.
- Un Binance order por user+orderId.
- Un active copy por allocation+origin+type.
- Cero PERSISTED sin operation/event links.
- `event.dispatch_intent_id = intent.id`.
- persisted qty no excede observed qty.
- Ambiguos conservan PENDING.
- Rechazados sin fill estan RELEASED.
- Cero segundo send en logs/gateway.
- Reconciliation backlog y oldest age vuelven a nivel estable.

## 10. Escalamiento

Escalar inmediatamente si:

- dos orderIds para una misma intencion;
- active+reserved supera budget;
- qty aplicada supera cumulative Binance;
- terminal fue sobrescrito;
- manual review perdio reserva;
- API key/subcuenta no corresponde;
- worker llama un endpoint de send;
- no se puede demostrar source+allocation exacta.

Mantener el sistema con nuevos dispatches apagados hasta RCA, test rojo, fix,
suite completa, SQL y aprobacion del rollout.
