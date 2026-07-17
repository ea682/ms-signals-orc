# Runbook de rollout MICRO_LIVE y LIVE

Estado inicial obligatorio: `AUDIT_ONLY`

## 1. Objetivo

Activar el flujo real gradualmente sin borrar intents, liberar ambiguos ni
desactivar la reconciliacion. Este runbook no autoriza una orden; define las
condiciones que un operador autorizado debe cumplir en staging/canary.

## 2. Precondiciones bloqueantes

- Suites completas y packages de ambos servicios verdes en el mismo commit.
- `git diff --check` limpio, cero `.rej`, cero Flyway versions duplicadas.
- `application.yml` parseado sin duplicate keys.
- Migration `V202607100002__copy_dispatch_manual_review_integrity.sql` aplicada.
- Constraint de status incluye `MANUAL_REVIEW`.
- Indices `ix_copy_dispatch_intent_manual_review` y
  `ux_copy_operation_event_legacy_client_order_id` presentes.
- SQL `copy_dispatch_performance_validation.sql` retorna cero violaciones.
- Reconciliador habilitado y dashboard operativo.
- Snapshot de users, allocations, metrics y active copies primado y no stale.
- API key/subcuenta canary verificada por canal seguro, nunca por logs.
- Leverage/margin account+symbol preconfigurado fuera del hot path.
- Fault injection y DB concurrency criticos cerrados.

Si falla una precondicion, no activar.

## 3. Kill switches

| Variable | Default | Efecto |
|---|---:|---|
| `COPY_NEW_DISPATCH_ENABLED` | false | master de todo primer send real, incluso reduce/close |
| `COPY_MICRO_LIVE_ENABLED` | false | habilita MICRO_LIVE si master esta on |
| `COPY_LIVE_ENABLED` | false | habilita modo LIVE si master esta on |
| `COPY_LIVE_DRY_RUN` | true | bloquea todos los sends LIVE |
| `COPY_LIVE_CANARY_ENABLED` | false | habilita entradas LIVE solo con whitelist |
| `COPY_RECONCILIATION_ENABLED` | true | mantiene recovery de intents existentes |
| `BINANCE_ORDER_SUBMIT_ENABLED` | false | ultima barrera en Binance engine |
| `BINANCE_LIVE_SAFE_VALIDATION_MODE` | true | mantiene validacion defensiva |

Las propiedades se inyectan al inicio; asumir restart requerido. Un sistema de
config dinamica solo puede cambiar esta regla si se demuestra y documenta.

Al detener nuevos dispatches, no apagar reconciliacion.

## 4. Orden de despliegue

1. Backup/verificacion operacional normal de PostgreSQL.
2. Ejecutar migration mediante el pipeline Flyway aprobado.
3. Verificar constraints e indices con SQL read-only.
4. Desplegar `ms-binance-engine` con submit false.
5. Desplegar `ms-signals-orc` con todos los dispatch switches false.
6. Confirmar health, pools, caches y workers.
7. Observar 30 minutos en audit-only.
8. Ejecutar carga local/staging sin Binance real.
9. Solo entonces evaluar fase MICRO_LIVE.

## 5. Fase 1: audit-only

Configuracion:

```text
COPY_NEW_DISPATCH_ENABLED=false
COPY_MICRO_LIVE_ENABLED=false
COPY_LIVE_ENABLED=false
COPY_LIVE_DRY_RUN=true
COPY_LIVE_CANARY_ENABLED=false
COPY_RECONCILIATION_ENABLED=true
BINANCE_ORDER_SUBMIT_ENABLED=false
```

Validar durante al menos una ventana representativa:

- `pending_intents`, `ambiguous_intents`, `persistence_pending_intents`.
- `manual_review_intents`, `oldest_pending_age`, backlog.
- cache misses/stale de users, allocations y metrics.
- candidate/sizing/pre-network timers.
- pool DB, CPU, heap, GC y executor rejection.
- cero local fake fill para allocation real bloqueada.

## 6. Fase 2: MICRO_LIVE canary

Solo despues de obtener `MICRO_LIVE_CANARY_READY` en el reporte actualizado.

Politica economica esperada:

```text
COPY_MICRO_LIVE_TOTAL_CAPITAL_USD=100
COPY_MICRO_LIVE_TARGET_LEVERAGE=5
fixedPerOperation=absent
globalMaxPositions=absent
```

`userMaxConcurrentPositions` solo se aplica cuando el usuario lo configuro.
El portafolio completo se dimensiona proporcionalmente y comparte los 100 USDC
por `user + wallet`, aun cuando existan varias estrategias.

Secuencia de activacion autorizada:

1. Una subcuenta y pocas allocations exactas.
2. `BINANCE_ORDER_SUBMIT_ENABLED=true` solo en la instancia canary.
3. `COPY_NEW_DISPATCH_ENABLED=true`.
4. `COPY_MICRO_LIVE_ENABLED=true`.
5. LIVE permanece false/dry-run.

Abortar ante cualquiera:

- duplicate economic effect;
- active+reserved > 100 por user+wallet;
- posiciones sobre `userMaxConcurrentPositions` cuando ese limite existe;
- sizing fijo por orden o seleccion dependiente del orden de eventos;
- intent sin operation/event link;
- replay que genera segundo send;
- ambiguity sin reserva;
- backlog o oldest age creciente;
- error rate, p99 o pool wait fuera del presupuesto;
- mismatch leverage/margin account+symbol.

## 7. Fase 3: LIVE canary

Requiere una fase MICRO_LIVE estable y `LIVE_CANARY_READY`.

Configuracion minima:

```text
COPY_LIVE_ENABLED=true
COPY_LIVE_DRY_RUN=false
COPY_LIVE_CANARY_ENABLED=true
COPY_LIVE_WHITELIST_ALLOCATION_IDS=<one-approved-allocation>
```

La whitelist debe contener al menos un criterio. Preferir allocation exacta y,
si corresponde, user+wallet+strategy como defensa adicional.

Verificar antes de cada entrada:

- exposure source valida y no stale;
- budget LIVE no usa fixed 20/100/5;
- symbol target correcto;
- leverage confirmado para la cuenta+symbol;
- snapshot runtime vigente;
- zero pending/ambiguous inesperado para la allocation.

Reductions LIVE no requieren whitelist de entrada, pero si master, LIVE enabled
y dry-run false. Esto permite salir de una estrategia no elegible sin abrir mas
riesgo. El master apagado bloquea todo primer send y se reserva para emergencia.

## 8. Promocion gradual

Expandir solo una dimension a la vez:

1. Mas eventos en la misma allocation.
2. Segunda allocation en la misma subcuenta.
3. Segunda subcuenta.
4. LIVE con mas symbols.
5. Replica adicional.

Mantener una fase al menos durante el volumen necesario para observar OPEN,
INCREASE, REDUCE, CLOSE, FLIP, partial, timeout y reconciliacion.

## 9. Verificacion SQL read-only

Ejecutar con `default_transaction_read_only=on`:

```powershell
psql -X -v ON_ERROR_STOP=1 -f src/main/resources/db/validation/copy_dispatch_performance_validation.sql
```

No usar una URL JDBC directamente sin convertirla al formato aceptado por
`psql`. No imprimir password ni connection string en logs del pipeline.

Resultados bloqueantes:

- duplicate idempotency key/order identity;
- PERSISTED sin links/progress;
- legacy duplicate clientOrderId;
- budget/position por encima del limite;
- ambiguo con reservation RELEASED;
- intents antiguos sin razon operativa.

## 10. Rollback

Rollback funcional, en este orden:

1. `COPY_NEW_DISPATCH_ENABLED=false` y restart controlado.
2. Confirmar que no se autoriza un nuevo send.
3. Mantener `COPY_RECONCILIATION_ENABLED=true`.
4. Mantener servicios y observabilidad activos.
5. No borrar intents/operations/events.
6. No liberar reservas ambiguas.
7. Clasificar backlog con el runbook de reconciliacion.
8. Si Binance engine debe aislarse, apagar submit despues de detener nuevos
   dispatches; conservar lookup para recovery si la arquitectura lo permite.

No revertir migration aditiva durante un incidente. Una version anterior puede
ignorar columnas/estado nuevos, pero los datos deben conservarse.

## 11. Evidencia de salida

Guardar por fase:

- commits/images desplegadas;
- variables no sensibles;
- migration version y SQL validation;
- p50/p75/p90/p95/p99/max de cada timer;
- throughput y error rate;
- CPU/heap/GC/threads/pools/locks;
- counts de intents/reservas/reconciliacion;
- incidentes y decisiones de rollback.

Sin esta evidencia no se cambia el dictamen del reporte.
