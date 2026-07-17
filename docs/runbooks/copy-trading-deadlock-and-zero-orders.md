# Runbook: deadlocks y cero ordenes Copy Trading

## Alcance

Procedimiento para SHADOW, MICRO_LIVE y LIVE. No ejecutar ordenes reales durante diagnostico. QA se consulta solo read-only.

## 1. Predeploy

1. Mantener `COPY_NEW_DISPATCH_ENABLED=false`, `COPY_LIVE_ENABLED=false` y `COPY_LIVE_DRY_RUN=true`.
2. Confirmar que la politica economica MICRO_LIVE V3 sea exactamente:

```text
COPY_MICRO_LIVE_TOTAL_CAPITAL_USD=100
COPY_MICRO_LIVE_TARGET_LEVERAGE=5
fixedPerOperation=absent
globalMaxPositions=absent
```

Un `userMaxConcurrentPositions` nullable puede limitar una allocation cuando el
usuario lo configura. LIVE no usa el presupuesto MICRO_LIVE.

3. Verificar presupuesto de conexiones: replicas de signals x Hikari max + otros servicios < `max_connections` con reserva operativa.
4. Tomar backup/logical snapshot segun politica de plataforma.
5. Ejecutar tests con PostgreSQL 16 en CI; no aprobar canary si la suite Testcontainers aparece omitida.

## 2. Migraciones

Aplicar en orden:

```text
V202607110001__shadow_event_dead_letter.sql
V202607110002__micro_live_wallet_budget.sql
```

La segunda usa `CREATE INDEX CONCURRENTLY` y `executeInTransaction=false`. Programarla fuera de maxima carga. No editar una migracion aplicada.

Si `202607110002` falla antes de quedar registrada como exitosa, cada sentencia
anterior puede haber quedado confirmada. La migracion elimina de forma
concurrente sus dos indices nuevos y los recrea, por lo que se puede reintentar
despues de diagnosticar el lock. No ejecutar `flyway repair`: una fila exitosa
no existe todavia y repair podria ocultar un indice ausente o invalido.

Para PostgreSQL, mantener
`FLYWAY_POSTGRESQL_TRANSACTIONAL_LOCK=false`. El valor usa el advisory lock de
sesion de Flyway y conserva exclusion mutua entre replicas. Con `true`, Flyway
puede dejar su conexion de schema history `idle in transaction`; la conexion
DDL queda esperando su `virtualxid` y `CREATE INDEX CONCURRENTLY` termina por
`lock_timeout`, aunque no exista un bloqueador externo.

Validar historial e indices:

```sql
SELECT version, description, success, installed_on
FROM futuros_operaciones.flyway_schema_history
WHERE version IN ('202607110001', '202607110002')
ORDER BY installed_rank;

SELECT n.nspname, c.relname, i.indisvalid, i.indisready
FROM pg_index i
JOIN pg_class c ON c.oid = i.indexrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'futuros_operaciones'
  AND c.relname IN ('ix_copy_dispatch_intent_micro_wallet_budget',
                    'ix_copy_operation_micro_wallet_budget');
```

Ambos indices deben existir con `indisvalid=true` e `indisready=true`. Luego ejecutar el script read-only `db/validation/micro_live_wallet_budget_validation.sql`; debe devolver cero violaciones. El script tambien reporta un indice ausente.

Si un indice concurrente queda invalido y `202607110002` sigue pendiente, no
ejecutar `flyway repair` ni eliminarlo manualmente: desplegar la migracion
corregida y reintentar en ventana controlada. Si Flyway ya muestra la version
como exitosa, no editarla; crear una migracion forward que repare el indice y
validar el historial antes de continuar.

## 3. Diagnostico de cero ordenes

Consultar:

```http
GET /internal/v1/copy/execution-accounts/diagnostics
```

Revisar en orden:

1. `hasNewDispatchEnabled`.
2. `hasMicroLiveEnabled` / `hasLiveEnabled`.
3. usuarios, API keys, capital, maxWallet y allocations elegibles.
4. `reasonsIfZero`.
5. log `copy.candidate.resolve.finished` y su `eligibilitySummary`.
6. log `copy.dispatch.order_intent`.
7. fila de `copy_dispatch_intent` por `source_event_id`/`client_order_id`.
8. estado Binance por lookup read-only de `orderId` o `clientOrderId`; nunca reenviar manualmente un timeout ambiguo.

Interpretacion rapida:

| Evidencia | Diagnostico |
|---|---|
| `eligibleUsers=0` + reasons | eligibility/guard/configuracion |
| `submittedTasks>0`, sin intent | camino legacy o fallo antes del claim durable |
| intent `REJECTED` | revisar `last_error_code`, capital total, filtros Binance y limite opcional del usuario |
| intent `RECONCILING` | resultado ambiguo; esperar worker, no resend |
| intent `MANUAL_REVIEW` | reconciliacion agotada; intervencion humana |
| intent `PERSISTENCE_PENDING` | Binance respondio, falta ledger local |
| resize/close huerfano | localizar por que OPEN fue omitida; no abrir para compensar |

## 4. Canary MICRO_LIVE

1. Mantener LIVE desactivado.
2. Dejar una sola cuenta/cartera canary aprobada con saldo disponible >=100 USDC; pausar nuevas entradas del resto de allocations MICRO_LIVE.
3. Activar `COPY_NEW_DISPATCH_ENABLED=true` y `COPY_MICRO_LIVE_ENABLED=true`.
4. Observar un ciclo OPEN -> fill/reconciliacion -> REDUCE/CLOSE.
5. Confirmar total user+wallet abierto+reservado <=100, sizing proporcional y ausencia de monto fijo por orden.
6. Confirmar que dos estrategias de la misma wallet consumen el mismo presupuesto.
7. Si existe `userMaxConcurrentPositions`, confirmar que se aplica solo a esa allocation; sin valor, no hay limite global.
8. Confirmar cero duplicados y cero ambiguos sin resolver.
9. No promover a LIVE mientras readiness tenga cero submitted/ACK/fill/close, error, duplicado, reconciliacion pendiente, slippage insuficiente o guard no final.

## 5. Deadlock

Ante `40P01`:

1. Correlacionar `flow`, `strategyKey`, `attempt` y tablas.
2. Confirmar retry `result=recovered` o `result=exhausted`.
3. Si se agota Shadow, verificar una sola fila `RECOVERABLE` en `shadow_event_dead_letter`.
4. No reprocesar a mano sin usar la misma idempotency key.
5. Capturar `pg_locks`, `pg_stat_activity` y deadlock graph antes de reiniciar.
6. Si crece DLQ, desactivar nuevas entradas/canary y mantener reconciliacion/cierres segun el procedimiento operativo aprobado.

## 6. Alertas minimas

- `increase(copy_deadlock_retry_total{result="exhausted"}[5m]) > 0`
- crecimiento de `copy_shadow_deadletter_total{result="recoverable"}`
- `copy_dispatch_total{result="ambiguous"}` sin posterior reconciliacion
- `copy_readiness_block_total{reason="micro_live_not_ready_zero_submitted_orders"}`
- `copy_reservation_rejected` sostenido
- `copy_guard_conflict_total` sostenido
- saturacion/espera Hikari (`hikaricp_connections_acquire_seconds`)

No usar wallet, userId, intentId o clientOrderId como labels Prometheus. Esos valores solo van en logs estructurados.

## 7. Rollback

1. Para detener absolutamente todo envio real: `COPY_NEW_DISPATCH_ENABLED=false`. Esto tambien bloquea cierres nuevos; usar solo como stop de emergencia.
2. Para detener aperturas conservando exits, mantener los switches reales activos y pasar allocations a `EXIT_ONLY` mediante el procedimiento administrativo aprobado.
3. Desactivar promotion: `COPY_LIVE_PROMOTION_ENABLED=false`.
4. Mantener `COPY_LIVE_ENABLED=false` y `COPY_LIVE_DRY_RUN=true`.
5. No borrar intents, operaciones, eventos ni DLQ: reconciliacion e idempotencia dependen de ellos.
6. Los indices y tablas aditivas pueden permanecer al volver el binario; no revertir migraciones destructivamente.

## Dictamen

Este codigo queda `FIXED_LOCAL_ONLY` hasta completar PostgreSQL 16, migracion controlada y canary MICRO_LIVE real posterior al despliegue. No habilitar LIVE basandose solo en dias transcurridos.
