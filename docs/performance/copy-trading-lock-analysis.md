# Analisis de locks Copy Trading

Fecha: 2026-07-11

## Hallazgo inicial

Los flujos Shadow escribian las mismas unidades logicas con orden diferente:

| Flujo | Orden observado |
|---|---|
| sync Shadow | `copy_wallet_profile` -> `shadow_copy_allocation` |
| evento Shadow | operacion/evento -> `shadow_copy_allocation` -> `copy_wallet_profile` |
| sync usuario | Shadow -> `user_copy_allocation` -> link Shadow |

La inversion profile/allocation permite ciclos PostgreSQL `40P01`. `syncDistribution` ademas conserva una transaccion para el job completo, por lo que los locks de distintas wallets pueden permanecer retenidos hasta el final del ciclo.

## Estrategia aplicada

1. Cada mutacion Shadow obtiene `pg_advisory_xact_lock` sobre `shadow-profile:<strategyKey>`.
2. Cuando un flujo toca varias unidades, normaliza, deduplica y ordena las keys antes del primer write.
3. Wallet/profile keys independientes no comparten lock.
4. Solo SQLState exacto `40P01` activa retry.
5. Cada retry ocurre despues del rollback, con backoff exponencial, jitter y maximo 3 intentos.
6. Un evento Shadow agotado se persiste por upsert en `shadow_event_dead_letter`, estado `RECOVERABLE`.
7. El sync agotado se informa como recuperable para el siguiente schedule.

## Dispatch real

El lock de presupuesto no comparte namespace con Shadow:

```text
userId | normalizedWalletId | MICRO_LIVE
```

Solo se obtiene para OPEN/INCREASE MICRO_LIVE. Bajo ese lock, una unica transaccion corta hace insert idempotente, snapshot agregado y reserva. LIVE, reductions y closes no toman ese lock. La llamada a `ms-binance-engine` ocurre despues del commit y nunca mantiene un lock PostgreSQL abierto.

## Propiedades demostradas localmente

- La misma idempotency key produce un solo intent logico.
- Dos estrategias de la misma user+wallet comparten 100 USDC.
- Una reserva concurrente que excederia 100 es rechazada antes del gateway.
- Keys Shadow ordenadas evitan orden inverso en el codigo.
- Retry reconoce `40P01`, se recupera o termina acotado.
- DLQ repite el mismo evento sin duplicar filas.
- Un timeout Binance queda en reconciliacion y no autoriza resend.

## Riesgos residuales

- `syncDistribution` todavia usa una transaccion para varios usuarios. El advisory lock elimina la inversion entre flujos participantes, pero una ruta futura que omita el lock podria volver a generar contencion larga.
- La suite PostgreSQL 16 no corrio en este host por ausencia de Docker. Las pruebas estan escritas, pero no se presenta su skip como evidencia de concurrencia real.
- Un `CREATE INDEX CONCURRENTLY` interrumpido puede dejar un indice invalido. El runbook exige revisar `pg_index` antes de reparar Flyway.
- Hikari mantiene maximo aproximado de 40 conexiones por replica. No se modifica hasta conocer el presupuesto global de todos los servicios.

## Mejora posterior recomendada

Extraer el trabajo por usuario de `syncDistribution` a un boundary transaccional `REQUIRES_NEW` o `TransactionTemplate`, con prueba PostgreSQL que ejecute Shadow, promotion y sync simultaneos. No debe hacerse como cambio cosmetico: modifica atomicidad del job y requiere definir que ocurre si falla el usuario N despues de confirmar N-1.

## Metricas

- `copy_deadlock_total{flow,table}`
- `copy_deadlock_retry_total{flow,attempt,result}`
- `copy_lock_wait_seconds{flow}`
- `copy_shadow_deadletter_total{result}`
- `copy_dispatch_total{mode,result}`

Alertar ante cualquier `result=exhausted`, crecimiento de DLQ o p95 de lock mayor al presupuesto del entorno.
