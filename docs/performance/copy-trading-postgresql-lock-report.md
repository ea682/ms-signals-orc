# Reporte de locks, waits y pool

Fecha: 2026-07-10

## 1. Fotografia QA

| Senal | Resultado |
|---|---:|
| Client backends | 34 |
| Active | 1 (auditoria) |
| Idle | 33 |
| Idle in transaction | 0 |
| Sesiones bloqueadas | 0 |
| Transacciones > 1 minuto | 0 |
| Advisory locks | 0 |
| Deadlocks acumulados desde restart | 2.178 |

Los waits `ClientRead` de 33 conexiones idle indican clientes esperando trabajo,
no contencion de PostgreSQL. Los locks relation observados estaban granted. No
se cancelo ni termino ninguna sesion.

El contador historico de deadlocks es alto, pero `pg_stat_database` no identifica
query, tabla ni servicio. Sin logs de deadlock, `pg_stat_statements` y
application names no se puede atribuir a copy trading.

En una comprobacion posterior aparecieron 4 sesiones bloqueadas de forma
transitoria. La repeticion inmediata mostro 0 bloqueados, 0 blockers y 0 locks
no concedidos. No se capturo duracion/aplicacion antes de que resolvieran y no se
atribuye ese episodio a la auditoria ni a copy trading.

## 2. Locks del diseno MICRO_LIVE/LIVE

### Budget lock

`PostgresCopyDispatchIntentStore.acquire` usa
`pg_advisory_xact_lock(hashtextextended(user|allocation|mode,0))` antes del
intent/budget. Esto serializa solamente una allocation real, no toda la wallet
ni todos los usuarios.

La transaccion contiene claim, budget y reservation. El HTTP Binance ocurre
despues de que `acquire` retorna, por lo que no deberia retener el advisory lock
durante red externa.

### Progress lock

`CopyOperationEventServiceImpl.recordRequired` toma un advisory xact lock por:

```text
dispatchIntentId + eventType + cumulativeQty + resultingQty
```

Luego hace el precheck e insert. Dos partials identicos se serializan; dos
progresos acumulativos distintos pueden persistir de forma independiente.

### Reconciliation row lock

`findReconciliationIdsForUpdateSkipLocked` reclama hasta 50 IDs. El metodo
transaccional actual actualiza lease/status y hace flush antes de retornar. El
worker procesa HTTP fuera de esa transaccion.

QA no permitio explicar el `FOR UPDATE`: el rol carece, correctamente, de
UPDATE. El filtro sin lock se midio read-only y el comportamiento lock se probo
localmente.

## 3. Pruebas de concurrencia locales

| Caso | Resultado |
|---|---|
| Dos workers mismo intent | uno ve row; otro SKIP LOCKED retorna 0 |
| Dos replicas misma allocation | una reservation autorizada; budget final 100 |
| Advisory allocation lock | serializacion confirmada |
| Partial identico | una fila de ledger |
| Partial distinto | dos filas |
| Deadlock opuesto | una tx `40P01`, la otra commit |
| Statement timeout | cancelacion `57014` |
| Lock timeout | cancelacion `55P03` |
| Hikari max=1 | segundo borrower expira por connection timeout |
| Rollback ledger | 0 filas post-rollback |
| Crash post-reservation | pending sobrevive y consume budget |

14 tests pasaron sobre PostgreSQL local 18. Deben repetirse en CI con
Testcontainers PostgreSQL 16 y, despues, con dos replicas reales del servicio.

## 4. Hikari y capacidad

Configuracion prod de `ms-signals-orc`:

| Propiedad | Valor configurado |
|---|---:|
| maximumPoolSize | 40 |
| minimumIdle | 8 |
| connectionTimeout | 5.000 ms |
| validationTimeout | 2.000 ms |
| maxLifetime | 1.500.000 ms (25 min) |
| idleTimeout | no explicito |
| keepaliveTime | no explicito |
| leakDetectionThreshold | no explicito |
| transaction/query timeout global | no explicito |

`ms-binance-engine` no usa datasource/Hikari.

Capacidad teorica exclusiva de signals:

| Replicas signals | Max teorico | % de `max_connections=100` |
|---:|---:|---:|
| 1 | 40 | 40% |
| 2 | 80 | 80% |
| 3 | 120 | 120%, imposible |

Esto no incluye metricas, positions, pgAdmin, jobs, migrations ni herramientas.
En la captura habia 34 client backends. Dos replicas configuradas al maximo
dejarian poco margen operacional.

No hay evidencia de pool exhaustion QA y no se recomienda aumentar 40. Antes
de produccion:

1. Dar `ApplicationName` y pool name distintos a cada servicio/replica.
2. Medir Hikari pending/active/acquire p95 y concurrencia real.
3. Presupuestar conexiones por servicio, dejando reserva operacional.
4. Evaluar un max menor por replica si el throughput lo permite.
5. Definir transaction timeout para writes criticos y observar fugas antes de
   activar leak detection en un valor util.

## 5. Riesgos abiertos

- El contador historico de 2.178 deadlocks no tiene attribution.
- Se observo una muestra fugaz de 4 bloqueados, resuelta antes del detalle.
- No se conoce cantidad de replicas desplegadas; no hay compose local para
  signals/binance.
- 22 sesiones JDBC idle usan un application name generico.
- Reconciliation con cardinalidad LIVE real no fue medido.
- Un worker lento conserva lease logico, aunque no el row lock; se debe validar
  takeover/lease con clock skew y crash de proceso en canary.
