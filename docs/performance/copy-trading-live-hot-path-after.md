# After del hot path Copy Trading

Fecha: 2026-07-11
JVM: Java 21
Ordenes reales: ninguna
Dictamen de esta medicion: `FIXED_LOCAL_ONLY`

## Metodo

Se repitio el benchmark CPU del baseline tres veces y se reporta la mediana. El benchmark mide identidad durable + politica de presupuesto; no incluye PostgreSQL, red ni Binance.

```powershell
.\mvnw.cmd -q "-Dcopy.benchmark.enabled=true" `
  "-Dtest=CopyDispatchLocalPerformanceTest,CopyDispatchConcurrencyBenchmarkTest" test
```

## Resultado local

| Escenario | p50 after ms | p95 after ms | p99 after ms | p95 vs baseline | Delta p95 ms |
|---|---:|---:|---:|---:|---:|
| identity + budget | 0.0026 | 0.0065 | no reportado | +4.84% | +0.0003 |
| 1 tarea | 0.2744 | 0.2744 | 0.2744 | -40.13% | -0.1839 |
| 10 tareas | 0.0309 | 0.1337 | 0.1337 | -66.26% | -0.2626 |
| 50 tareas | 0.0198 | 0.0620 | 0.0916 | -22.21% | -0.0177 |
| 100 tareas | 0.0243 | 0.0610 | 0.1032 | -58.42% | -0.0857 |
| 300 tareas | 0.0196 | 0.0994 | 0.2274 | +1.95% | +0.0019 |
| 1000 tareas | 0.0137 | 0.0561 | 0.1633 | +27.50% | +0.0121 |

El p95 principal queda dentro del presupuesto de 5%. La escala 1000 supera el porcentaje relativo, pero el delta absoluto es `0.0121 ms`; ese benchmark no ejecuta ninguno de los componentes modificados y presenta variacion de scheduler entre corridas. No se interpreta como evidencia de regresion causal. Dos estrategias por evento dieron mediana `0.0211 ms`, delta absoluto `+0.0060 ms`.

## Throughput

| Tareas | Throughput mediano eventos/s |
|---:|---:|
| 1 | 112.35 |
| 10 | 35,637.92 |
| 50 | 47,312.64 |
| 100 | 52,007.49 |
| 300 | 100,136.85 |
| 1000 | 299,994.00 |

Las cifras de 1 tarea incluyen arranque del executor y no representan throughput estable. Para 300/1000 tareas hay ruido significativo del scheduler; se conservan como detector de stalls, no como SLA.

## Cambios del hot path

- Eligibility y guard siguen usando snapshots cacheados: cero queries por allocation.
- El pre-sizing MICRO_LIVE agrega como maximo las posiciones activas de la wallet; el contrato limita ese conjunto a 5.
- Solo una nueva exposicion MICRO_LIVE toma el advisory lock `userId + walletId + mode`.
- LIVE, reductions y closes no toman el lock de presupuesto MICRO_LIVE.
- El snapshot atomico usa una consulta agregada y dos indices parciales/cubrientes.
- La llamada HTTP a Binance permanece fuera de transaccion.
- Readiness y decision full se ejecutan fuera del despacho y en batch.
- Metricas adicionales son contadores/timers Micrometer en memoria; no agregan I/O.

## PostgreSQL

No se pudo ejecutar PostgreSQL 16 Testcontainers porque Docker no esta disponible en este host. Por tanto, no se afirma un after JDBC real. Los p95 QA read-only del baseline permanecen como referencia, no como validacion de la nueva consulta por wallet.

Antes de canary se debe ejecutar:

```sql
\i src/main/resources/db/validation/micro_live_wallet_budget_validation.sql
```

La suite PostgreSQL queda incluida y se habilita automaticamente con Docker o con `copy.postgres.test.jdbc-url` apuntando exclusivamente a la base desechable `copy_trading_test`.

## Pruebas

- `ms-signals-orc`: 439 pruebas, 0 fallos, 0 errores, 1 omitida (Testcontainers sin Docker).
- `ms-binance-engine`: 33 pruebas, 0 fallos, 0 errores.
- Empaquetado Spring Boot: OK; migraciones y validacion incluidas en el JAR.

## Conclusion

No hay una regresion local significativa demostrada en el trabajo CPU medido. Falta evidencia JDBC PostgreSQL 16 y un evento MICRO_LIVE posterior al despliegue; por eso el resultado no asciende a `MICRO_LIVE_CANARY_READY`.
