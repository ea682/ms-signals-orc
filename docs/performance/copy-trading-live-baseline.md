# Baseline del camino real de copy trading

Fecha: 2026-07-10

Servicios:

- `ms-signals-orc`, branch `feature/1.4.27`, base `f4f96e7`.
- `ms-binance-engine`, branch `feature/1.0.16`, base `684d5e7`.

## 1. Alcance y regla de seguridad

Este baseline cubre MICRO_LIVE y LIVE desde el evento Hyperliquid hasta el ack,
la persistencia local y la reconciliacion. No se enviaron ordenes, cancelaciones,
cambios de leverage ni requests privados a Binance. PostgreSQL se inspecciono
con `default_transaction_read_only=on` y una transaccion `READ ONLY`.

Las cifras CPU no representan latencia end-to-end. Las cifras de PostgreSQL se
obtuvieron con tablas de intent vacias o casi vacias y tampoco representan carga
productiva. No existe una medicion autorizada de red Binance real en este trabajo.

## 2. Entorno de medicion

| Elemento | Valor |
|---|---|
| Host | Windows 11 Home Single Language |
| CPU | AMD Ryzen 7 5800H, 8 cores / 16 logical |
| RAM | 31.4 GiB visibles |
| Java | Temurin OpenJDK 21.0.11 LTS |
| PostgreSQL inspeccionado | 16.10 |
| Maven | Wrapper de cada repositorio |
| Red Binance | No utilizada |

## 3. Camino observado antes del cambio

| Fase | Componente real | Riesgo baseline |
|---|---|---|
| T0-T1 | `HyperliquidDirectDeltaIngestServiceImpl` | Dedupe local/distribuido y lane |
| T1-T3 | `HyperliquidDirectCopyDispatchServiceImpl` / candidate resolver | Candidate resolver podia terminar en loaders sincronicos |
| T3-T4 | `BinanceEngineServiceImpl` | Segunda resolucion podia hacer DB y HTTP de metricas |
| T4-T7 | sizing y construccion de `OperationDto` | LIVE depende de exposicion; MICRO de limites fijos |
| T5-T6 | `PostgresCopyDispatchIntentStore.acquire` | Claim y budget atomicos; lock por allocation |
| T8-T11 | ORC HTTP -> Binance engine -> Binance | Dominado por red externa; no medido aqui |
| T11-T13 | normalizacion, ledger y operation | Un fill podia quedar sin enlace requerido al ledger |
| Recovery | reconciliador | Agotamiento usaba estado no explicito para revision manual |

Brechas baseline verificadas:

1. Un gate real bloqueado podia fabricar un fill SHADOW local.
2. Users, allocations y metric candidates podian cargar DB/HTTP en cache miss.
3. Faltaba `MANUAL_REVIEW` como estado operacional explicito.
4. `copy_operation_event_id` no se enlazaba al intent antes de `PERSISTED`.
5. El manejo concurrente de progreso del ledger podia consultar dentro de una
   transaccion abortada por unique violation.
6. Caches de clientes/settings Binance no tenian limite de cardinalidad.
7. La remocion dinamica del lock account+symbol tenia una carrera que podia
   crear dos locks vivos para la misma key.
8. Timers Binance incluian labels de symbol y no separaban pre-network/network.

## 4. Baseline funcional

| Servicio | Tests | Failures | Errors | Wall observado |
|---|---:|---:|---:|---:|
| signals antes | 390 | 0 | 0 | 28.446 s |
| Binance antes | 26 | 0 | 0 | 15.883 s |

El wall de Maven incluye compilacion, inicio de JVM y estado incremental del
workspace. Sirve como evidencia de reproducibilidad, no como KPI productivo.

## 5. Baseline CPU puro

Benchmark: SHA-256 de identidad/idempotency + politica de budget en memoria.
Warmup: 2.000/5.000 iteraciones segun prueba. Sin DB, HTTP, JSON ni Spring MVC.

| Medida | Baseline |
|---|---:|
| p50 identity + budget | 0.0040 ms |
| p95 identity + budget | 0.0106 ms |
| 100 eventos, dos estrategias, promedio/evento | 0.1554 ms |

Baseline concurrente CPU:

| Eventos | Threads | p50 ms | p95 ms | p99 ms | max ms | EPS |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 1 | 0.3297 | 0.3297 | 0.3297 | 0.3297 | 112.87 |
| 10 | 10 | 0.0262 | 0.1161 | 0.1161 | 0.1161 | 36,403 |
| 50 | 50 | 0.0214 | 0.0630 | 0.1206 | 0.1206 | 38,943 |
| 100 | 64 | 0.0247 | 0.0815 | 0.1047 | 0.6313 | 48,605 |
| 300 | 64 | 0.0150 | 0.0623 | 0.2220 | 0.4094 | 114,443 |
| 1,000 | 64 | 0.0125 | 0.0378 | 0.1214 | 0.4608 | 357,641 |

El caso de un evento esta dominado por startup de threads y no debe usarse como
estimacion de una orden.

## 6. Baseline PostgreSQL read-only

Estado observado:

| Medida | Valor |
|---|---:|
| dispatch intents | 0 |
| PERSISTED con enlaces faltantes | 0 |
| reservas pending | 0 |
| manual review | 0 |
| allocations reales activas | 2 |

`EXPLAIN (ANALYZE, BUFFERS)` observado antes de la correccion:

| Query | Plan / tiempo |
|---|---|
| lookup por idempotency key | unique index, 0.027 ms, shared hit 2 |
| snapshot de allocations activas | seq scan, 0.075 ms, shared hit 1; tabla de 2 filas |
| pending budget | usa `ix_copy_dispatch_intent_allocation_budget` |
| reconciliation/ledger | seq scan con tablas vacias; no concluyente |

El schema desplegado ya tenia la tabla de intents y sus indices base, pero el
constraint de status no incluia `MANUAL_REVIEW`. El plan con cardinalidad casi
cero no valida p95, lock wait, pool pressure ni comportamiento de autovacuum.

## 7. Presupuesto objetivo

| Tramo | p50 | p95 | p99 |
|---|---:|---:|---:|
| local source -> HTTP ORC | <10 ms | <25 ms | <50 ms |
| Binance engine pre-network | <2 ms | <5 ms | <10 ms |
| claim + reserva | <3 ms | <10 ms | <20 ms |
| allocation + guard cache | <1 ms | <3 ms | <5 ms |
| persistencia post-ack | <5 ms | <15 ms | <30 ms |

No hay evidencia end-to-end suficiente para declarar cumplidos estos SLO. Las
metricas agregadas permiten medirlos despues de desplegar en audit-only.

## 8. Hipotesis de optimizacion

| ID | Hipotesis | Evidencia requerida |
|---|---|---|
| H1 | snapshots cache-only eliminan spikes de DB/HTTP en T2-T4 | cache hit/miss y timers |
| H2 | clientes/settings bounded preservan reuse sin crecimiento ilimitado | cache size, heap y GC |
| H3 | stripes estables eliminan carrera y churn de locks | concurrency test y lock wait |
| H4 | timers sin symbol reducen cardinalidad | cantidad de series |
| H5 | separar pre-network/network localiza el cuello real | p50/p95/p99 por timer |
| H6 | una query agregada de budget evita roundtrips repetidos | datasource timings y EXPLAIN |

## 9. Baseline faltante obligatorio

Antes de LIVE canary aun se necesita, con datos anonimizados y staging:

- Testcontainers o PostgreSQL con cardinalidad productiva.
- Claim/reserva con 1, 10 y 100 threads y dos/cuatro replicas.
- Burst por misma wallet/symbol y allocations distintas.
- Pool exhaust, deadlock, DB timeout por cada persistencia y restart real.
- JFR de CPU, heap, GC, locks y allocations.
- Latencia ORC->Binance engine con HTTP local y pooling real.
- Canary controlado para latencia de red Binance; este documento no la inventa.
