# Wallet Strategy End-To-End Financial Engine - Final Local Report

Fecha: 2026-07-14  
Repositorios: ETL, Metrics y Signals reales  
Estado: integracion local GREEN; dinero real deshabilitado

## 1. Resultado ejecutivo

El sistema dejo de tratar la wallet como unidad economica unica. La identidad canonica
es `wallet|strategy|scopeType|scopeValue` y se conserva de extremo a extremo junto con
generationId, sourceEventId y versiones.

ETL posee hechos; Metrics posee analisis/ranking/Top 10; Signals posee sizing, gates,
simulacion, dispatch y calibracion. El sizing unico vive en el `copy-target-core`
interno de Signals. No se modificaron Binance ni Hyperliquid porque el contrato actual
permitio cerrar esta integracion sin introducir acoplamiento nuevo.

## 2. Flujo de datos

```text
Hyperliquid event
  -> Signals identity + metric full decision
  -> copy-target-core sizing
  -> SHADOW executable / dispatch gate
  -> operation event + transactional outbox
  -> Kafka copy-operation-event-persisted-v1
  -> ETL economic facts/cycles/calibration
  -> PostgreSQL V2/V3 read models
  -> Metrics summary/full/windows/race/Top10
  -> Signals generation-aware cache and next decision
```

La matriz capital x leverage es cold path: Signals genera 44 escenarios, PostgreSQL los
desacopla y Metrics los lee. No hay dependencia HTTP circular.

## 3. Reglas financieras cerradas

- Equity autoritativa obligatoria para exposicion nueva.
- CLOSE/REDUCE nunca bloqueados por falta de metricas/equity.
- FLIP close-first y nuevo lado con autorizacion independiente.
- Notional proporcional a exposure/equity; leverage solo cambia margen/riesgo.
- Round down, no boost a minNotional y common scaling.
- UNKNOWN/null no se convierte en cero favorable.
- Fees, funding, slippage, latency, venue basis, capacity y liquidation se versionan y
  bloquean cuando falta evidencia.
- Account flows no se confunden con PnL.
- Summary no ejecuta; full puede habilitar SHADOW; dinero requiere gates adicionales.

## 4. Competencia y operacion

Metrics expone SourceSkill, Race, Health, Copyability, Evidence y Scalability por
separado. Top 10 usa hysteresis, persistence, advantage, promotion benefit y rotation
cost. Una estrategia historica ausente queda PAUSED, no desaparece. La misma wallet
puede tener LONG_ONLY y SHORT_ONLY simultaneamente con estados distintos.

Signals aplica CANDIDATE/SHADOW/WATCH/REDUCE_CAPITAL/MICRO_LIVE_RECOVERY/PAUSED/RETIRED,
capitalMultiplier gradual, copy guard, freshness, evidence, capacity y feature flags.
MICRO_LIVE y LIVE permanecen false por default y no existe promocion automatica.

## 5. Persistencia, idempotencia y generaciones

Se agregaron migraciones aditivas 20260714 en los tres ownerships. No se alteraron
migraciones aplicadas ni datos productivos. Dispatch, escenarios, evaluaciones,
decisiones y calibraciones incluyen strategyKey+generationId y claves versionadas.

Repetir un evento no duplica escenarios, facts o dispatch. Payload conflict y mismatch
de generation fallan cerrado. Cache de Metrics/Signals se invalida por generation.

## 6. Tests ejecutados

| Componente | Resultado |
|---|---|
| copy-target-core interno | 36 passed, 0 failed |
| ETL `mvnw clean package` | 147 total, 0 failed/errors, 2 skipped opt-in |
| Metrics Jest | 190 total, 189 passed, 1 skipped opt-in |
| Metrics build | GREEN |
| Metrics ESLint global | GREEN, sin auto-fix global |
| Signals `mvnw clean package` | 640 total, 0 failed/errors, 4 skipped |
| Signals PostgreSQL lock/distribution | 6 passed |
| Signals PostgreSQL array persistence | 1 passed |

PostgreSQL 16.14 se uso para Flyway, canonical/repair, race persistence, snapshots,
locks y arrays. Docker no estaba disponible; los caminos relevantes que dependian de
Testcontainers se ejecutaron contra una instancia PostgreSQL 16 aislada.

## 7. Performance

Metrics local hot p95: summary20 6.574 ms, summary100 6.140 ms, full20 4.665 ms,
windows20 5.157 ms, leaderboard 55.316 ms y matrix44 6.898 ms. Query counts son
constantes por flujo y full no hace N+1.

Signals hot decision p95 52.3734 us sin llamadas remotas; dispatch local 1000 eventos
p95 0.0539 ms. Estos numeros no incluyen latencia de exchange/red/DB productiva.

ETL conserva un benchmark historico de 36.53 eventos/s sobre 8.800 eventos; falta una
medicion nueva sobre la cardinalidad productiva actual para comparar antes/despues.

## 8. Observabilidad

Los tres servicios emiten logs estructurados con identidad, generation, versiones,
decision, reason y elapsedMs en los caminos nuevos. Signals usa Micrometer para cache,
generation, calibration, copy guard, portfolio limits, dispatch, reconciliation y
budget, incluido `signals.shadow.reality_score`. Metrics expone por `GET /metrics` los
14 contadores requeridos de race, health, copyability, evidence, scalability, drift,
selection/OOS y simulacion.

## 9. Lo que no se declara validado

- No hubo deploy, ordenes reales, promocion ni activacion de generation.
- MICRO_LIVE/LIVE no son `REAL_VALIDATED`.
- No se midio Binance/Hyperliquid real, order-book stress ni crisis de salida.
- Correlation de portfolio requiere series sincronizadas; sin ellas queda UNKNOWN.
- ShadowRealityScore real requiere pares ejecutables reales suficientes.
- Las pruebas externas opt-in no se ejecutaron para preservar datos reales.

## 10. Cutover

1. Completar rebuild V2 y revisar quality/dead letters.
2. Activar una sola generation mediante el mecanismo existente.
3. Metrics en COMPARE y comparar contratos/ranking/matriz.
4. Signals en COMPARE/V2 sin nuevas exposiciones.
5. Habilitar SHADOW ejecutable y recolectar evidencia.
6. Verificar calibration, freshness, capacity y observabilidad.
7. Evaluar MICRO_LIVE manualmente; no habilitar globalmente.
8. LIVE solo despues de evidencia real y aprobacion explicita.

## 11. Rollback

Volver read mode a COMPARE/V1 sin borrar V2/V3, no cambiar ACTIVE durante el incidente,
invalidar caches, detener workers cold si causan presion, conservar evidence/outbox y
mantener CLOSE/REDUCE. Nunca cerrar posiciones solo por cambiar read mode.

## 12. Accion manual necesaria

Desplegar primero con flags monetarios apagados, aplicar migraciones en staging,
ejecutar el replay controlado y revisar dashboard/alerts. Luego iniciar SHADOW
ejecutable. La primera habilitacion MICRO_LIVE debe ser manual, por estrategia y con
capital limitado; este cambio no la realiza automaticamente.
