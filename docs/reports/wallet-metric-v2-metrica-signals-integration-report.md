# Wallet Metric Economic V2 - Metrica Cuenta / Signals Integration Report

Fecha de cierre tecnico: 2026-07-13

## 1. Resumen ejecutivo

Se completo la integracion de lectura Wallet Metric Economic V2 entre
`ms-metrica-cuenta` y `ms-signals-orc` para el flujo discovery -> full -> copy guard ->
SHADOW. La unidad real es una estrategia independiente, no una wallet:

```text
strategyKey = lower(walletId)
            + "|" + upper(strategyCode)
            + "|" + upper(scopeType)
            + "|" + upper(scopeValue)
```

El modo V2 solo publica una generacion `ACTIVE`, conserva UNKNOWN como null, exige FULL y
guard coherentes para nuevas exposiciones y nunca hace fallback silencioso a V1 o a todos
los usuarios. Esta tarea no habilita MICRO_LIVE ni LIVE, no activa generaciones, no emite
ordenes y no modifica datos productivos.

## 2. Estado anterior

- Los endpoints publicos y los consumidores conservaban contratos V1 y V2 parcialmente
  superpuestos.
- La seleccion podia perder scope al razonar por wallet o estrategia incompleta.
- Summary/full/guard no tenian una frontera canonica comun entre TypeScript y Java.
- Signals no tenia un snapshot atomico, durable y generation-aware de los tres recursos.
- Los flags legacy permitian configurar fallback a todos los usuarios cuando una allocation
  no estaba disponible.
- La query V2 de Signals enviaba `limit` aunque el contrato Nest canonico usa
  `limitWallet`.

## 3. Arquitectura final

```text
PostgreSQL V2 (solo generation ACTIVE)
        |
        v
EconomicMetricV2Repository -- batching / no V1 fallback
        |
        v
EconomicJewelV2Service -- summary / full / exact / windows
        |
        v
MetricStrategyReadPort -- V1 | COMPARE | V2
        |
        v
OperacionesController HTTP
        |
        v
MetricWalletsInfoClient
        |
        v
MetricV2SnapshotStore -- validate / singleflight / atomic swap
        |                    |
        |                    +--> PostgreSQL durable snapshot
        v
MetricCopyDecisionGateway / CopyStrategyRuntimeRouter
        |
        v
SHADOW allocation exacta por strategyKey
```

Metrica Cuenta resuelve el modo una vez en `MetricReadModeResolver` y
`MetricStrategyReadService`. Signals lo hace una vez en `MetricWalletReadModeResolver`.
COMPARE observa diferencias pero no mezcla fuentes ni opera con V2.

## 4. Contrato V2 canonico

Los tres fixtures compartidos conceptualmente verifican:

- `metricVersion=2` y `sourceVersion=wallet_metric_economic_v2`;
- `generationId`, `generationActivatedAt`, `computedAt`, `dataAsOf`;
- identidad completa y `strategyKey` canonico;
- `certificationStatus`, `degradationState`;
- `allowNewEntries`, `decisionFinal`, `evaluationMode`;
- `qualityFlags`, `reasonCodes`, `unknownEconomicFields`;
- ciclos, historia, freshness y coverage;
- payload summary/full/windows sin convertir null a cero.

Ubicaciones:

- Metrica Cuenta: `test/fixtures/contracts/metric-v2-*.json`.
- Signals: `src/test/resources/contracts/metric-v2-*.json`.

El validator rechaza version, fuente, generation, campos requeridos o `strategyKey`
incoherentes. Campos adicionales compatibles se preservan.

## 5. Endpoints y fuentes

| Endpoint | Modo V2 | Query canonica | Fuente | Uso Signals |
|---|---|---|---|---|
| `GET /operaciones/metrica/joyas` | summary | `limitWallet`, `dayz`, `simulation=summary` | State/certification ACTIVE | Discovery; nunca autoriza copy |
| `GET /operaciones/metrica/joyas` | full | `limitWallet`, `dayz`, `simulation=full` | Facts/ciclos/reglas ACTIVE | Finalists y decision SHADOW |
| `GET /operaciones/metrica/copy-guard/windows` | V2 | `limitWallet`, `dayz` | Facts/ciclos ACTIVE por strategyKey | Ventanas y bloqueo de nueva exposicion |
| `GET /operaciones/metrica/copy-decision` | full exacto | wallet + strategy + scope | Unidad V2 exacta | Promocion/materializacion exacta |

Los metodos Feign V2 envian `limitWallet`; los metodos V1 conservan `limit`. Los endpoints
diagnosticos V2 previos permanecen temporalmente por compatibilidad, pero la ruta canonica
es el port comun.

## 6. Generacion ACTIVE

- El repository selecciona exclusivamente `wallet_metric_generation_v2.status='ACTIVE'`.
- BUILDING, VALIDATING, WAITING, FAILED y SUPERSEDED quedan fuera.
- Cero ACTIVE produce `V2_ACTIVE_GENERATION_MISSING`; V2 no cae a V1.
- Mas de una ACTIVE se considera corrupcion contractual y falla de forma explicita.
- Summary, full y guard deben corresponder a la misma generacion.
- Signals rechaza una combinacion con generation ausente o diferente y conserva el ultimo
  snapshot coherente solo hasta su limite de staleness.

La tarea no cambio ni activo generaciones. El rebuild sigue siendo responsabilidad del ETL
y del proceso de activacion existente.

## 7. Summary

Summary hace discovery barato sobre unidades ACTIVE:

- no carga facts ni ejecuta simulacion full;
- `evaluationMode=SUMMARY`;
- `decisionFinal=false`;
- `allowNewEntries=false`;
- no habilita SHADOW, MICRO_LIVE ni LIVE;
- preserva UNKNOWN y reasons;
- ordena candidatos sin deduplicar por wallet.

Signals lo usa para el conjunto/ranking a refrescar, no como autorizacion operativa.

## 8. Full

Full evalua por `strategyKey`:

- ciclos completos e historia minima;
- PnL neto despues de fee/funding/slippage/redondeo;
- expectancy, profit factor y drawdown;
- coverage y freshness;
- reglas de simbolo y flags criticos;
- certificacion compatible y `allowNewEntries` de la unidad.

Los defaults iniciales son 30 ciclos, 30 dias, PnL/expectancy netos positivos, profit factor
neto >= 1.15, drawdown <= 35%, coverage completa y sin UNKNOWN critico. Solo una respuesta
FULL completa puede establecer `decisionFinal=true` y `eligibleForShadow=true`.

## 9. Copy guard

Las ventanas son `1d`, `3d`, `1w`, `2w`, `3w`, `1mo`, `2mo`, `3mo`, `6mo`, `9mo`,
`1y`, `2y` y `all`. Cada una conserva PnL neto, coverage, ciclos, freshness y UNKNOWN.

El guard se evalua sobre la misma identidad/generacion que FULL. Una ventana requerida
negativa, incompleta o UNKNOWN bloquea nueva exposicion. No convierte ausencia en cero.

## 10. SHADOW y promocion

- La allocation SHADOW incluye wallet, strategy, scope y strategyKey.
- Dos strategies o dos simbolos de la misma wallet siguen independientes.
- Summary no auto-inscribe.
- FULL + guard coherentes permiten SHADOW si la politica lo autoriza.
- `METRIC_WALLET_V2_SHADOW_AUTO_ENROLL_ENABLED` permanece `false` por defecto.
- El gateway V2 devuelve `V2_MONEY_PROMOTION_DISABLED` para promocion monetaria.
- MICRO_LIVE y LIVE no se habilitaron ni se probaron con dinero.

## 11. Seguridad por accion

| Estado de Metrica/Cache | OPEN | INCREASE | CLOSE | REDUCE | FLIP |
|---|---|---|---|---|---|
| FULL + guard validos y frescos | Permite segun allocation | Permite segun allocation | Permite | Permite | Cierra y abre solo si autorizado |
| Summary solamente | Bloquea | Bloquea | Permite estado existente | Permite estado existente | Cierra; bloquea nueva apertura |
| Cache ausente/stale | Bloquea | Bloquea | Permite si existe posicion | Permite si existe posicion | Cierra; bloquea apertura |
| Contrato/generacion invalida | Bloquea | Bloquea | Permite si existe posicion | Permite si existe posicion | Cierra; bloquea apertura |
| Copy guard negativo | Bloquea | Bloquea | Permite | Permite | Cierra; bloquea apertura |

La aplicacion rechaza al iniciar `fail-open-new-exposure=true` o
`allow-close-on-failure=false`. En V2, `CopyAllocationSafetyPolicy` exige allocation exacta
y nunca permite fallback global aunque los flags V1 esten configurados de forma insegura.

## 12. Cache, refresh e idempotencia

- Refresh summary: default `PT2M`.
- Refresh full: default `PT10M`.
- Refresh guard: default `PT2M`.
- Staleness maxima: default `PT10M`.
- Singleflight evita refresh concurrente duplicado por nodo.
- Summary/full/guard se validan como conjunto y se publican con swap atomico.
- Cambio de generation o candidate set invalida combinaciones previas.
- Cada entrada conserva strategyKey, generation, version, fetched/dataAsOf/expiresAt,
  decision y reasons.
- El hot path no hace HTTP y no ejecuta simulacion full.
- Idempotency/fingerprints SHADOW incluyen `strategyKey`.

La persistencia PostgreSQL usa `pg_advisory_xact_lock` y reemplazo transaccional para que
dos replicas no publiquen una mezcla parcial.

## 13. Migracion

Se agrego `V202607130007__metric_v2_strategy_generation_identity.sql`, posterior a la
version maxima encontrada. Es aditiva y:

- detecta colisiones antes del backfill;
- normaliza wallet/strategy/scope y completa `strategy_key`;
- adapta allocations, profiles, operaciones, eventos y posiciones SHADOW;
- agrega defaults/NOT NULL compatibles donde corresponde;
- crea indices parciales para allocations/posiciones activas;
- crea `metric_strategy_snapshot_v2` con PK `(snapshot_type, strategy_key)`;
- valida tipo, version y expiracion;
- no borra tablas, columnas, migraciones ni filas V1.

Flyway valido 38 migraciones y aplico la nueva en PostgreSQL aislado. Existe una advertencia
de compatibilidad de herramienta por usar PG18 local cuando Flyway declara soporte hasta
PG17.

## 14. Configuracion

Metrica Cuenta:

```text
METRICS_READ_MODE=COMPARE
METRICS_ECONOMIC_V2_ENABLED=true
METRICS_ECONOMIC_V2_READ_CUTOVER_ENABLED=false
METRICS_V2_REQUIRE_ACTIVE_GENERATION=true
```

Signals:

```text
METRIC_WALLET_READ_MODE=COMPARE
METRIC_WALLET_JOYAS_SIMULATION=summary
METRIC_WALLET_V2_SUMMARY_REFRESH_AFTER=PT2M
METRIC_WALLET_V2_FULL_REFRESH_AFTER=PT10M
METRIC_WALLET_V2_COPY_GUARD_REFRESH_AFTER=PT2M
METRIC_WALLET_V2_MAX_STALENESS=PT10M
METRIC_WALLET_V2_FAIL_OPEN_NEW_EXPOSURE=false
METRIC_WALLET_V2_ALLOW_CLOSE_ON_FAILURE=true
METRIC_WALLET_V2_SHADOW_AUTO_ENROLL_ENABLED=false
COPY_TRADING_DEFAULT_EXECUTION_MODE=SHADOW
```

Los ejemplos local/prod estan redacted. No se modificaron secretos productivos.

## 15. Pruebas exactas

| Verificacion | Resultado |
|---|---|
| Metrica Cuenta, suite completa | 155 passed, 6 skipped, 0 failed; 14 suites passed y 2 skipped |
| Metrica Cuenta, PostgreSQL explicito | 5/5 passed |
| Metrica Cuenta, contratos/endpoints | 12/12 controller tests y fixtures verdes |
| Metrica Cuenta `npm run build` | passed, 7.8 s |
| ESLint de todos los TS cambiados | passed |
| npm audit productivo | 0 vulnerabilidades |
| Signals `mvn test` | 610 tests, 0 failures, 0 errors, 5 skipped |
| Signals `mvn clean package` | BUILD SUCCESS, jar creado |
| Signals PostgreSQL explicito | passed |
| Signals Spring context | passed |
| Integracion HTTP real | summary=5, full=5, guard=5, 13 ventanas, keys/generation coherentes |
| Safety allocation V2 | flags inseguros no habilitan fallback global |

Los cinco skips de Signals incluyen pruebas opt-in HTTP, benchmark y PostgreSQL en la suite
normal; se ejecutaron explicitamente por separado. Un test Testcontainers historico no se
ejecuto al no haber Docker; la integracion relevante se cubrio con PostgreSQL local aislado.

## 16. Rendimiento

Metrica Cuenta warm, 30 iteraciones:

| Caso | p50 | p95 | Max |
|---|---:|---:|---:|
| summary 20 | 3.08 ms | 3.98 ms | 10.46 ms |
| summary 100 | 2.87 ms | 3.21 ms | 3.51 ms |
| full 20 | 3.16 ms | 3.64 ms | 3.92 ms |
| guard 20 | 3.46 ms | 4.10 ms | 5.31 ms |

Signals hot lookup, 100.000 operaciones: p50 3.7797 us, p95 21.1215 us, max
27.7480 us y cero llamadas remotas. Primer refresh HTTP cold: 339 ms total.

No habia baseline V2 equivalente ejecutable; no se invento un "antes" numerico. Los
detalles y limites estan en los reportes de rendimiento de ambos repositorios.

## 17. Limpieza y legado

Decisiones principales:

- REUSE: simulador economico existente, TypeORM/pg, entidades y servicios SHADOW validos.
- ADAPT: controller, repository V2, service V2, Feign client, decision gateway, router y
  entidades de identidad.
- CONSOLIDATE: ports/adapters de modo, contrato V2, snapshot store y safety policy.
- DELETE: dependencias npm directas sin uso (`@nestjs/microservices`, `kafkajs`,
  `swagger-ui-express`), JUnit 4 directo y fallback all-users en la ruta V2.
- KEEP_TEMPORARY: adapters/DTO/repositorios/caches V1 solo por rollback demostrado.
- DEPRECATE: flag de cutover anterior y flags de fallback V1.
- BLOCKED_FROM_DELETE: endpoints publicos diagnosticos y capabilities legacy con
  consumidores/contratos aun vigentes.

Inventarios completos:

- `ms-metrica-cuenta/docs/reports/wallet-metric-v2-legacy-cleanup-report.md`
- `ms-signals-orc/docs/reports/wallet-metric-v2-legacy-cleanup-report.md`

## 18. Advertencias honestas

1. Docker no estaba disponible; se uso PostgreSQL 18 aislado, no Testcontainers PG16.
2. El lint global de Metrica Cuenta conserva deuda preexistente CRLF/Prettier; todos los
   TypeScript modificados pasan lint scoped.
3. `npm audit --omit=dev` esta limpio; el audit incluyendo dev reporta 20 vulnerabilidades
   de toolchain antigua (4 low, 8 moderate, 8 high). Resolverlas exige una modernizacion de
   desarrollo separada, no cambios arbitrarios productivos.
4. `mvn clean package` reporta warnings preexistentes: propiedades MapStruct no mapeadas,
   `ThreadDeath` deprecado y APIs unchecked/deprecated. No impidieron el build.
5. Los primeros intentos de `npm run build` encontraron un `EPERM` transitorio sobre
   `.d.ts` retenidos por el watcher de la prueba HTTP. Tras liberar los handles, el comando
   estandar paso y recreo `dist`; no queda como bloqueo.
6. No se levanto Signals completo contra brokers/exchanges y no se emitieron ordenes.

## 19. Cutover recomendado

1. Completar y validar rebuild V2.
2. Activar una generacion mediante el proceso existente, nunca manualmente desde Signals.
3. Metrica Cuenta en COMPARE; observar diferencias por strategyKey.
4. Verificar summary/full/guard, freshness y UNKNOWN en staging.
5. Signals en COMPARE, confirmando que el runtime operativo sigue en la fuente anterior.
6. Metrica Cuenta a V2.
7. Bloquear nuevas exposiciones durante la ventana de observacion.
8. Signals a V2, solo SHADOW y auto-enroll deshabilitado.
9. Observar refresh, cache, guard, cierres y cambios de generacion.
10. No habilitar MICRO_LIVE/LIVE como parte de este cutover.

Metricas operativas a observar: refresh summary/full/guard, hit/miss, generation change,
contract error, open blocked y close allowed on metric failure, ademas de latencias
p50/p95/p99 y cardinalidad por strategyKey.

## 20. Rollback

1. Volver Signals a COMPARE/V1.
2. Volver Metrica Cuenta a COMPARE/V1.
3. No cambiar ni borrar la generacion ACTIVE como parte del rollback de lectura.
4. No borrar tablas/snapshots V2.
5. No cerrar posiciones por cambiar read mode.
6. Mantener CLOSE/REDUCE disponibles.

## 21. Condicion para retirar V1

V1 puede eliminarse solo despues de un periodo productivo V2 acordado sin divergencias
materiales, inventario de consumidores externos cerrado, rollback probado, observabilidad
suficiente y aprobacion explicita de una tarea destructiva separada. Hasta entonces queda
aislado, no mezclado con V2.

## 22. Acciones no realizadas

No se hizo commit, push, merge, tag, deploy, activacion de generacion, cambio de offsets,
TRUNCATE, cambio de secretos, orden Binance, MICRO_LIVE ni LIVE.
