# Wallet Metric V2 - Signals Legacy Cleanup Report

Fecha de verificacion: 2026-07-13

## Resumen

Signals conserva V1 y COMPARE solo como rollback, pero el modo V2 usa un contrato,
snapshot store y decision gateway propios. No transforma DTO V1 en V2, no consulta full
en el hot path y no permite fallback a todos los usuarios.

La limpieza evito borrar entidades, columnas o migraciones historicas. Las entidades
persistentes fueron adaptadas a `strategyKey`; la migracion nueva es aditiva.

## Inventario y decision

| Elemento | Repositorio | Uso actual | Decision | Evidencia | Accion |
|---|---|---|---|---|---|
| `MetricWalletsInfoClient` | ms-signals-orc | Cliente HTTP unico de Metrica Cuenta | CONSOLIDATE | V1, summary/full V2, exact y guard comparten la interfaz | Se corrigio `limitWallet` solo para endpoints V2; V1 conserva `limit` |
| `MetricWalletServiceImpl` | ms-signals-orc | Servicio historico y refresh coordinado | ADAPT | Tests V1/COMPARE/V2 | Delega V2 al snapshot store; V1 queda aislado |
| `MetricaWalletDto` | ms-signals-orc | Contrato V1/rollback y proyeccion historica | KEEP_TEMPORARY | Consumidores V1 reales | No se usa como contrato HTTP V2 canonico |
| `MetricStrategySnapshotDto` | ms-signals-orc | Contrato V2 canonico | REUSE | Fixtures, validator, HTTP y gateway | Preserva null/UNKNOWN y toda la identidad |
| `CopyDecisionDto` | ms-signals-orc | Contrato interno de promocion existente | ADAPT | Gateway y servicios de promocion lo consumen | El adapter V2 marca dinero deshabilitado aunque SHADOW sea elegible |
| `CopyGuardWindowSnapshotDto` | ms-signals-orc | Contrato historico V1 | KEEP_TEMPORARY | Rollback V1/COMPARE | V2 usa snapshots canonicos por `strategyKey` y generacion |
| `MetricWalletReadModeResolver` | ms-signals-orc | Fuente unica de modo | REUSE | Binding test y Spring context | V2 no habilita fallback; COMPARE no opera con V2 |
| `MetricV2SnapshotStore` | ms-signals-orc | Cache canonica V2 | CONSOLIDATE | Summary/full/guard, singleflight, validacion cruzada | Swap atomico; hot path solo memoria |
| `PostgresMetricV2SnapshotPersistence` | ms-signals-orc | Persistencia durable de cache | REUSE | Prueba PostgreSQL y advisory lock | Replace transaccional, una fila por tipo + strategyKey |
| Caches Caffeine V1 | ms-signals-orc | Runtime V1/rollback | KEEP_TEMPORARY | `METRIC_WALLET_READ_MODE=V1` | No se leen ni mezclan en V2 |
| `MetricCopyDecisionGateway` | ms-signals-orc | Decision exacta de promocion | ADAPT | Tests exact/full/guard | V2 requiere FULL exacto y bloquea promocion monetaria |
| `CopyStrategyRuntimeRouter` | ms-signals-orc | Matching por estrategia/scope | ADAPT | Tests LONG/SHORT/SYMBOL y strategyKey | No colapsa por wallet |
| `HyperliquidCopyCandidateResolver` | ms-signals-orc | Seleccion del usuario en hot path | ADAPT | Tests de allocations y failure modes | V2 exige allocation exacta y decision saludable |
| `CopyAllocationSafetyPolicy` | ms-signals-orc | Politica central de fallback | CONSOLIDATE | Test con flags inseguros | En V2 siempre filtra allocation y nunca usa all-users fallback |
| `OperacionEventIngestServiceImpl` | ms-signals-orc | Ingest historico | ADAPT | Usa la politica central | Elimina logica duplicada de flags |
| `UserCopyAllocationEntity` | ms-signals-orc | Allocation productiva | ADAPT | Migracion y tests de normalizacion | Identidad completa + `strategyKey` |
| `ShadowCopyAllocationEntity` | ms-signals-orc | Allocation SHADOW | ADAPT | Migracion y servicios SHADOW | Scope independiente por estrategia |
| `CopyWalletProfileEntity` | ms-signals-orc | Perfil/certificacion | ADAPT | Migracion | Profile key canonica |
| Operaciones/eventos/estado SHADOW | ms-signals-orc | Ledger SHADOW vigente | ADAPT | Entidades y migracion | Propagan strategy/scope/key en idempotencia |
| `V202607130007__metric_v2_strategy_generation_identity.sql` | ms-signals-orc | Evolucion aditiva | REUSE | Flyway y prueba PostgreSQL | Backfill, validacion de colisiones, indices y snapshot table |
| Fallback `byWallet`/all-users en ruta V2 | ms-signals-orc | Comportamiento inseguro previo | DELETE | Safety tests y busqueda de consumidores | Imposible en V2 aun si el flag legacy esta en true |
| Flags `fallback-all-users-on-empty-allocation` | ms-signals-orc | Rollback V1 | DEPRECATE | Aun enlazados a V1 | La policy central los ignora en V2 |
| Dependencia directa JUnit 4 | Maven | Sin tests JUnit 4 | DELETE | Busqueda sin imports `org.junit.*` V4 | Eliminada de `pom.xml` |
| `ProcesBinanceService.getPositions` | ms-signals-orc | Snapshot autoritativo Binance | ADAPT | La implementacion productiva y su test ya existian | Se elimino el default no soportado; toda implementacion debe declarar la capability |
| `CopySimulationJobStore.requestPause/resume` | ms-signals-orc | Control de jobs de simulacion | ADAPT | PostgreSQL implementa ambas operaciones | Se eliminaron defaults no soportados y se hicieron obligatorias en el contrato |

## Dependencias y duplicacion

- Se elimino `junit:junit`; la suite usa JUnit Platform/Jupiter.
- `mvn dependency:analyze` termino `BUILD SUCCESS`. Sus avisos sobre starters y
  transitivas de Spring son falsos positivos habituales del analizador bytecode y no
  justifican retirar starters activos.
- No se creo un segundo Feign client para V2. El mismo cliente expone metodos con contratos
  distintos y el snapshot store los coordina.
- No se crearon caches paralelas por wallet. El snapshot V2 usa `strategyKey` y
  `generationId`; caches V1 quedan fuera del modo V2.

## Configuracion viva

- `METRIC_WALLET_READ_MODE=V1|COMPARE|V2` gobierna el consumo.
- `METRIC_WALLET_V2_*_REFRESH_AFTER` controla refresh summary/full/guard.
- `METRIC_WALLET_V2_MAX_STALENESS` controla fail-closed de exposicion nueva.
- `METRIC_WALLET_V2_FAIL_OPEN_NEW_EXPOSURE` debe ser `false`; la aplicacion rechaza una
  configuracion insegura.
- `METRIC_WALLET_V2_ALLOW_CLOSE_ON_FAILURE` debe ser `true`.
- `METRIC_WALLET_V2_SHADOW_AUTO_ENROLL_ENABLED=false` por defecto.
- `COPY_TRADING_DEFAULT_EXECUTION_MODE=SHADOW` permanece como default seguro.

## Evidencia

- `mvn test`: 610 tests, 0 failures, 0 errors, 5 skipped.
- `mvn clean package`: BUILD SUCCESS y jar Spring Boot creado.
- PostgreSQL explicito: migracion y snapshot store verdes.
- Spring context: verde.
- Fixtures summary/full/copy guard: parseo y validacion verdes.
- Cliente HTTP: V2 envia `limitWallet`; V1 sigue enviando `limit`.

## Retiro final V1

Retirar DTO, caches, metodos Feign y flags V1 solo cuando Metrica Cuenta y Signals hayan
operado en V2 durante el periodo acordado, COMPARE no muestre divergencias materiales,
todos los consumidores externos esten identificados y exista rollback de datos/codigo
aprobado. La eliminacion debe ser una tarea posterior explicita; esta migracion no destruye
historia.
