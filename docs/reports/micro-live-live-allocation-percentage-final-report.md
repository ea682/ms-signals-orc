# Informe final - allocation_pct SHADOW / MICRO_LIVE / LIVE

Fecha de validacion: 2026-07-11

Estado: `PRODUCTION_CANARY_READY`

## 1. Causa raiz comprobada

`allocation_pct` tenia dos significados incompatibles: porcentaje economico y
marcador positivo de una allocation MICRO_LIVE. `ShadowPromotionServiceImpl`
escribia `0.000001` para satisfacer consultas que exigian un valor positivo y
`MicroLivePromotionServiceImpl` podia heredarlo al crear LIVE. El sizing MICRO
ya era fijo, pero persistencia, elegibilidad y promocion no compartian ese
contrato.

## 2. Por que reaparecio

Los arreglos anteriores protegian el consumidor de presupuesto MICRO, pero no
eliminaban el escritor del sentinel, la condicion positiva del repositorio ni
la herencia MICRO -> LIVE. Cada nueva promocion volvia a producir el dato
ambiguo aunque el calculo 100/20/5 fuera correcto.

## 3. Escritores y lectores

El mapa completo esta en
`docs/specs/micro-live-live-allocation-percentage-sdd.md`.

Escritores corregidos:

- `ShadowPromotionServiceImpl`;
- `MicroLivePromotionServiceImpl`;
- `UserCopyAllocationServiceImpl`;
- `UserWalletCopyPlanRepository` y entidades JPA;
- migracion Flyway.

Lectores validados:

- `CopyBudgetResolver` y `BinanceEngineServiceImpl`;
- `MetricWalletServiceImpl`;
- queries de elegibilidad y snapshot runtime;
- promociones, guards, caches y planes.

## 4. Unidad del porcentaje

La unidad unica es fraccion decimal: `1.000000 = 100%`, `0.100000 = 10%`.
Rango LIVE `(0,1]`, escala maxima 6 y redondeo `HALF_UP`. MICRO_LIVE no usa
porcentaje para sizing.

## 5. Fuente LIVE canonica

La fuente es el allocator efectivo de `ms-signals-orc`, despues de seleccion
por usuario, maximos, guards y reescalado. Se materializa como snapshots
`STAGED -> COMPLETED/FAILED` en:

- `live_allocation_distribution_run`;
- `live_allocation_distribution_detail`.

La promocion solo consume el ultimo snapshot exacto y `COMPLETED` mediante
`LiveAllocationPercentageResolver`.

## 6. Multiples estrategias

La identidad economica es
`userId + walletId + strategyCode + scopeType + scopeValue`. Cada detalle lleva
el porcentaje de estrategia y el total de wallet. La suma por wallet y usuario
se valida antes de publicar y otra vez al resolver. Dos estrategias con 0.06 y
0.04 conservan total 0.10; no terminan en 0.20.

## 7. Archivos

Nuevos:

- paquete `service/copy/allocation` con request, resolution, resolver,
  publisher y servicio PostgreSQL;
- `V202607110007__allocation_percentage_contract.sql`;
- especificacion SDD, runbook y este informe;
- tests PostgreSQL 16 y contrato de repositorio.

Modificados:

- entidades y repositorios de allocation/plan;
- `MetricWalletServiceImpl`;
- servicios de promocion SHADOW y MICRO;
- allocator de usuarios y `application.yml`;
- tests de budget, metricas, promociones, distribucion y runtime.

## 8. Migracion

`V202607110007` permite NULL, agrega metadata de sizing/procedencia, crea los
snapshots, migra solo MICRO sentinel con evidencia combinada, conserva legacy
ambiguo como ignorado y pausa LIVE probado como herencia invalida. Incluye
constraints de rango y contrato. Fue aplicada dos veces sobre PostgreSQL 16.14
sin error para demostrar idempotencia logica.

## 9. Legacy detectado

En el snapshot read-only de investigacion se detectaron dos allocations MICRO
seguras (ids 505 y 506), un plan sentinel+100 asociado y cero LIVE sospechosos.
La migracion no inventa porcentajes para filas ambiguas.

## 10. Pruebas creadas

Se cubren los 14 escenarios SDD: NULL MICRO, elegibilidad, 100/20/5,
distribucion actual, cambio 0.15 -> 0.07, missing, invalidos, directo LIVE, dos
estrategias, queries mode-aware, legacy, concurrencia, rollback transaccional e
idempotencia. Tambien se prueban STAGED/FAILED, stale, total inconsistente,
constraints y upsert concurrente del plan.

## 11. Resultados

- `mvnw clean package`: 501 tests, 0 failures, 0 errors, 2 skipped.
- Skips: benchmark e integracion dead-letter que requieren infraestructura
  opcional y no pertenecen a este cambio.
- PostgreSQL externo: 12 tests, 0 failures, 0 errors, 0 skipped.
- `git diff --check`: sin errores; solo avisos de politica LF/CRLF de Git.
- JAR: contiene
  `BOOT-INF/classes/db/migration/V202607110007__allocation_percentage_contract.sql`.

## 12. PostgreSQL 16

Validado en PostgreSQL 16.14 real, aislado en un directorio temporal. Se
probaron migracion doble, constraints, resolver, agregados, snapshots fallidos,
plan concurrente y advisory locks. La instancia y sus archivos se eliminaron
al finalizar.

## 13. Concurrencia

Dos promociones simultaneas del mismo perfil producen exactamente una LIVE,
un cierre MICRO y un resultado NOOP para la segunda. Dos estrategias de la
misma wallet serializan el plan con `ON CONFLICT DO NOTHING` + lock de fila y
conservan el mismo total de wallet. Se mantiene advisory lock canonico y retry
de deadlock `40P01`.

## 14. Logs y metricas

Eventos principales:

- `copy.allocation.percentage.resolved`;
- `copy.promotion.live.percentage.resolved`;
- `copy.promotion.live.rejected`;
- `copy.allocation.legacy_sentinel.detected`;
- `copy.live_distribution.staged/published/failed`.

Los logs incluyen source, decision id, calculated/valid-until y reasonCode sin
secretos. No se detectaron claves duplicadas en los templates nuevos. Las
metricas usan tags de baja cardinalidad y no IDs.

## 15. SQL productivo

Preflight read-only, post-deploy, alertas y rollback estan en
`docs/runbooks/allocation-percentage-production-validation.md`. El runbook
informa candidates seguros, ambiguos y LIVE sospechosos; valida NULL MICRO,
fuente LIVE, sumas por wallet, elegibilidad y 100/20/5.

## 16. Riesgos residuales

- `ms-wallet-metric-etl` conserva un modelo wallet-level incompatible con el
  detalle multi-estrategia; debe seguir deshabilitado como fuente LIVE.
- Si el allocator deja de publicar, el snapshot vence y bloquea promociones
  nuevas. MICRO continua y REDUCE/CLOSE no se bloquean.
- Un run con fallo parcial queda `FAILED`, genera alerta y no puede promover;
  el siguiente ciclo debe repararlo.
- El despliegue debe empezar por canary, ejecutar preflight y conservar backup
  de las filas legacy antes de Flyway.

## 17. Declaracion final

- No hay escritura nueva de `0.000001`.
- MICRO_LIVE nuevo persiste NULL y usa capital fijo 100/20/5.
- LIVE no hereda porcentaje MICRO y exige snapshot vigente.
- Falta, stale, scope incorrecto, ambiguedad o exceso bloquean la promocion sin
  cerrar MICRO.
- El plan ya no mezcla sentinel con capital fijo.
- No se enviaron ordenes Binance ni se modifico `ms-binance-engine`.

Resultado exacto: `PRODUCTION_CANARY_READY`.
