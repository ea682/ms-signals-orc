# Runbook - Rolling coverage SHADOW a MICRO_LIVE

## Alcance

Este rollout cambia solo la fuente del gate de coverage. No ejecuta ordenes,
no promueve allocations manualmente y no modifica eventos historicos.

## Pre-deploy

1. Confirmar que Flyway aplicara
   `V202607100001__shadow_promotion_rolling_coverage_index.sql`.
2. Medir filas y tamano de `shadow_copy_operation_event`.
3. La migracion usa `CREATE INDEX` para seguir la convencion transaccional del
   repositorio. PostgreSQL toma un lock `SHARE` durante el build: permite reads
   y bloquea writes. Programar ventana de mantenimiento si el ledger es grande.
4. Como alternativa controlada por DBA, crear el mismo indice con
   `CREATE INDEX CONCURRENTLY IF NOT EXISTS` antes de Flyway. La migracion sera
   luego un no-op por nombre. No ejecutar esta alternativa dentro de una
   transaccion.
5. No habilitar `ROLLING` antes de confirmar que el indice es valido.

## Fase 1: audit-only

Variables:

```text
SHADOW_PROMOTION_COVERAGE_ROLLING_ENABLED=true
SHADOW_COVERAGE_MODE=AUDIT
SHADOW_PROMOTION_COVERAGE_WINDOW_DAYS=14
SHADOW_PROMOTION_COVERAGE_MAX_EVENTS=500
SHADOW_PROMOTION_COVERAGE_MIN_EVENTS=100
SHADOW_PROMOTION_COVERAGE_MIN_PERCENT=95
```

En `AUDIT` se calcula rolling en una consulta batch, pero el promoter sigue
decidiendo con coverage historico. Comparar durante varias corridas:

- `coverageSourceUsed=HISTORICAL`;
- `coverageDecision` efectivo;
- `rollingCoverageDecision` contrafactual;
- latencia de `shadow.coverage.evaluated` y del job completo;
- allocations que cambian de decision.

Ejecutar las queries 1, 5, 7, 9, 10 y 11 de
`db/validation/shadow_rolling_coverage_validation.sql` desde una sesion
read-only o replica.

## Fase 2: canary rolling

Cambiar a:

```text
SHADOW_COVERAGE_MODE=ROLLING
```

Limitar temporalmente `COPY_PROMOTION_CANDIDATE_LIMIT` si se necesita un
canary pequeno. No enlazar ni promover filas a mano. Verificar:

- `rollingEvaluable >= 100` en promociones;
- `rollingCoveragePct >= 95`;
- el siguiente gate (full decision/copy guard) sigue visible;
- no hay dos allocations reales abiertas para la misma unidad;
- una allocation con `linked_live_allocation_id` aparece `YA_PROMOVIDA`;
- no hay sequential scan completo en el EXPLAIN;
- p95 de la consulta por allocation menor a 50 ms.

## Fase 3: rollout

Ampliar el candidate limit gradualmente. Monitorear:

- `SHADOW_COVERAGE_ROLLING_QUERY_FAILED`;
- `SHADOW_COVERAGE_ROLLING_INSUFFICIENT_SAMPLE`;
- `SHADOW_COVERAGE_ROLLING_BELOW_THRESHOLD`;
- promociones `MICRO_LIVE_CREATED`;
- no-ops `ALREADY_PROMOTED`;
- tiempo total de `copy.promotion.shadow_to_micro.finished`.

## Rollback

Cualquiera de estas opciones vuelve al gate historico sin redeploy:

```text
SHADOW_COVERAGE_MODE=LEGACY
```

o:

```text
SHADOW_PROMOTION_COVERAGE_ROLLING_ENABLED=false
```

Reiniciar la aplicacion para rebind de configuracion. El indice puede quedar:
no cambia la semantica ni afecta el hot path. No borrar eventos ni allocations.

## Criterio de avance

No avanzar de AUDIT a ROLLING si:

- el indice no aparece valido en `pg_index`;
- el plan hace full sequential scan del ledger;
- p95 supera 50 ms por allocation;
- hay timestamps futuros o nulos sin explicar;
- el job produce errores de query;
- no puede explicarse la diferencia historico/rolling;
- falla cualquier gate previo ya existente.
