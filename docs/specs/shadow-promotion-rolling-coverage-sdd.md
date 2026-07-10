# SDD - Rolling coverage para promocion SHADOW a MICRO_LIVE

## 1. Problema

`ShadowPromotionServiceImpl` usa contadores acumulados de la ultima
`shadow_wallet_profile_validation`. Por eso, errores antiguos permanecen en el
denominador indefinidamente y pueden bloquear una allocation aunque su
comportamiento reciente sea correcto.

El cambio reemplaza solamente el gate de coverage de la promocion
`SHADOW -> MICRO_LIVE`. No cambia copy guard, full decision, actividad, dias,
posiciones cerradas, PnL, drawdown, capital, simbolos, idempotencia ni gates de
`MICRO_LIVE -> LIVE`.

## 2. Unidad de evaluacion

La clave primaria de evaluacion es `shadow_allocation_id`.

La identidad funcional equivalente es:

`wallet_id + copy_strategy_code + scope_type + scope_value`

Nunca se agregan eventos solo por wallet. Dos allocations de la misma wallet
se consultan, calculan y auditan de forma independiente.

## 3. Caracterizacion legacy

Los contadores historicos provienen de la validacion asociada al
`wallet_profile_id` de la allocation:

- `SIMULATED`: exito SHADOW actual.
- `RECORDED`: alias legacy de exito.
- `SKIPPED`: fallo evaluable.
- `ERROR`: fallo evaluable.
- `DUPLICATE`: diagnostico no evaluable; no entra al denominador.
- otras decisiones o `decision IS NULL`: no evaluables.

La implementacion legacy usa:

```text
historicalSuccessful = max(recordedEvents, simulatedEvents)
historicalCountableFailures = skippedEvents + errorEvents
historicalEvaluable = historicalSuccessful + historicalCountableFailures
historicalCoveragePct = historicalSuccessful / historicalEvaluable * 100
```

Si el snapshot no tiene exitos, el comportamiento existente consulta el total
de eventos de la allocation como fallback. Este fallback se conserva solo para
el gate legacy y para compatibilidad de auditoria; el rolling nunca cuenta
decisiones no evaluables.

## 4. Ventana rolling

Defaults:

- `windowDays = 14`
- `maxEvents = 500`
- `minEvaluableEvents = 100`
- `minimumPercent = 95`
- `rollingEnabled = true`
- `mode = ROLLING`

Para cada `shadow_allocation_id`:

1. usar exclusivamente `event_time`;
2. incluir `event_time >= windowStart`;
3. incluir `event_time <= windowEnd` para excluir eventos futuros;
4. filtrar primero decisiones evaluables;
5. ordenar por `event_time DESC, id_event DESC`;
6. limitar a los `maxEvents` mas recientes;
7. aplicar la misma compatibilidad de exito que legacy.

```text
rollingSuccessful = max(rollingRecorded, rollingSimulated)
rollingCountableFailures = rollingSkipped + rollingErrors
rollingEvaluable = rollingSuccessful + rollingCountableFailures
rollingCoveragePct = rollingSuccessful / rollingEvaluable * 100
```

`id_event DESC` hace determinista el limite cuando dos eventos comparten
`event_time`. La frontera inferior es inclusiva. Todas las fechas se construyen
y comparan en UTC.

`event_time IS NULL` se excluye. No se usa `date_creation` silenciosamente. La
tabla y la entidad actuales declaran `event_time NOT NULL`; cualquier nulo
legacy se reporta mediante la query post-deploy de calidad de datos.

## 5. Decision de coverage

Decisiones del calculo rolling:

- cero evaluables: `NEEDS_MORE_DATA`,
  `SHADOW_COVERAGE_ROLLING_NO_EVENTS`;
- consulta fallida: `COVERAGE_NOT_READY`,
  `SHADOW_COVERAGE_ROLLING_QUERY_FAILED`;
- `0 < rollingEvaluable < minEvaluableEvents`: `NEEDS_MORE_DATA`,
  `SHADOW_COVERAGE_ROLLING_INSUFFICIENT_SAMPLE`;
- muestra suficiente y porcentaje menor al threshold: `COVERAGE_NOT_READY`,
  `SHADOW_COVERAGE_ROLLING_BELOW_THRESHOLD`;
- muestra suficiente y porcentaje mayor o igual al threshold:
  `COVERAGE_READY`, `SHADOW_COVERAGE_ROLLING_READY`.

Una excepcion de PostgreSQL no habilita fallback permisivo cuando el modo
efectivo es `ROLLING`.

## 6. Modos y rollback

`rollingEnabled=false` fuerza `LEGACY`, independientemente de `mode`.

- `LEGACY`: decide con `historicalCoveragePct` y
  `copy.promotion.min-shadow-coverage-pct`. No consulta el ledger rolling.
- `AUDIT`: calcula ambos, decide con historical y registra el resultado que
  habria producido rolling.
- `ROLLING`: decide con rolling y `minimumPercent`; historical queda solo para
  auditoria.

`coverageSourceUsed` vale `HISTORICAL` en `LEGACY/AUDIT` y `ROLLING` en
`ROLLING`. En modo legacy se registra `SHADOW_COVERAGE_LEGACY_USED`.

## 7. Configuracion

Convencion del servicio:

```yaml
copy:
  promotion:
    coverage:
      rolling-enabled: true
      mode: ROLLING
      window-days: 14
      max-events: 500
      min-evaluable-events: 100
      minimum-percent: 95
```

Variables:

- `SHADOW_PROMOTION_COVERAGE_ROLLING_ENABLED`
- `SHADOW_COVERAGE_MODE`
- `SHADOW_PROMOTION_COVERAGE_WINDOW_DAYS`
- `SHADOW_PROMOTION_COVERAGE_MAX_EVENTS`
- `SHADOW_PROMOTION_COVERAGE_MIN_EVENTS`
- `SHADOW_PROMOTION_COVERAGE_MIN_PERCENT`

El binding debe rechazar startup si dias/eventos son menores a uno, si
`minEvaluableEvents > maxEvents`, si el porcentaje queda fuera de `[0,100]` o
si el modo no es valido.

## 8. Consulta y performance

El job obtiene todos los candidatos y ejecuta una sola consulta rolling batch.
La consulta:

- filtra por la lista de `shadow_allocation_id`;
- filtra la ventana UTC y decisiones evaluables en PostgreSQL;
- aplica `row_number()` por allocation y limita antes de agregar;
- devuelve cero o una fila por allocation.

No se carga historia completa en Java y no se agrega una consulta del ledger
por candidato. El hot path de copia no consume este servicio.

Indice requerido:

```text
(shadow_allocation_id, event_time DESC, id_event DESC) INCLUDE (decision)
```

Puede ser parcial para decisiones evaluables. El objetivo operativo es p95
menor a 50 ms por allocation y ausencia de sequential scan completo. El plan
real debe verificarse post-deploy con `EXPLAIN (ANALYZE, BUFFERS)` sobre una
replica o sesion read-only.

## 9. Snapshot y auditoria

Cada evaluacion expone en `copy_promotion_audit.reason_details`:

- `historicalSimulatedEvents`
- `historicalSkippedEvents`
- `historicalErrorEvents`
- `historicalCoveragePct`
- `rollingSimulatedEvents`
- `rollingSkippedEvents`
- `rollingErrorEvents`
- `rollingEvaluableEvents`
- `rollingCoveragePct`
- `rollingWindowDays`
- `rollingMaxEvents`
- `rollingMinEvents`
- `rollingWindowStart`
- `rollingWindowEnd`
- `coverageThresholdPct`
- `coverageSourceUsed`
- `coverageDecision`
- `coverageReasonCode`

Los logs `shadow.coverage.evaluated`, `shadow.promotion.coverage_blocked` y
`shadow.promotion.coverage_ready` usan los mismos valores sin payloads ni
secretos.

Una allocation con `linked_live_allocation_id IS NOT NULL` se reporta como
`YA_PROMOVIDA`; no vuelve a ser candidata ni se marca como bloqueada por
coverage historico.

## 10. Invariantes

- No borrar, reescribir ni reclasificar eventos SHADOW.
- No reinterpretar reason codes de posiciones heredadas.
- No promover manualmente allocations.
- No emitir ordenes reales.
- Mantener idempotencia por allocation real ya existente y constraint unico.
- Rolling aprobado solo permite continuar con los otros gates; nunca promueve
  por si mismo.
- Un copy guard o full decision bloqueado sigue bloqueando.
- El gate de posiciones cerradas sigue aplicando.

## 11. Tests de aceptacion

Los tests cubren: historico malo/reciente bueno, historico bueno/reciente malo,
99 eventos, exactamente 100 al threshold, frontera inclusiva, eventos antiguos,
limite de 500, orden descendente, timestamps futuros, allocations independientes,
SKIPPED/ERROR en denominador, DUPLICATE excluido, cero eventos, fallo de query,
modo legacy, auditoria historica, inmutabilidad del ledger, idempotencia y
otros gates.

## 12. Rollout

1. desplegar indice y codigo con `SHADOW_COVERAGE_MODE=AUDIT`;
2. comparar historical contra rolling y revisar query latency/plan;
3. confirmar que los cambios de decision son esperados;
4. activar un canary con `SHADOW_COVERAGE_MODE=ROLLING`;
5. revisar promociones y auditoria;
6. ampliar rollout;
7. ante regresion usar `SHADOW_PROMOTION_COVERAGE_ROLLING_ENABLED=false` o
   `SHADOW_COVERAGE_MODE=LEGACY`.

Mejora futura fuera de alcance: distinguir `WAITING_FOR_FRESH_OPEN` para
posiciones heredadas de una perdida real de OPEN.
