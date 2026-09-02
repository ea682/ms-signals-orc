# ADR: Copy Policy V1 Path-Dependent Replay And Ownership

Estado: ACEPTADO  
Fecha: 2026-08-28  
Decision owner: Signals + Copy Target Core  
Alcance: HISTORICAL, SHADOW, MICRO_LIVE y LIVE

## Contexto

La política aprobada ya no puede representarse con una matriz estática de capital por
leverage. El resultado depende del orden temporal, del equity MTM, de fees, funding,
slippage, liquidaciones, filtros históricos y del estado que deja cada operación.

La fuente histórica certificada actual contiene fills y funding de la wallet origen,
pero no contiene `sourceAccountEquityUsd`, `sourceMarginUsedUsd`, leverage certificado
ni provenance de margen. Convertir esos faltantes en cero o inferir margen sin evidencia
haría que un replay aparentemente válido ocultara una indisponibilidad material.

## Decisión

1. `modules/copy-target-core` en Signals es la única autoridad del sizing de Copy Policy
   V1. `TargetPortfolioCalculator` permanece como primitiva pura y un runner incremental,
   determinista y streaming expone exactamente esa primitiva a cada step del replay.
2. El mismo calculator y las mismas reglas económicas se usan en HISTORICAL, SHADOW,
   MICRO_LIVE y LIVE. Los adapters sólo obtienen y normalizan evidencia; no recalculan
   exposición, capital, leverage, fees, funding, slippage ni liquidación.
3. La unidad de simulación es una estrategia. CORE y STATIC son cuentas, equity curves,
   rankings y ciclos de eliminación independientes. No existe allocator entre mundos ni
   renormalización de estrategias.
4. Cada mundo inicia con 100 USDT. Se reserva 10% y se autoriza 90% del remanente: el
   capital máximo inicial de sizing es 81 USDT. El compounding es defensivo:
   `sizingEquity = min(currentMtmEquity, 100)`; ganancias no elevan sizing y pérdidas sí
   lo reducen. Un único factor común escala el portfolio cuando excede capacidad.
5. `capitalCommitmentRatio = sourceMarginUsedUsd / sourceAccountEquityUsd`. El margen
   sólo es usable con provenance `EXPLICIT` o `DERIVED_CERTIFIED`; la derivación exige
   notional y leverage certificados. `UNAVAILABLE` bloquea OPEN, INCREASE y el nuevo
   lado de FLIP, pero nunca impide CLOSE/REDUCE. Los faltantes permanecen `null`, nunca
   se convierten silenciosamente en cero.
6. HISTORICAL y MICRO_LIVE usan leverage objetivo fijo x5. LIVE permite el valor elegido
   por el usuario entre x1 y x20, sujeto a los límites históricos/actuales aplicables.
   `targetNotionalUsd = targetMarginUsd * ourLeverage`.
7. Métrica es dueño y orquestador del replay económico. Cada evento se procesa
   estrictamente en orden temporal y consume únicamente evidencia conocida a `T`.
   Métrica entrega al runner de Signals el snapshot origen, target account state, equity,
   capital autorizado, leverage y reglas Binance válidas. Signals devuelve el portfolio
   target y sus reason codes, sin calcular PnL ni alterar el estado económico.
8. El contrato es request/response streaming sobre un proceso persistente para evitar un
   arranque, una llamada HTTP o una consulta SQL por evento. En FLIP, Métrica solicita y
   aplica primero el cierre (fees, slippage y PnL), recalcula el estado y recién entonces
   solicita el nuevo lado. Si queda bloqueado, la cuenta permanece flat.
9. Métrica produce una única trayectoria económica y una única equity curve por mundo.
   Las ventanas se derivan de esa curva; no se ejecuta un replay por ventana. La
   complejidad objetivo es O(N + W), sin consultas por evento ni relecturas futuras.
10. Binance es autoridad exclusiva para precio de ejecución y mark, reglas de símbolo,
   leverage brackets, fees y funding. Los snapshots históricos deben ser válidos a `T`.
   Un símbolo no ejecutable omite sólo esa operación y no redistribuye capital.
11. El transcript de decisiones de sizing y el replay económico de Métrica son inmutables,
    versionados y schema-qualified bajo `futuros_operaciones`, incluyendo policy version,
    market-data digest, input digest, coverage, reason codes y contadores. HISTORICAL
    nunca reserva budget ni envía órdenes.
12. Métrica deriva ventanas/rankings y aplica gates de negocio ya existentes. No porta
    fórmulas de sizing a TypeScript. Si STATIC no tiene contrato/gate existente, reporta
    `UNAVAILABLE`; no inventa umbrales.
13. ETL continúa siendo autoridad de hechos master y provenance. Propaga campos con su
    evidencia y conserva indisponibilidad. No decide sizing, ranking ni eliminación.
14. La comparación cross-mode exige exactamente la misma policy version y market-data
    digest. Un mismatch, dato ausente, digest incompleto o cobertura insuficiente falla
    cerrado con reason code estable.

## Contrato mínimo de evidencia

Un replay económico certificable requiere, con validez temporal:

- equity de la cuenta origen;
- margen explícito o notional + leverage ambos certificados;
- fills/órdenes y timestamps suficientes para execution price y shortfall;
- filtros de símbolo, estado de trading y límites de leverage;
- mark price, funding rate/interval y leverage brackets;
- fee real de cuenta o fallback oficial VIP0, con ambas piernas de FLIP;
- metadata de posición/margin mode ONE_WAY + CROSSED.

La ausencia de cualquiera de estos campos no autoriza defaults económicos. Se persiste
`UNAVAILABLE` y se bloquea únicamente la acción que necesitaría la evidencia.

## Consecuencias

- El runner streaming no es un segundo replay: no calcula equity, fees, funding,
  slippage, liquidación, gates ni rankings. Es únicamente la frontera ejecutable del
  calculator canónico de Signals.
- La matriz `capital x leverage` puede conservarse como artefacto diagnóstico, pero no
  certifica la política path-dependent ni puede alimentar gates económicos finales.
- Los resultados deben explicar por qué una operación fue ejecutada, omitida o bloqueada.
- Los índices temporales de un B2B pueden acelerar descubrimiento, pero no certifican
  performance. La prueba final se ejecuta sin ayudas no productivas.
- La evidencia histórica actual de la target wallet no basta para habilitar entradas;
  un replay sin operaciones por faltantes se clasifica `BLOCKED_INPUT_UNAVAILABLE`, no
  como estrategia económica válida de cero trades.

## ADRs reemplazados parcialmente

Este ADR reemplaza de `canonical-copy-sizing-and-simulation-ownership.md`:

- la afirmación de que leverage nunca multiplica target notional;
- la matriz estática como simulación económica suficiente;
- cualquier inferencia implícita de margen desde notional/leverage no certificados.

Se conservan de aquel ADR la autoridad única de Signals, la prohibición de duplicar
fórmulas en Métrica y el fail-closed ante evidencia ausente/stale/inconsistente.

## Rollback

Deshabilitar el runner V1 y la lectura de sus nuevos resultados sin borrar evidencia.
HISTORICAL/SHADOW/MICRO_LIVE/LIVE permanecen sin nuevas entradas; CLOSE/REDUCE siguen
habilitados por el camino de salida seguro. No se vuelve a la matriz estática como
autoridad económica y no se rellenan faltantes con defaults.
