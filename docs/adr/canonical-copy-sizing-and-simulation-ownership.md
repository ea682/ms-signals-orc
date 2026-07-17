# ADR: Canonical Copy Sizing And Simulation Ownership

Estado: ACEPTADO  
Fecha: 2026-07-14

## Contexto

El repositorio contiene el modulo real `modules/copy-target-core`. Signals lo consume
como `com.apunto:copy-target-core:3.0.0-SNAPSHOT`. Metricas tambien posee un simulador
TypeScript historico con reglas de margen por posicion; mantener ambos como autoridad
permite resultados incompatibles.

Existe otra carpeta `C:\Proyectos\Proyecto-copy-trading\copy-target-core`, pero no es la
fuente de verdad y queda explicitamente fuera de este cambio.

## Decision

1. El unico sizing canonico es el modulo interno de Signals.
2. `TargetPortfolioCalculator` se usa sin diferencias en replay, simulation, SHADOW,
   MICRO_LIVE y LIVE.
3. El leverage cambia margen/riesgo, nunca multiplica target notional.
4. La matriz capital x leverage se calcula en Signals y se persiste versionada.
5. Metricas lee la matriz persistida; no recalcula sizing en TypeScript.
6. El simulador TypeScript queda diagnostico/deprecated hasta retirar consumidores.
7. Todo input incluye equity autoritativa, version de snapshot, filtros Binance y
   calculator/policy/symbol versions.
8. Missing/stale/invalid equity bloquea OPEN/INCREASE/nuevo lado FLIP, nunca CLOSE/REDUCE.
9. MICRO_LIVE y LIVE permanecen deshabilitados por default.
10. El read model `copy_simulation_job_v3`/`copy_capital_leverage_simulation_v3`
    se comparte en PostgreSQL bajo `futuros_operaciones`. Metricas lo consulta por lote,
    solo para jobs `COMPLETED` con coincidencia exacta de `strategyKey+generationId`.
11. El contrato publico de lectura es
    `GET /operaciones/metrica/v2/simulation-matrix`; una matriz ausente, incompleta o de
    otra generacion devuelve UNKNOWN con reason codes y nunca se sustituye por sizing TS.

## Consecuencias

- Un cambio matematico exige tests del core, reinstalar el modulo local y contract tests.
- Summary de Metricas no simula. Full puede combinar hechos con escenarios persistidos.
- Escenarios sin evidencia historica explican UNKNOWN y fallan cerrado.
- No se introduce una dependencia circular Metrics -> Signals en hot path: la matriz es
  un read model asincrono/persistido.
- La lectura de matriz no crea jobs, no llama Binance y no cambia allocations.

## Alternativas rechazadas

- Portar las formulas a TypeScript: duplicacion y drift.
- Hacer HTTP sincrono Signals durante `/joyas`: latencia/acoplamiento y riesgo de ciclo.
- Usar la carpeta externa: contradice la fuente real indicada y no es parte del repo.

## Rollback

Deshabilitar lectura de matriz V3 y volver a modo COMPARE. No borrar escenarios ni
evidencia. El sizing real continua en core; CLOSE/REDUCE permanecen operativos.
