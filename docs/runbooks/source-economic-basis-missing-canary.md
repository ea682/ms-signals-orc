# Canary preparado: SOURCE_ECONOMIC_BASIS_MISSING

Estado: **PREPARADO, NO EJECUTADO**.

## Alcance

- Una sola wallet:
  `0xf5d81a135f756ca16544e53c20fc20643ec3ad53`.
- Ventana máxima: 10 minutos.
- Solo `platform=hyperliquid` y eventos `REDUCE`/`CLOSE`.
- Sin replay histórico, migraciones, cambios en Sentinel ni lectura de
  producción durante la preparación.

Antes de iniciar, registrar el commit desplegado, la hora UTC y los valores
acumulados de:

- `signals_economic_basis_complete_total`
- `signals_economic_basis_missing_total`
- `signals_economic_basis_ambiguous_total`
- `signals_movement_event_published_total`
- `signals_movement_event_missing_price_total`
- `signals_movement_event_missing_quantity_total`
- `signals_movement_event_missing_source_identity_total`
- cuarentenas ETL con `reasonCode=SOURCE_ECONOMIC_BASIS_MISSING`
- eventos ETL procesados
- efectos económicos duplicados

Los IDs concretos se correlacionan desde los logs estructurados por
`sourceEventId`, `sourceSequence` y `sourceEconomicFingerprint`; nunca se usan
como labels de métricas.

## Gate

Para el subconjunto con `economicBasisStatus=COMPLETE`:

```text
completeBasisEvents = processedEvents
SOURCE_ECONOMIC_BASIS_MISSING = 0
silentLoss = 0
duplicateEconomicEffects = 0
unresolvedIdentities = 0
```

Los eventos `MISSING_AUTHORITATIVE_FILL` o `AMBIGUOUS` no entran al numerador
de completos y deben conservar su evidencia durable y su disposición
fail-closed.

Abortar inmediatamente si aparece cualquiera de estas condiciones:

- un `USER_FILL` completo pierde cantidad, precio, identidad, secuencia o
  fingerprint;
- un evento incompleto se procesa con un valor económico sintetizado;
- crece cualquier contador de campo faltante para `economicBasisStatus=COMPLETE`;
- hay más de un movimiento o efecto económico por `sourceEventId`;
- el publicador o consumidor acumula backlog durante dos intervalos de
  observación consecutivos.

## Rollback

1. Detener la ventana del canary.
2. Volver al artefacto anterior de `ms-signals-orc`; no hay rollback de schema
   porque este cambio no agrega migraciones.
3. Conservar sin alterar los movimientos, outbox y cuarentenas producidos
   durante la ventana.
4. Exportar la lista de `sourceEventId` afectados para reconciliación offline.
5. No ejecutar replay hasta demostrar identidad, cantidad, precio, timestamp,
   orden y fingerprint consistentes.

