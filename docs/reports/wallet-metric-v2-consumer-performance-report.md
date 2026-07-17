# Wallet Metric V2 - Signals Consumer Performance Report

Fecha de medicion: 2026-07-13

## Diseno de latencia

El hot path de un delta no realiza HTTP ni full simulation. Un refresh coordinado fuera de
banda obtiene summary, full y copy guard una vez por ciclo, valida el conjunto completo y
hace un `AtomicReference` swap. La decision por operacion consulta mapas inmutables por
`strategyKey`.

## Mediciones

Entorno local Java 21, datos sinteticos en memoria, 100 lotes y 100.000 evaluaciones:

| Operacion hot path | Min | p50 | p95 | Max | Llamadas remotas |
|---|---:|---:|---:|---:|---:|
| Lookup + validacion FULL/guard V2 | 2.4361 us | 3.7797 us | 21.1215 us | 27.7480 us | 0 |

El benchmark de refresh sintetico comprobo exactamente tres llamadas remotas por ciclo:
una summary, una full y una copy guard. No hubo llamada por candidato ni por delta.

La integracion HTTP real contra Metrica Cuenta recien levantada produjo:

| Etapa cold/first refresh | Tiempo | Resultado |
|---|---:|---|
| summary | 258 ms | 5 strategy units |
| full | 51 ms | 5 strategy units |
| copy guard | 25 ms | 5 strategy units, 13 ventanas |
| total coordinado | 339 ms | Snapshot coherente |

Estos tiempos incluyen JIT/cold runtime, HTTP loopback, parseo y validacion. El hot path
medido arriba no hereda los 339 ms.

## Antes y despues

No habia un consumidor V2 integrado equivalente antes de este cambio, por lo que no se
presenta un numero historico inventado. La mejora demostrable es estructural:

| Antes/riesgo | Despues |
|---|---|
| Consulta/decision con identidad parcial | Map por `strategyKey` completo |
| Riesgo de usar respuesta vacia como todos elegibles | Fail-closed explicito |
| Summary podia confundirse con decision final | Summary nunca habilita SHADOW |
| Full potencialmente solicitado al decidir | Full solo en refresh fuera de banda |
| Cache por wallet mezclaba scopes/generaciones | Cache atomica por strategy + generation |
| Nodos podian reemplazar snapshots concurrentemente | Advisory transaction lock en persistencia |

## Cache y disponibilidad

- Summary refresh: default 2 minutos.
- Full refresh: default 10 minutos.
- Guard refresh: default 2 minutos.
- Staleness maxima para nueva exposicion: default 10 minutos.
- Refresh duplicado en el mismo nodo se une por singleflight.
- Si un refresh falla, se conserva el snapshot anterior pero OPEN/INCREASE fallan cerrado
  cuando expira.
- CLOSE/REDUCE usan el estado local y siguen permitidos ante falla de Metrica Cuenta.
- Un cambio de generacion o del conjunto de candidatos invalida la combinacion anterior.

## Limites de la medicion

El benchmark no incluye Kafka, Hyperliquid, Binance, red externa, ejecucion de orden,
contencion productiva ni latencia de disco remoto. Tampoco mide una orden real: por
restriccion de seguridad no se emitieron operaciones. Debe repetirse en staging con
cardinalidad productiva y metricas p50/p95/p99 antes del cutover.
