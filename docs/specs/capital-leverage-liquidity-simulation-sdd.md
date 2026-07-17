# SDD: Capital, Leverage And Liquidity Simulation

Estado: APROBADO PARA IMPLEMENTACION  
Fecha: 2026-07-14  
Repositorio: `ms-signals-orc`

## 1. Fuente canonica

El modulo interno `modules/copy-target-core` calcula sizing determinista. La carpeta
externa no se usa. La simulacion es cold-path y persiste un read model; nunca corre en el
hot path ni envia ordenes.

## 2. Matriz obligatoria

Capital: 100, 250, 500, 1000, 5000, 10000, 50000, 100000, 250000, 500000, 1000000.
Leverage: x5, x10, x15, x20. Total exacto: 44 escenarios. Orden estable capital ascendente
y leverage ascendente. MICRO corresponde a 100/x5 con la misma calculadora.

Cada escenario incluye sizing/coverage, gross/net, fees, funding, slippage, rounding,
minNotional/missed movement, venue/latency, turnover, drawdown, PF/expectancy, fill,
liquidation, margin, capacity, confidence, status/reasons y versions. Si un insumo falta,
el campo es NULL con status/reason; no se inventa economics.

## 3. Costes y atribucion

`net = gross - fees - funding - slippage - rounding - missed - venue - latency`.
Los source events y model versions deben coincidir. Capture ratio usa source replicable
positivo; denominador no positivo produce N/A. Fee/funding divergences se modelan por
venue/quote/time; una tarifa estimada reduce confidence.

## 4. Latencia y venue

Grid: 0.5, 1, 2, 5, 10, 30 segundos. Outputs: entry/exit/total latency loss, lag
sensitivity, break-even latency y edge half-life. Se usan precios historicos Binance
cuando existan. Si faltan, reason `HISTORICAL_BINANCE_PRICE_MISSING`, prior conservador
explicito y penalizacion Evidence; nunca precio actual como si fuera historico.

Venue basis compara Hyperliquid source price/mark con Binance executable/mark en la misma
ventana. Timestamp o simbolo no alineable produce UNKNOWN.

## 5. Liquidez y capacidad

Regimenes: normal/median/p25/p10/stress/crisis/emergency. Estrategias SINGLE_MARKET,
FRAGMENTED, TWAP y PARTICIPATION_CAP. Order-book simulation soporta VWAP, depth, partial
fill, staleness, concurrency y follower aggregate notional.

Outputs: normal, stress y emergency exit capacity, fill ratio, impact/slippage y strategy.
No book/stale book produce UNKNOWN/NO_BOOK; no se asume fill completo.

## 6. Liquidacion y margen

Inputs: leverage brackets, maintenance margin, mark, isolated/cross, balance, unrealized,
funding, posiciones simultaneas, gap/delayed close y ADL evidence. Outputs: survival rate,
minimum distance, margin stress p95/p99 y leverage robustness.

Cuando faltan brackets/mark/margin mode, riesgo de liquidacion es UNKNOWN y el escenario
no puede ser elegido como best band. El leverage no modifica target notional.

## 7. Worker, persistencia e idempotencia

El worker procesa 44 escenarios, cursor 0..44 y indices 0..43, con pause/resume/retry.
Una migracion aditiva amplia constraints anteriores de 40 y agrega economics/risk JSON
versionado sin reescribir migraciones aplicadas. Upsert por job+scenarioIndex; input hash
distinto bajo la misma identidad es conflicto.

Jobs de liquidez se crean en batch para bandas configuradas. La completion solo ocurre
con los 44 escenarios persistidos. Cache/read keys incluyen strategyKey, generation,
capital/leverage, calculator/policy/model versions y mode.

`copy_simulation_job_v3.execution_mode` admite SHADOW, MICRO_LIVE y LIVE porque describe
el origen de evidencia fria; no habilita ejecucion ni fallback. El constraint debe rechazar
cualquier otro modo y las flags de promocion monetaria permanecen deshabilitadas.

El contrato de lectura se materializa en Metricas mediante
`GET /operaciones/metrica/v2/simulation-matrix?walletId&strategyCode&scopeType&scopeValue&generationId`.
La consulta usa un unico batch para FULL, selecciona el ultimo job COMPLETED exacto y
exige 44 indices distintos. Tabla inexistente, job ausente, generacion distinta,
duplicados o menos de 44 escenarios producen `available=false`, status UNKNOWN/INCOMPLETE
y reason codes; no producen una matriz parcial marcada como completa.

## 8. Rendimiento y observabilidad

Contadores scenario total/unknown/liquidation/latency-break-even/liquidity no-book; timer
por matrix/scenario/persistence. Logs incluyen strategy identity, scenario, versions,
status/reason y elapsedMs. Yield configurable protege hot work.

Medir matrix compute, scenario upsert y liquidity simulation. Objetivo full20<30s se
evalua end-to-end; hot path no lee/calcula matriz.

## 9. Pruebas

Tests del core: 44 exactos, todas las bandas, x5/x10/x15/x20, request inmutable, MICRO
igual a calculo directo, leverage no infla notional, equity missing, round-down,
minNotional y common scaling. Signals: cursor 44, resume, idempotencia, migration contract,
economics UNKNOWN, latency grid, historical-price-missing, order book/partial/concurrency,
stress/emergency capacity y liquidation inputs missing.

## 10. Cutover/rollback

Instalar primero el modulo interno, aplicar migracion aditiva, recalcular cold scenarios y
comparar 40 previos con sus equivalentes dentro de 44. Rollback detiene worker/lectura de
nueva version; no borra escenarios ni afecta ejecucion real.

## 11. Aceptacion

Hay exactamente 44 escenarios versionados, todos los campos tienen valor o status/reason,
no hay NaN/Infinity, ninguna simulacion habilita dinero y no existe sizing paralelo usado
para decisiones.
