# Adaptive Copy Strategy Capital Engine - Implementation Report

Fecha: 2026-07-14  
Estado local: implementado y probado  
MICRO_LIVE/LIVE: deshabilitados por defecto

## 1. Ownership y fuente canonica

Signals es propietario de la decision operativa y del sizing. La unica fuente de
matematica de cartera es:

```text
ms-signals-orc/modules/copy-target-core
```

La carpeta externa `C:\Proyectos\Proyecto-copy-trading\copy-target-core` no fue usada
ni modificada. `TargetPortfolioCalculator` es compartido por simulacion, SHADOW,
MICRO_LIVE y LIVE; no existe una formula paralela por modo.

## 2. Formula y reglas

```text
sourceExposureRatio = abs(sourcePositionNotionalUsd) / sourceAccountEquityUsd
targetNotionalUsd = targetAllocatedCapitalUsd * sourceExposureRatio * capitalMultiplier
targetRequiredMarginUsd = targetNotionalUsd / targetLeverage
```

El leverage no multiplica `targetNotionalUsd`; solo cambia margen y riesgo. Quantity y
notional se redondean hacia abajo. Una pierna bajo minNotional se omite, nunca se eleva
artificialmente. Si el portfolio no cabe se aplica common scaling.

Equity origen missing/stale/invalid bloquea OPEN, INCREASE y el lado nuevo de FLIP.
CLOSE y REDUCE siguen permitidos. FLIP cierra primero y autoriza el nuevo lado como una
decision separada.

## 3. Decision adaptativa

`AdaptiveCapitalDecisionEngine` combina:

- estado operativo y capitalMultiplier en escalones de 0.25;
- decision full/final y elegibilidad SHADOW;
- freshness de metricas y equity;
- copy guard;
- EvidenceScore;
- capacity frente a capital solicitado;
- feature flags MICRO_LIVE/LIVE.

Estados PAUSED/RETIRED bloquean exposicion nueva. WATCH exige persistencia de salud y
recupera como maximo un escalon por ciclo. Capacity UNKNOWN falla cerrado para dinero
real, pero permite analizar SHADOW con reason code. CLOSE/REDUCE siempre son
desapalancamiento y no se bloquean por una falla de metricas.

`CopyGlobalCapitalAllocator` valida balance disponible, margen usado, reserva de
seguridad y margen solicitado antes de reservar. `PostgresCopyDispatchIntentStore`
serializa la reserva MICRO_LIVE y rechaza overbooking.

## 4. Consumo de Metrics V2

`MetricCopyDecisionGateway` exige respuesta exacta por strategyKey, FULL, generationId
y contrato institucional valido. `MetricV2SnapshotStore` mantiene caches separados por
strategy+generation y rechaza una generation distinta a la decision exacta.

Summary nunca abre. Full puede autorizar SHADOW, pero no MICRO_LIVE/LIVE. Una falla del
servicio de metricas bloquea exposicion nueva y conserva salida. El cambio de generation
invalida snapshots incompatibles.

## 5. Idempotencia y persistencia

La identidad de dispatch incluye strategyKey, generationId, sourceEventId, allocation,
action, executionMode y fingerprint del payload. Duplicados identicos son no-op;
payload divergente no reutiliza silenciosamente una orden previa. Estados terminales no
retroceden y un FLIP duplicado no vuelve a abrir.

La migracion `V202607140002__copy_economic_calibration_lineage_v3.sql` agrega lineage
de generation/strategy a allocations, dispatch y operation events e indices de lectura.

## 6. Seguridad

- `MICRO_LIVE_ENABLED=false` por defecto.
- `LIVE_ENABLED=false` por defecto.
- Ningun endpoint o worker activa estrategias automaticamente.
- No existe fallback LIVE a SHADOW/simulacion.
- No se realizaron ordenes reales ni llamadas privadas capaces de operar.
- El capital USD 100 usado por el escenario SHADOW no reemplaza configuracion LIVE.

## 7. Pruebas y build

El modulo interno `copy-target-core` ejecuto 36 tests, 0 fallos, y se instalo como JAR
local. Signals completo ejecuto 640 tests, 0 fallos/errors y 4 skips; las integraciones
PostgreSQL que Docker no pudo iniciar se ejecutaron separadamente contra PostgreSQL
16.14: locks/distribution 6 tests y array persistence 1 test, todos verdes.

Casos cubiertos: formula proporcional, leverage no multiplica notional, round down,
minNotional, common scaling, equity gates, FLIP, decision adaptativa, recovery, capacity,
idempotencia, payload conflict, budget reservation, cache generation-aware, contratos y
fail-closed.

`mvnw clean package` y empaquetado del JAR finalizaron GREEN con Java 21. Micrometer
expone warning/reduce/pause/recovery, correlation/capacity limit y el contador canonico
de cambio de generation; el nombre legacy se conserva como alias temporal.

## 8. Riesgo residual

La correlacion economica entre estrategias no puede calcularse sin series sincronizadas
de retornos; mientras falten, debe permanecer UNKNOWN y no puede mejorar una asignacion.
Capacity, liquidation, basis o liquidity sin evidencia historica tambien permanecen
UNKNOWN. No se declara MICRO_LIVE/LIVE `REAL_VALIDATED` sin fills reales.

La siguiente accion operativa es recolectar SHADOW ejecutable, revisar calibration y
hacer una promocion MICRO_LIVE manual y controlada; no cambiar flags globales durante
el cutover inicial.
