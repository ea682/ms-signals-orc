# SDD - Promoción SHADOW -> MICRO_LIVE -> LIVE con Full Exacta

## Objetivo

Evitar que una allocation quede bloqueada indefinidamente por un copy guard summary persistido.

Una allocation shadow que ya cumplió evidencia runtime debe pedir una decisión full exacta a métricas antes de promover a MICRO_LIVE.

## Reglas SHADOW -> MICRO_LIVE

- Si faltan días/eventos/cierres/cobertura shadow, no llamar métricas full.
- Si shadow está lista y el bloqueo actual viene de summary/falta full, llamar `CopyDecisionGateway`.
- No promover si:
  - `decisionFinal=false`
  - `requiresFullSimulation=true`
  - `fullMaterialized=false`
  - `factPayloadLoaded=false`
  - `copyGuard.action != ALLOW`
  - `canMicroLive=false`
  - timeout/fallo
- Promoción idempotente: si ya existe MICRO_LIVE activa para la misma unit key, no duplicar.

## Reglas MICRO_LIVE -> LIVE

- LIVE requiere evidencia runtime MICRO_LIVE real.
- La full decision live-entry puede bloquear o permitir, pero no reemplaza:
  - `minMicroDays=7`
  - `minMicroOrders=10`
  - `maxErrorRatePct=5`
- No usar 60 días como mínimo duro. El mínimo duro de métricas es 30 días.
- El hot path LIVE no llama métricas ni DB lenta.

## Observabilidad

Eventos requeridos:

- `shadow.promotion.evidence.checked`
- `shadow.promotion.full_decision.required`
- `shadow.promotion.full_decision.result`
- `shadow.promotion.micro_live.created`
- `shadow.promotion.micro_live.noop_existing`
- `shadow.promotion.blocked`
- `micro_live.promotion.full_decision.result`

## Componentes

- `CopyDecisionGateway`: interfaz para pedir decisión full exacta.
- `MetricCopyDecisionClient`: implementación HTTP contra métricas.
- `ShadowPromotionServiceImpl`: decide promoción, no construye HTTP.
- `MicroLivePromotionServiceImpl`: mantiene evidencia runtime y consulta full live-entry solo fuera del hot path.
