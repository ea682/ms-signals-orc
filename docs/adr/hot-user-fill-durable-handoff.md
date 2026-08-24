# ADR: Durable HOT USER_FILL handoff

Estado: ACEPTADO  
Fecha: 2026-08-24

## Contexto

Signals recibe movimientos Hyperliquid por HTTP y los entrega a Kafka para que
`ms-wallet-metric-etl` materialice Canonical, Fact y Cycle. Un `202 Accepted` no puede
representar solamente una entrada en memoria: un crash entre la respuesta y la escritura
durable produciria una perdida silenciosa del camino HOT/LIVE.

El transporte puede repetir una identidad por timeout, perdida del ACK, restart o dos
replicas concurrentes. Esas repeticiones deben conservar entrega al menos una vez sin
duplicar el efecto economico. Un mismo `sourceIdentity` con economia contradictoria no es
un retry valido y debe fallar cerrado.

## Decision

1. Todo `USER_FILL` autoritativo usa siempre el guard durable; el cache local no puede
   resolver ni omitir su idempotencia.
2. Signals responde `202` solamente despues de que inbox, ledger y outbox requeridos
   queden confirmados por una unica transaccion PostgreSQL.
3. Un claim `PROCESSING` vigente no es un exito: el caller recibe un resultado reintentable.
4. Tras vencer el lease, otra replica puede reclamar el trabajo; fencing y constraints
   unicos impiden dos ledger u outbox logicos para la misma identidad.
5. La publicacion Kafka es asincrona y al menos una vez. El outbox durable sobrevive a
   caidas de proceso y de Kafka; la unicidad economica se aplica en almacenamiento.
6. Una repeticion con el mismo payload es un no-op durable. Una repeticion con payload
   economico distinto se registra como conflicto y falla cerrado.
7. `POSITION_DELTA` permanece audit-only: nunca se promueve a `USER_FILL`, Fact o Cycle.
8. El contrato operacional incluye gauges de claims vencidos, backlog y edad del outbox,
   ademas de contadores de ingreso, durabilidad, publicacion, duplicados y conflictos.
9. La certificacion usa los JAR reales con PostgreSQL 16 y Kafka locales, e incluye crash,
   restart, outage, replay y dos replicas.

## Invariantes

- `SUCCESSFUL_ACK => DURABLE_RECOVERABILITY`.
- `PROCESSED => LEDGER && REQUIRED_OUTBOX`.
- `PERMANENT_DATA_LOSS = 0`.
- `DUPLICATE_ECONOMIC_EFFECT = 0`.
- `CONTRADICTORY_ECONOMICS_FAIL_CLOSED = true`.

## Consecuencias

- El camino autoritativo puede pagar la latencia de una transaccion antes del ACK.
- PostgreSQL no disponible implica ausencia de exito, nunca degradacion a memoria.
- Kafka no disponible acumula outbox durable y activa alertas; no crea otro ledger.
- Los consumidores siguen obligados a ser idempotentes porque el transporte es al menos
  una vez.

## Alternativas rechazadas

- ACK antes de persistir: permite perdida silenciosa ante crash.
- Dedupe solamente en memoria: no coordina replicas ni sobrevive restart.
- Publicacion Kafka dentro de la transaccion: acopla dos sistemas sin atomicidad comun.
- Aceptar payload contradictorio como duplicado: ocultaria una divergencia economica.

## Rollback

No se admite rollback a ACK en memoria para `USER_FILL`. Ante una regresion se detiene la
adopcion del artefacto y se conserva el ultimo JAR certificado; no se borran inbox, ledger,
outbox ni evidencia de conflictos.
