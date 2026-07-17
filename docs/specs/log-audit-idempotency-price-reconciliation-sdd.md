# SDD - Replica duplicate acknowledgement and late Binance price reconciliation

## Evidence

Production logs from 2026-07-13 contain 25,475 Hyperliquid direct-ingest payload
conflicts and the same number of generic HTTP 502 errors. The second replica was
blocked before order dispatch, but the 502 response caused a Kafka fallback and
another rejection. The conflicting payloads shared wallet, symbol and source
timestamp; differences were replica-local event IDs and derived position state.

The same log set contains filled Binance orders whose average price was not
available in the immediate response. The later lookup found the authoritative
price, but close reconciliation failed because the linked copy operation was
already inactive. A non-reduce-only `FLIP` was also reconstructed as `REDUCE`,
creating the wrong ledger progression.

## Contract

### Direct-ingest dedupe

1. The first claim for an idempotency key remains the only delivery allowed to
   reach copy decision and order dispatch.
2. A second delivery with the same key and the same immutable source identity
   (wallet, symbol and source timestamp) is a replica-derived divergence. It MUST
   be acknowledged as a duplicate/no-op, increment a dedicated metric and never
   return a retryable 5xx response.
3. The divergent derived payload MUST be observable at WARN level for audit.
4. Reuse of the same key for a different immutable source identity remains a
   fail-closed idempotency conflict.
5. Signals MUST canonicalize both legacy replica keys and source-identity keys
   before claiming the guard. A Flyway migration MUST seed canonical aliases for
   historical legacy claims so an already-processed Kafka backlog cannot be
   treated as new during rolling deployment.

### Late execution evidence

1. Reconciliation MUST first resolve `copy_operation` by the durable
   `copyOperationId` linked to the dispatch intent, including inactive rows.
2. If that identifier is unavailable, the existing allocation/strategy lookup
   remains the fallback.
3. A non-reduce-only `FLIP` with no active target-side operation is `FLIP_OPEN`.
4. A non-reduce-only `ADJUST` is `OPEN` when no active operation exists and
   `INCREASE` when one exists. A reduce-only FLIP/ADJUST is an exit.
   When the intent already links a ledger event, its persisted event type is the
   authority for this classification.
5. Reconciliation of an already-applied fill MUST NOT mutate position quantity a
   second time.
6. The existing ledger event for the same dispatch progress MUST be upgraded
   monotonically with authoritative base price, price status, notional, realized
   PnL and fee, in addition to extended fill evidence.
7. A price with status `AVAILABLE` MUST replace a fallback price marked
   `PENDING_RESOLUTION`; pending data MUST never overwrite available data.
8. Authoritative late price evidence MUST also update the linked
   `copy_operation` entry/close price, including an already-inactive close,
   without changing its active state or quantity.

## Acceptance tests

1. Replica-local payload differences with equal immutable identity are a 202
   duplicate path, not an exception.
2. Equal idempotency key with a different symbol or source timestamp remains a
   conflict.
3. An already-closed operation linked by `copyOperationId` reconciles a close
   without reopening or closing it twice.
4. A non-reduce-only FLIP produces an OPEN ledger event.
5. Economic evidence upgrades pending base price/status and does not regress an
   available value.
6. The migration preserves legacy claims and creates canonical aliases without
   overwriting an alias that already exists.

## Dispatch replay fingerprint addendum

### Evidence

The same production log set contains 272 retries of one BTCUSDC rebalance
increase. The executable order remained `MARKET BUY LONG qty=0.006` with the
same clientOrderId, but the durable intent was reported as a payload conflict.
The existing intent was already `REJECTED`; repeated source delivery therefore
created alert noise and left the target adjustment blocked for hours.

### Contract

1. The request hash MUST cover fields that change the Binance order or the
   immutable first-claim policy: symbol, side, positionSide, orderType, qty,
   orderPrice, timeInForce, leverage, userMaxConcurrentPositions,
   reservePosition, reduceOnly, configureAccountSettings and clientOrderId.
2. Market-derived `referencePrice`, `requestedNotionalUsd` and
   `requestedMarginUsd` MUST NOT participate in the request hash.
3. A replay whose only changes are those derived economic fields MUST resolve
   from the existing durable intent. It MUST NOT create a conflict, reserve
   budget again or send a second order.
4. A changed quantity or executable LIMIT price under the same idempotency key
   remains a fail-closed payload conflict.
5. A terminal `REJECTED` intent remains rejected on an equivalent replay; this
   rule removes false conflicts but does not turn terminal rejection into an
   automatic resend.
6. A real payload conflict is emitted once by the durable intent store. The
   coordinator MUST NOT duplicate the same condition as a second ERROR log.

### Acceptance tests

1. Two MARKET requests with equal executable fields and different market
   reference/margin/notional produce the same request hash.
2. A quantity change produces a different request hash.
3. A LIMIT order price change produces a different request hash.
