# SDD - Copy Trading Proportional Portfolio V3

Status: `SPEC_APPROVED_FOR_IMPLEMENTATION`

Version: `3.0.0`

Date: 2026-07-13

This specification supersedes every runtime rule that assigns a fixed USD 20
margin to an operation, imposes a global five-position limit, derives exposure
from source margin, or falls back to USD 1,000 source equity.

## 1. Scope And Ownership

The economic identity is:

```text
walletId + strategyCode + strategyVersion + scopeType + scopeValue
```

Component ownership:

| Component | Responsibility |
| --- | --- |
| `ms-sentinel-hyperliquid` | Publish an internally consistent source portfolio and authoritative equity observation. |
| `ms-signals-orc` | Own `TargetPortfolioCalculator`, lifecycle decisions, real execution intents and executable shadow. |
| `ms-binance-engine` | Enforce exchange contracts and return authoritative execution evidence. |
| `ms-wallet-metric-etl` | Build economic cycles, simulation facts, calibration and certification read models. |
| `ms-metrica-cuenta` | Read canonical facts; it must not run an independent sizing implementation. |

No test in this change may enable real dispatch. No order may be submitted by a
unit, integration, migration, replay or simulation test.

## 2. Global Invariants

1. `sourceExposureRatio = sourcePositionNotionalUsd / sourceAccountEquityUsd`.
2. `targetNotionalUsd = targetAllocatedCapitalUsd * sourceExposureRatio`.
3. `targetRequiredMarginUsd = targetNotionalUsd / targetLeverage`.
4. MICRO_LIVE uses USD 100 total capital and 5x leverage unless an explicit,
   versioned future policy replaces it.
5. There is no fixed amount per operation.
6. There is no global maximum position count. A nullable user limit is allowed.
7. `roundedQuantity <= rawQuantity`; exchange rounding is always toward zero.
8. A target below `minQty` or `minNotional` is skipped, never rounded up.
9. `usedMargin + reservedMargin <= allocatedCapital`.
10. REDUCE cannot increase absolute exposure.
11. CLOSE cannot be blocked by equity, certification or budget.
12. FLIP cannot open the opposite side before authoritative zero is confirmed.
13. Same normalized input and versions produce byte-equivalent decisions.
14. One intent identity produces at most one real exchange submission.
15. EXECUTABLE_SHADOW and MICRO_LIVE pre-submit targets are equal for the same
    USD 100, 5x input and versions.
16. Simulations never reserve real budget or call the Binance order endpoint.
17. LIVE never creates a synthetic order, fill, fee, funding or PnL fallback.

## 3. Authoritative Equity

Required source fields:

```text
sourceAccountEquityUsd
equityObservedAt
equitySource
equityFreshnessMs
sourceSnapshotVersion
sourcePositionNotionalUsd
sourcePositionQuantity
sourceMarkPrice
sourceEntryPrice
sourceLeverage
sourceSide
sourceEventId
```

Preconditions for OPEN, INCREASE and FLIP OPEN:

- equity is present, finite and greater than zero;
- equity age is at or below the configured maximum age;
- `equityObservedAt` is not in the future relative to calculation time;
- position and equity share `sourceSnapshotVersion`;
- source notional is present and non-negative.

Failure transitions:

| Condition | Decision |
| --- | --- |
| Missing equity | `BLOCKED_SOURCE_EQUITY_MISSING` |
| Equity <= 0 | `BLOCKED_SOURCE_EQUITY_INVALID` |
| Equity older than policy | `BLOCKED_SOURCE_EQUITY_STALE` |
| Snapshot mismatch | `BLOCKED_SOURCE_SNAPSHOT_MISMATCH` |

REDUCE, CLOSE and FLIP CLOSE bypass these entry preconditions. No fallback
equity value is permitted.

Entry eligibility and exit sizing are independent inputs. Suspending a metric,
guard or certification sets `canOpenOrResize=false`, but it must not replace
the allocation's last valid sizing capital with zero before evaluating a
source REDUCE. Otherwise an ineligible strategy could keep excess exposure
precisely when it should be allowed to de-risk. The gate is applied only to
OPEN/INCREASE/FLIP_OPEN after the target has been calculated.

Missing or stale equity must not turn a still-open source position into an
inferred target CLOSE. If the source snapshot still contains that position,
the calculator emits only the equity block and preserves the existing target
position. An authoritative flat source snapshot may still produce CLOSE; this
is an exit decision and does not require equity-based sizing.

Tests RED:

- missing, zero, negative and stale equity block exposure increases;
- snapshot mismatch blocks exposure increases;
- the same inputs still allow REDUCE and CLOSE;
- a still-open source leg plus invalid equity never synthesizes CLOSE;
- a future equity observation is rejected as invalid;
- source notional, not source margin, drives exposure.

Acceptance: no literal or configuration path can supply USD 1,000 when source
equity is absent.

## 4. TargetPortfolioCalculator

`copy-target-core` is a pure Java 21 module with no Spring, database, Kafka or
HTTP dependency. The only public calculation entry point is:

```text
TargetPortfolioCalculator.calculate(TargetPortfolioRequest)
```

Input contains source snapshot, target account snapshot, Binance filters,
optional user limits and all model versions. Output contains selected and
omitted legs, deltas, coverage, scale factor and decision values.

Preconditions:

- currencies and quote assets are explicit;
- all timestamps use UTC instants;
- all quantities and prices use decimal values;
- source legs have stable identities.

Postconditions:

- output ordering is deterministic;
- every input leg appears exactly once as selected or omitted;
- every output decision includes a reason code;
- all computed money/quantity fields are non-negative;
- input collections are not mutated.

Tests RED:

- property tests over arbitrary valid portfolios;
- permutation invariance;
- duplicate leg identity rejection;
- deterministic tie-breaking by symbol and source leg id.

Acceptance: runtime OPEN/RESIZE, EXECUTABLE_SHADOW, replay and capital/leverage
simulation call the same calculator version.

## 5. Proportional Sizing

For each source leg:

```text
exposure = abs(sourceNotional) / sourceEquity
rawTargetNotional = targetCapital * exposure
rawTargetMargin = rawTargetNotional / targetLeverage
```

Exposure above 100% is valid when the source account is leveraged. It is not
silently clamped to one. Portfolio scaling, target exchange leverage and user
risk limits constrain the destination.

Failure states are specific: invalid equity blocks; zero source notional is
`SKIPPED_ALREADY_AT_TARGET` for a flat target; negative absolute inputs are
invalid contracts.

Tests RED include USD 500,000 equity / USD 100,000 notional / USD 100 target /
5x leverage => USD 20 target notional and USD 4 target margin.

## 6. Full Portfolio

Calculation phases:

1. Normalize and validate all source legs.
2. Resolve Binance contracts and filters.
3. Sort by supported symbol, descending exposure, descending liquidity,
   ascending rounding error, symbol and source leg id.
4. Apply an optional user position limit.
5. Calculate total required margin.
6. Apply one common `portfolioScaleFactor` when available margin is lower.
7. Round quantities down through exchange filters.
8. Calculate deltas against authoritative Binance positions.

The calculator must not use arrival order, map iteration order or one-leg-at-a-
time capital consumption.

An inferred CLOSE caused by a source leg being absent is valid only when the
portfolio snapshot is authoritative: `sourcePortfolioComplete=true`, source
and portfolio snapshot versions match, and every advertised source leg was
resolved. An incomplete, contradictory or partially resolved empty snapshot
must preserve current target positions and emit a stable block reason. A
complete empty snapshot is authoritative flat state and may close them.

Tests RED:

- source permutations yield identical targets;
- common scaling preserves target ratios within rounding tolerance;
- nullable limit selects every executable leg;
- configured limit selects deterministically and records omissions;
- manual/external target positions are reported as conflicts, not overwritten.
- incomplete-empty versus complete-empty source snapshots cannot be confused.

### 6.1 Authoritative target-position ownership

For every real-mode calculation, `TargetPortfolioCalculator` receives three
different position sets. They are not interchangeable:

```text
existingPositions          = aggregate position returned by Binance
managedExistingPositions   = aggregate of all active real copies for the user
portfolioExistingPositions = subset attributed to the current wallet/allocation
```

`existingPositions` is an account-level exchange fact. A Binance position does
not carry an allocation owner, so using the complete exchange quantity as the
current quantity of each allocation would let one strategy reduce or close the
exposure owned by another. Deltas for one portfolio are therefore calculated
against `portfolioExistingPositions`, while the account invariant is checked
against `managedExistingPositions`:

```text
aggregate(existingPositions) == aggregate(managedExistingPositions)
```

The comparison is by canonical `targetSymbol + side`. It includes every active
real allocation of that user, including other wallets and strategies. SHADOW
positions are excluded from the real-account aggregate.

If the authoritative Binance snapshot is unavailable/stale, or any aggregate
quantity differs, the calculation is fail-closed for new exposure:

```text
OPEN / INCREASE / FLIP_OPEN = blocked
REDUCE / CLOSE / FLIP_CLOSE = permitted
```

Stable reasons are `BLOCKED_TARGET_POSITION_SNAPSHOT_UNAVAILABLE`,
`BLOCKED_TARGET_POSITION_SNAPSHOT_STALE` and
`BLOCKED_EXISTING_EXPOSURE_CONFLICT`. A manual position, external close,
partial-fill persistence mismatch, lost intent or restart desynchronization
therefore cannot produce a second opening. The snapshot is invalidated after
every attempted real dispatch so a subsequent calculation cannot reuse
pre-order account state.

Two distinct source legs resolving to the same canonical
`targetSymbol + side` are an attribution collision. Until attribution is
explicitly represented by the input contract, the whole entry calculation is
fail-closed with `BLOCKED_TARGET_SYMBOL_COLLISION`; colliding existing target
positions are preserved rather than inferred closed, and explicit
REDUCE/CLOSE remains available. The engine must never submit two deltas that
both subtract the same aggregate existing quantity.

For EXECUTABLE_SHADOW, the simulated managed state is authoritative and no
private Binance call is made. For multiple allocations sharing a Binance
symbol, their attributed quantities may differ while the sum across all
allocations must equal the real Binance aggregate.

Tests RED cover one-way and hedge-mode position mapping, manual exposure,
external close, snapshot failure, two allocations sharing one symbol, exits
during conflict, source-alias target collisions, cache invalidation and the
prohibition of literal empty position inputs in real sizing.

## 7. Binance Filters And Rounding

Preconditions include `TRADING`, quote asset, lot-size, notional, precision,
leverage and account position-mode compatibility.

Quantity:

```text
roundedQty = floor(rawQty / stepSize) * stepSize
```

Failures:

```text
SKIPPED_SYMBOL_NOT_SUPPORTED
SKIPPED_NO_BINANCE_ALIAS
SKIPPED_BELOW_MIN_QTY
SKIPPED_BELOW_MIN_NOTIONAL
SKIPPED_ROUNDED_TO_ZERO
BLOCKED_LEVERAGE_LIMIT
BLOCKED_MARGIN_MODE_MISMATCH
BLOCKED_EXISTING_EXPOSURE_CONFLICT
```

No policy may increase quantity to meet an exchange minimum.

Tests RED cover changing filters, suspended symbols, min quantity, min
notional, precision, one-way/hedge mode and reduce-only closes.

## 8. Lifecycle

States:

```text
FLAT -> OPEN_PENDING -> OPEN
OPEN -> INCREASE_PENDING -> OPEN
OPEN -> REDUCE_PENDING -> OPEN|FLAT
OPEN -> CLOSE_PENDING -> FLAT
OPEN -> FLIP_CLOSE_PENDING -> FLAT -> FLIP_OPEN_PENDING -> OPEN
any pending -> RECONCILING -> prior state|next state|MANUAL_REVIEW
```

OPEN creates a new economic cycle. INCREASE and REDUCE update that cycle. CLOSE
ends it. Reopening creates a new `copy_operation` identity and cycle.

Partial FLIP close remains `FLIP_CLOSE_PENDING`; opposite OPEN is forbidden.
Out-of-order events are durably classified and reconciled against source and
Binance snapshots.

Before a real REDUCE/CLOSE, the latest target-position snapshot determines the
safe exit quantity without becoming an entry gate:

- authoritative zero means the user already closed externally; no redundant
  Binance order is sent;
- authoritative quantity below the locally attributed quantity caps the
  reduce-only request to the exchange quantity;
- unavailable or stale target-position evidence falls back to the local
  reduce-only quantity, because snapshot failure must not block de-risking.

An authoritative external-flat transition first persists an idempotent,
required `RECONCILED_CLOSE` ledger event using the deterministic close
`clientOrderId`, then marks the local copy and economic cycle closed. Since no
fill exists, `priceClose`, fee, funding, slippage and PnL remain `null` with
`economicDataStatus=UNAVAILABLE`; no synthetic execution fact is allowed.
Retry after a crash reuses the same ledger event and completes only the missing
local state transition.

Tests RED cover duplicate OPEN, CLOSE-before-OPEN, delayed INCREASE, repeated
FLIP, stale events, partial close, reopen identity, external manual close,
authoritative quantity capping, snapshot-unavailable exit fallback and a
partial REDUCE while entry eligibility is suspended.

## 9. Economic Persistence

Persistence must distinguish `KNOWN`, `PENDING_RECONCILIATION` and
`UNAVAILABLE`; unknown fee, funding or slippage is never numeric zero.

Required identifiers and values are those in the approved implementation
request, including source event, intent, client order, Binance order, trade
identities, requested/executed quantities, individual fills, fees, funding,
gross/net PnL and latency timestamps.

Transaction postcondition: an exchange submission always has a durable intent
created before send. Fill persistence and budget release are idempotent.

Tests RED cover missing avg price, late trades, late commission, funding after
close and crash boundaries before/after send/fill/persist.

## 10. MICRO_LIVE Real

Policy `MICRO_LIVE_PROPORTIONAL_V3`:

```text
capital = USD 100
leverage = 5x
fixedPerOperation = absent
globalMaxPositions = absent
```

The account must have at least the configured capital plus exchange-required
buffers. The calculator uses all executable source legs and scales them as one
portfolio. Optional user limits remain nullable.

Any legacy `maxMarginPerOperationUsd=20` or `maxConcurrentPositions=5` value is
ignored by V3 and exposed as a migration warning.

The nullable `userMaxConcurrentPositions` is request-scoped, not a service-wide
property. It must travel from the exact allocation into the durable dispatch
intent and the atomic PostgreSQL budget reservation. This closes the race where
two concurrent entries both passed a stale calculator snapshot. A changed
limit on a replay of the same intent is a payload conflict, not a silent reuse.
`null` remains unlimited and no default value may be synthesized.

The optional limit is account-wide and applies only to positions that would be
new. Existing attributed positions and a FLIP replacing an attributed opposite
side remain in the target set even when the user lowers the limit below the
current open count; the limit cannot manufacture a close to make room for a
different symbol. Positions belonging to other allocations consume available
slots. A candidate that fails exchange minima after deterministic scaling does
not consume a slot, and adding a lower-ranked candidate cannot dilute an
already selected higher-ranked candidate below an exchange minimum. The atomic
PostgreSQL reservation remains the final concurrency authority.

## 11. Capital And Leverage Simulation

The COLD matrix is the Cartesian product of:

```text
capital = 100,250,1000,5000,10000,50000,100000,250000,500000,1000000
leverage = 5,10,15,20
```

Exactly 40 scenario results are persisted per strategy event/snapshot version.
Simulation state is `PENDING`, `RUNNING`, `PAUSED`, `COMPLETED` or `FAILED` and
has a resume cursor. It consumes the same normalized calculator request as
runtime, with a simulation-only target account.

Postconditions: no dispatch intent, budget reservation, real operation or
Binance order can reference a simulation run.

If authoritative source equity is missing, stale, invalid or belongs to a
different snapshot, every scenario remains simulation-only with an explicit
`UNAVAILABLE_<reason>` economics status. Target/PnL economics are not
fabricated as zero and the matrix cannot support certification.

Tests RED verify 40 unique combinations, pause/resume idempotency, HOT priority,
and no side effects on runtime tables.

## 12. Liquidity

For large-capital bands, a versioned order-book snapshot provides bids/asks,
capture time and source. The model produces VWAP, slippage, depth consumption,
fill percentage, unfilled notional, duration and participation.

States are `NO_BOOK`, `INSUFFICIENT_DEPTH`, `ESTIMATED` and `PARTIALLY_REAL_VALIDATED`.
Single market, fragmented, TWAP and participation-cap scenarios are separate.
Capital bands without sufficient depth evidence cannot be `REAL_VALIDATED`.

## 13. Calibration

Calibration joins MICRO_LIVE real and EXECUTABLE_SHADOW by source event set,
strategy identity, USD 100 capital, 5x leverage and every model version.

```text
pnlCaptureRatio = microLiveNetPnl / executableShadowNetPnlSameEvents
```

Zero denominator returns `UNAVAILABLE`, not zero or infinity. Partial/mismatched
event windows are rejected. Output includes fill, fee, slippage, latency and PnL
errors with sample counts and confidence level.

## 14. Global Certification And User Adoption

Certification identity includes wallet, strategy/version, capital band,
leverage, exchange, quote and sizing/symbol/fee/funding/slippage/liquidity model
versions. Evidence and state enums are exactly those in the approved request.

Promotion is manual by default. This applies both to global certification state
transitions and to the legacy SHADOW -> MICRO_LIVE allocation creator:
`copy.promotion.enabled` and `copy.promotion.job.enabled` default to `false`.
The evaluator may still be invoked explicitly for audit, but a scheduled job
must not create a real-mode allocation unless an operator has enabled both
switches deliberately. The actor, reason, prior state, next state, evidence
snapshot and timestamp are immutable audit data.

Certification is reusable by users. User balance, allocation, leverage,
permissions, account mode, manual positions and limits are validated separately.
One user's failure cannot revoke global evidence unless it exposes a global
model defect.

## 15. LIVE Real

LIVE preconditions:

- certification state `LIVE_APPROVED` for the exact identity and band;
- user adoption validation successful;
- all runtime source and exchange preconditions successful;
- real dispatch feature flags enabled by an operator outside this change.

Failure postcondition:

```text
LIVE_NOT_EXECUTED + reasonCode + reasonDetail
```

No SHADOW fallback exists. Reductions and closes remain available while entry
certification is degraded, suspended or revoked.

## 16. Idempotency And Recovery

Intent identity:

```text
sourceEventId + allocationId + intentType + intentVersion
```

Payload conflict stores previous hash, incoming hash and a field-level diff,
blocks send, alerts and transitions to reconciliation/manual review.

Recovery uses authoritative order and position state. Retry is allowed only
when no exchange order can exist for the client order id. Ambiguous responses
remain reconciling and must never cause a second submit.

Tests RED include 100 duplicates, two threads, two pods, Kafka replay, duplicate
outbox, timeout, ambiguous response and every requested crash boundary.

## 17. Decision Taxonomy

The minimum decision codes are the complete list in the approved V3 request.
Every decision persists the calculator versions, source values, exchange
filters, account values and prior/target/resulting quantities used.

Dispatch ambiguity has one stable classification per outcome:

```text
transport timeout before an authoritative acknowledgement
    -> EXECUTION_TIMEOUT_RECONCILING
lost, malformed or otherwise non-authoritative response
    -> EXECUTION_AMBIGUOUS_RECONCILING
reconciled final fill whose prior durable cause was a timeout
    -> RECONCILED_AFTER_TIMEOUT
reconciled final fill from any other ambiguous cause
    -> EXECUTED | PARTIALLY_FILLED
```

The historical aliases `BINANCE_RESPONSE_AMBIGUOUS`,
`BINANCE_OUTCOME_AMBIGUOUS` and `PERSISTENCE_RECOVERED` are forbidden as
persisted V3 `reasonCode` values. Timeout classification must inspect the
exception cause chain because HTTP clients commonly wrap socket/connect/read
timeouts. An ambiguous outcome always enters `RECONCILING`; a replay with the
same intent may query by `clientOrderId` but cannot submit a second order.
Certification/readiness queries must count both current
`EXECUTION_TIMEOUT_RECONCILING` / `EXECUTION_AMBIGUOUS_RECONCILING` rows and
the two historical ambiguity aliases while old rows still exist. An unresolved
current-code row cannot be invisible to promotion evidence.

Unknown failures map to `REJECTED_BY_BINANCE_UNKNOWN` only at the Binance
boundary; domain validation cannot emit generic `FAILED`.

## 18. Migration And Rollback

Migration phases:

1. Add nullable V3 fields and tables.
2. Dual-read equity fields while V3 dispatch remains disabled.
3. Run deterministic executable-shadow replay.
4. Enable V3 MICRO_LIVE only after manual validation.
5. Leave V2 facts readable for comparison.

Rollback disables V3 entry intents and matrix workers, continues REDUCE/CLOSE
reconciliation, preserves all V3 rows, and restores V2 reads. It never deletes
source events, intents, fills, cycles, simulations or certifications.

Operational SQL, dashboards and active runbooks are part of the V3 contract.
They must validate the USD 100 aggregate MICRO_LIVE capital boundary and an
optional per-user `userMaxConcurrentPositions`; they must not classify a
quantity above USD 20 or a portfolio above five positions as a violation by
itself. Historical V2 documents may retain the former values only when marked
as superseded and must never be used as canary instructions.

## 19. Required Test Gates

```text
unit
property
contract
PostgreSQL migration/integration
Kafka replay
Binance adapter
lifecycle
concurrency and two-pod
restart/recovery
SHADOW/MICRO_LIVE parity
capital/leverage matrix
liquidity
deterministic a445 replay
operational validation rejects legacy fixed USD 20/five-position thresholds
request-scoped user position limit survives dispatch, persistence and replay
```

Tests are first committed in RED against this contract. Implementation follows.

## 20. Release Gate

The highest local gate is `LIVE_ENGINE_CANARY_READY`. `LIVE_CERTIFIED` requires
real, externally observed MICRO_LIVE evidence and cannot be produced by local
tests or replay. Deployment and feature-flag activation are out of scope.
