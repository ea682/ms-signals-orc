# Copy Trading V3 - Final Local Evidence

Date: 2026-07-13

Final system gate: `LOCAL_TEST_READY`

This gate is intentionally lower than the locally implemented capabilities. No
deployment, secret change, production LIVE activation, or real Binance order was
performed.

## Executive Verdict

The V3 implementation removes fixed USD 20 sizing and the global five-position
limit from the executable path. MICRO_LIVE is a total USD 100, 5x proportional
portfolio. A user position limit is nullable, account-wide and applies only to
new positions. LIVE has no simulated fallback. REDUCE/CLOSE remain de-risking
operations and are independent from entry capital/certification failures.

The calculator, durable dispatch, recovery, exchange evidence, COLD 10 x 4
simulation, calibration, certification, user adoption and V3 read model are
implemented and locally tested. The historical a445 dataset cannot produce a
valid executable replay because it contains no authoritative source equity. It
therefore fails closed, as required.

## 1. SDD Specifications

- `ms-signals-orc/docs/specs/copy-trading-proportional-portfolio-v3-sdd.md`
- `ms-signals-orc/docs/specs/live-certification-adoption-v3-sdd.md`
- `ms-sentinel-hyperliquid/docs/specs/authoritative-equity-snapshot-v3-sdd.md`
- `ms-binance-engine/docs/specs/authoritative-execution-evidence-v3-sdd.md`
- `ms-binance-engine/docs/specs/public-order-book-snapshot-v3-sdd.md`
- `ms-wallet-metric-etl/docs/specs/copy-certification-v3-sdd.md`
- `ms-wallet-metric-etl/docs/specs/wallet-metric-economic-v2-sdd.md`
- `ms-metrica-cuenta/docs/specs/copy-read-model-v3-sdd.md`

Each V3 specification defines invariants, authority, failure behavior, states,
acceptance tests and rollback boundaries. The ETL specification also prevents
duplicate Flyway versions per deployable location.

## 2. Implementation

| Owner | Implemented responsibility |
|---|---|
| `copy-target-core` | Pure deterministic `TargetPortfolioCalculator`, proportional sizing, filters, rounding, full-portfolio scale, lifecycle deltas, optional user limit and liquidity simulation |
| `ms-sentinel-hyperliquid` | Authoritative equity/position snapshot contract and fail-closed source authority |
| `ms-signals-orc` | Executable shadow, MICRO_LIVE/LIVE target orchestration, durable intents, account-wide budget, lifecycle, economic cycles, COLD simulations, certification/adoption gates and recovery |
| `ms-binance-engine` | Current symbol/filter contract, order book, order/fill/trade/fee/funding/position evidence and stable exchange outcome taxonomy |
| `ms-wallet-metric-etl` | Economic read model, cycle facts, COLD calibration worker, generation isolation and certification evidence |
| `ms-metrica-cuenta` | Read-only V3 simulations/calibrations/certifications API without legacy fallback claims |

The legacy `/operaciones/metrica/v2` simulator remains an explicitly legacy API.
It is not accepted as V3 execution, calibration or certification evidence.

## 3. Migrations

Signals V3:

- `V202607130002__copy_economic_execution_v3.sql`
- `V202607130003__copy_simulation_certification_v3.sql`
- `V202607130004__copy_dispatch_payload_conflict_v3.sql`
- `V202607130005__copy_dispatch_user_position_limit_snapshot_v3.sql`

ETL V3:

- `V202607130001__copy_economic_read_model_v3.sql`
- `V202607130002__copy_calibration_worker_v3.sql`

Migration evidence:

- Signals `V...0002` applied twice on PostgreSQL 18; eight inspected economic
  columns and four constraints remained valid.
- Signals `V...0003/0004/0005` applied twice by the integration fixture; both
  certification/adoption and payload-conflict flows passed.
- ETL V2/V3 migrations executed in the PostgreSQL integration suite.
- Unique Flyway versions: Signals `36/36`, ETL legacy `5/5`, ETL V2 `8/8`.
- Two byte-identical stale legacy files sharing `202606080001` were removed;
  canonical versions `202606080002` and `202606080003` are unchanged.

## 4. Modified Contracts

- Source snapshots carry equity value/time/source/freshness/version and position
  notional/quantity/mark/entry/leverage/side/event identity.
- Target calculations return selected/omitted legs, target notional/margin/qty,
  actual delta, common scale, coverage and reason per leg.
- Dispatch identity remains `sourceEventId + allocationId + intentType + version`;
  a conflicting payload persists old/new hashes and field diff, blocks submit and
  opens one durable alert.
- Execution evidence uses nullable economics with `KNOWN`,
  `PENDING_RECONCILIATION` or `UNAVAILABLE`; unknown fee/funding/slippage is not
  persisted as zero.
- V3 metrics endpoints expose simulations, calibrations and certifications only.

## 5. RED And GREEN Evidence

| SDD increment | RED | GREEN |
|---|---:|---:|
| Stable timeout/ambiguity taxonomy | 7 failures in 47 tests | 47 passed |
| Promotion query recognizes current ambiguity codes | 1 failure | passed |
| Account-wide optional user position limit | 4 failures in 23 tests | 23 passed |
| Source alias target collision | 1 failure | passed |
| Flyway version integrity after merge | 1 failure in 1 test | full ETL suite passed |

Latest aggregate local evidence:

| Module | Passed | Skipped | Failed/Error |
|---|---:|---:|---:|
| `copy-target-core` | 36 | 0 | 0 |
| `ms-signals-orc` | 582 | 2 | 0 |
| `ms-binance-engine` | 40 | 0 | 0 |
| `ms-sentinel-hyperliquid` | 20 | 0 | 0 |
| `ms-wallet-metric-etl` | 61 | 2 | 0 |
| `ms-metrica-cuenta` | 134 | 1 | 0 |
| **Total** | **873** | **5** | **0** |

Signals includes `17/17` PostgreSQL 18 concurrency tests and `2/2` PostgreSQL
certification/migration tests. Metrics also passed `npm run build`. Sixteen YAML
documents parsed successfully with duplicate-key detection. No conflict marker,
`.rej`, `.orig`, active-location Flyway duplicate, V3 fixed-USD-20 setting,
global-five-position setting or USD-1000 equity fallback was found.

The skipped tests are one opt-in CPU benchmark, one PostgreSQL-16-only DLQ plan,
two destructive/reference ETL jobs and one opt-in legacy V2 PostgreSQL simulation.
Three additional Testcontainers classes that require exact PostgreSQL 16 did not
start because Docker is not installed; this is an external gate item.

## 6. a445 Replay

Evidence: `ms-wallet-metric-etl/docs/reports/a445-proportional-v3-replay-20260713.md`.

- Historical `+22.419839 USDC` is obsolete because it used forbidden fixed sizing.
- 8,800 source rows were examined; zero carry authoritative equity and all are
  marked estimated.
- Result: `BLOCKED_SOURCE_EQUITY_MISSING` for OPEN/INCREASE/FLIP-open.
- REDUCE/CLOSE remain allowed when a managed target position exists.
- No PnL, fee, funding, slippage or liquidity value was manufactured.

## 7. Capital x Leverage Matrix

All 40 combinations are implemented as distinct COLD jobs for capitals 100,
250, 1,000, 5,000, 10,000, 50,000, 100,000, 250,000, 500,000 and 1,000,000,
with leverage 5x, 10x, 15x and 20x. Matrix property/determinism/isolation tests
pass. Every a445 cell is honestly `UNAVAILABLE_BLOCKED_SOURCE_EQUITY_MISSING`.

## 8. Calibration

The durable worker compares identical event set, window, strategy, engine/model
versions, USD 100 and 5x. It records fill/slippage/fee/latency/PnL differences and
`pnlCaptureRatio`. a445 has no real MICRO_LIVE fills, so calibration is absent,
not zero and not successful.

## 9. Economic Persistence

One `copy_operation` represents one OPEN-to-CLOSE cycle. Reopen creates a new
operation/cycle. Requested/executed quantities, individual fills, trade/order IDs,
prices, fees, funding, PnL, slippage and stage latency fields are versioned and
nullable by availability state. Cumulative recovery applies only the unpersisted
fill delta.

## 10. Global Certification

Certification identity includes wallet/strategy/version/scope/capital band/
leverage/exchange/quote and all sizing, symbol, fee, funding, slippage and
liquidity model versions. Evidence and state transitions are durable and audited.
Automatic initial promotion is disabled; approval/activation is manual.

## 11. User Adoption

Global certification is reused, while balance, allocated capital, leverage,
quote, margin mode, API permissions, manual positions, optional limits and risk
are validated per user/allocation. The final LIVE entry gate requires both an
approved certification and valid current adoption.

## 12. LIVE Audit

Full case table: `docs/reports/copy-trading-v3-live-edge-case-audit-20260713.md`.
It documents expected behavior, durable state, reason, retry, reconciliation,
alert, entry blocking, exit permission and evidence for every requested capital,
sizing, Binance, event and recovery case.

## 13. Additional Edge Cases Found

- Target snapshot unavailable or internally inconsistent.
- Manual/other-allocation exposure on the same target symbol.
- External partial or full close before local reconciliation.
- Two source aliases resolving to the same target symbol and side.
- Optional limit selecting an untradable candidate or evicting an incumbent.
- Current ambiguity writers not matching promotion-read aliases.
- Crash after operation persistence but before economic ledger persistence.

## 14. Bugs Corrected

Seven production bugs and the ETL Flyway merge regression are listed with their
tests in the LIVE audit. The corrections preserve exits and fail closed for new
exposure.

## 15. Idempotency Evidence

Duplicate/replay/two-thread/restart tests prove one durable identity and at most
one real submit path. Payload mismatch never reuses an intent silently. Repeated
conflicts increment one open record. `clientOrderId` and Binance order lookup are
used before any retry after an ambiguous boundary.

## 16. Concurrency Evidence

`17/17` PostgreSQL 18 tests cover concurrent claims, two logical replicas,
cross-strategy USD 100 budget serialization, account-wide position reservation,
progress uniqueness, deadlock retry and statement/lock timeout behavior.

## 17. Recovery Evidence

Recovery covers crash before/after reservation, submit, partial fill, fill and
local persistence. Timeout uses `EXECUTION_TIMEOUT_RECONCILING`; lost/malformed/
5xx/429 outcome uses `EXECUTION_AMBIGUOUS_RECONCILING`. A full fill recovered
after timeout uses `RECONCILED_AFTER_TIMEOUT`; other recovered full/partial fills
use `EXECUTED`/`PARTIALLY_FILLED`.

## 18. Promotion Runbook

Use `docs/runbooks/copy-trading-live-rollout.md`. Apply migrations through the
approved pipeline, run read-only validations, create certification, validate
user adoption, activate manually, start with a controlled MICRO_LIVE/canary and
observe reconciliation/economics before expanding.

## 19. Degradation Runbook

On stale equity, data-risk, model/version drift, calibration deterioration,
exchange ambiguity or unresolved economic evidence: block OPEN/INCREASE, keep
REDUCE/CLOSE, move certification to `LIVE_DEGRADED`/`SUSPENDED`, retain durable
intents and reconcile. Use `docs/runbooks/copy-dispatch-incident-reconciliation.md`.

## 20. Rollback Runbook

Disable V3 entry/simulation workers and LIVE activation flags; do not delete
economic evidence or reverse additive migrations during an incident. Continue
reconciliation and exits. ETL/API read rollback uses the documented feature/read
switch in `ms-wallet-metric-etl/docs/runbooks/wallet-metric-v2-rollback.md`.

## 21. External Blockers

1. Immutable a445/source history with authoritative equity and matching position
   snapshot versions.
2. Real MICRO_LIVE USD 100 at 5x fills for same-event calibration.
3. Controlled Binance canaries for timeout, 429/5xx, filter drift, partial fill,
   unknown average price, funding and real margin pressure.
4. Exact PostgreSQL 16 integration/Testcontainers run; local host only provides
   PostgreSQL 18 and has no Docker CLI.
5. Deployed two-pod Kafka/PostgreSQL fault rehearsal and operator sign-off.
6. Real order-book evidence before validating high-capital liquidity bands.

## 22. Final Gate

`LOCAL_TEST_READY`

Not justified yet: `MICRO_LIVE_VALIDATION_READY`, `LIVE_ENGINE_CANARY_READY`,
`LIVE_PARTIALLY_CERTIFIED` or `LIVE_CERTIFIED`.

The software can calculate executable shadow and the 10 x 4 matrix when a valid
authoritative snapshot is supplied, but the available a445 and real-exchange
evidence do not permit a higher system gate.
