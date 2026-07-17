# Wallet Metric Economic V2 - Signals consumer final certification

Date: `2026-07-15`  
Method: strict SDD, contract tests, PostgreSQL 16, real local HTTP  
Verdict: `SIGNALS_V2_CONTRACT_CERTIFIED`

## Executive result

Signals now consumes an immutable V2 strategy snapshot and validates all metadata
required before new exposure. The hot decision path performs no HTTP call, no FULL
simulation and no database query. OPEN/INCREASE fail closed on absent, stale,
incomplete, mixed-generation or low-coverage data. CLOSE/REDUCE remain available
without asking Metrics to authorize a new entry.

This verdict certifies the consumer contract and local integration. It does not enable
real-money operation and does not override the global production coverage blocker.

## V2 input contract

For OPEN and INCREASE, `MetricCopyDecisionGateway` requires:

- canonical `strategyKey`;
- non-empty `generationId` and generation agreement across summary/FULL/guard;
- `metricVersion=2`;
- `sourceVersion=wallet_metric_economic_v2`;
- valid `dataAsOf` within staleness policy;
- FULL evaluation, `decisionFinal=true`, facts loaded and
  `requiresFullSimulation=false`;
- valid copy-guard action;
- sufficient `coveragePct` and `evidenceStatus=PASSED`;
- complete 44-scenario matrix when matrix evidence is required;
- V2 response source, calculator and policy metadata.

Missing metadata does not receive a default. Unexpected V1 source, generation mismatch,
duplicate strategy identity, stale snapshot, UNKNOWN critical evidence, incomplete
matrix or insufficient coverage returns a reasoned rejection and increments the
coverage/contract telemetry.

## CLOSE and REDUCE

CLOSE and REDUCE are risk-reducing actions and bypass new-entry authorization. They use
the durable/local position and dispatch state, retain idempotency and remain possible
when Metrics has no ACTIVE generation, a refresh fails or V2 data expires. This avoids
turning an analytics outage into trapped exposure.

The bypass does not permit quantity growth: routing and runtime guards still distinguish
reducing from exposure-increasing deltas.

## Snapshot, cache and refresh

The refresh cycle obtains summary, FULL and copy-guard outside the hot path, validates
the complete set and swaps one immutable snapshot atomically. Cache identity is
`strategyKey + generationId`; a generation change invalidates the prior set. A failed
refresh may retain the old object for diagnostics, but OPEN/INCREASE fail closed after
the staleness bound.

The PostgreSQL snapshot store uses generation and complete strategy identity. It does
not merge V1 and V2 rows and rejects incomplete fixture populations. The real local HTTP
integration loaded five summary, five FULL and five guard units atomically; the negative
fixture failed closed.

## Canonical sizing and 44 scenarios

Only the internal module was used:

`ms-signals-orc/modules/copy-target-core`

No external `copy-target-core` was used or modified. The matrix remains exactly:

```text
11 capitals: 100, 250, 500, 1,000, 5,000, 10,000, 50,000,
              100,000, 250,000, 500,000, 1,000,000
4 leverages:  x5, x10, x15, x20
11 x 4 = 44 scenarios
```

The leverage changes required margin, not requested target notional. Quantity, price,
step size, min notional and existing exposure remain canonical core inputs. Missing
market/economic evidence is represented by UNKNOWN reason codes rather than optimistic
zeroes. Matrix generation is a resumable cold-path worker, not per-operation work.

## Fail-closed matrix

| Condition | OPEN/INCREASE | CLOSE/REDUCE |
|---|---|---|
| no ACTIVE V2 | reject | allow reducing action |
| incomplete metadata | reject | allow reducing action |
| generation mismatch | reject | allow reducing action |
| stale data | reject | allow reducing action |
| coverage below policy | reject + telemetry | allow reducing action |
| evidence not PASSED | reject | allow reducing action |
| matrix missing/incomplete | reject when required | allow reducing action |
| source V1/unexpected | reject | allow reducing action |

## PostgreSQL 16 and HTTP evidence

`MetricV2SnapshotPostgresIntegrationTest` passed against PostgreSQL `16.14`. Flyway
validated 40 migrations and applied the three required migrations in the isolated
Signals test database `metric_v2_test_signals_20260715_0110`.

`MetricV2HttpContractIntegrationTest` passed against the temporary Metrics service on
`127.0.0.1:4114`. It consumed the same generation and rejected an incomplete snapshot.
No request was sent to Binance and no order path was enabled.

## Performance

Latest opt-in hot-path benchmark, 100,000 decisions after snapshot load:

| Metric | Result |
|---|---:|
| min | 25.5587 us |
| p50 | 31.2510 us |
| p95 | 45.3723 us |
| max | 48.3820 us |
| remote calls per decision | 0 |

These figures measure in-memory lookup plus V2 validation. They exclude Kafka, network,
PostgreSQL, Binance and actual order acknowledgement and are not an exchange-latency
promise. Refresh latency was measured separately by Metrics; it does not run for each
delta.

## Tests and build

| Command/suite | Result |
|---|---|
| focused V2 consumer suite | 19/19 passed |
| real local HTTP contract | 1/1 passed |
| PostgreSQL 16 snapshot integration | 1/1 passed |
| full `./mvnw test` | 642 tests, 0 failures, 0 errors, 4 skipped |
| `./mvnw clean package` | 642 tests, 0 failures, 0 errors, 4 skipped; JAR built |
| internal `copy-target-core` tests | 36/36 passed |
| hot decision benchmark | 100,000 decisions completed |
| `git diff --check` | passed; only CRLF warnings |

One Testcontainers class could not find Docker and therefore reported zero executed
tests. Equivalent PostgreSQL behavior was exercised against the local PostgreSQL 16.14
instance. The four skips remain explicit. Build warnings are pre-existing MapStruct
unmapped targets, Java `ThreadDeath` deprecations, Hyperliquid deprecated/unchecked API
use and Mockito self-attach; none was introduced as a V2 contract bypass.

## Production safety flags

Defaults remain fail-safe:

```text
COPY_NEW_DISPATCH_ENABLED=false
COPY_LIVE_ENABLED=false
COPY_LIVE_CANARY_ENABLED=false
COPY_LIVE_DRY_RUN=true
COPY_LIVE_SCALE_ENABLED=false
COPY_MICRO_LIVE_ENABLED=false
COPY_PROMOTION_ENABLED=false
COPY_PROMOTION_JOB_ENABLED=false
COPY_LIVE_PROMOTION_ENABLED=false
COPY_LIVE_PROMOTION_JOB_ENABLED=false
```

The Metrics read default in Signals remains `COMPARE`; no code in this closure changes
the production mode or enables money.

## Objective table

| Objetivo | Resultado | Evidencia | Estado | Bloqueo o limitacion |
|---|---|---|---|---|
| Require complete V2 metadata | strict DTO and gateway validation | focused tests | COMPLETED | none in consumer code |
| Fail closed OPEN/INCREASE | all invalid/stale/low-coverage cases reject | decision tests | COMPLETED | production has no eligible ACTIVE snapshot |
| Keep CLOSE/REDUCE | risk-reducing bypass retained | policy tests | COMPLETED | still subject to local execution safety |
| Prevent V1 fallback | source/version/generation validated | contract + store tests | COMPLETED | none |
| Atomic generation cache | immutable snapshot swap | store/PG tests | COMPLETED | refresh cadence remains operational dependency |
| Preserve matrix 44 | internal core and worker tests | 36 core tests | COMPLETED | market evidence may remain UNKNOWN |
| Hot path latency | p95 45.3723 us, zero remote calls | opt-in benchmark | COMPLETED | excludes exchange/network |
| Real HTTP integration | five units loaded atomically | 1 integration test | COMPLETED | isolated fixture |
| Enable MICRO_LIVE/LIVE | intentionally disabled | config proof | NOT_APPLICABLE | requires later controlled cutover |

## Residual risk

Signals is contract-ready but cannot create a valid new-entry snapshot from production
until ETL publishes a certifiable ACTIVE generation and Metrics exposes it. A later
staging exercise must include production-like cardinality, refresh contention, network
faults and exchange latency before enabling any money flag.

No deploy, production write, cutover, commit, push, Binance order or other real order
was performed.
