# Copy Trading V3 - LIVE Edge Case Audit

Date: 2026-07-13

Scope: `copy-target-core`, `ms-sentinel-hyperliquid`, `ms-signals-orc`,
`ms-binance-engine`, `ms-wallet-metric-etl`, and `ms-metrica-cuenta`.

## Legend

- `NEW`: reevaluate only after a new source/account snapshot.
- `LOOKUP`: query the existing Binance order/position; never submit again.
- `AUTH`: a submit retry is legal only after authoritative non-existence is proven.
- `B`: OPEN/INCREASE blocked. `A`: operation allowed.
- `AUTO`: deterministic automated test. `PG18`: local PostgreSQL concurrency test.
- `CANARY_REQUIRED`: behavior is locally guarded but still needs real exchange or
  infrastructure fault evidence before LIVE certification.
- Every real submit has a durable intent created before the Binance call.

## Capital

| Case | Expected behavior | Durable final | reasonCode | Retry | Reconcile | Alert | Entry | Exit | Evidence |
|---|---|---|---|---|---|---|---|---|---|
| Capital zero | No target exposure and no submit | no send | `BLOCKED_INSUFFICIENT_MARGIN` | NEW | no | warn | B | A | AUTO `zeroOrNegativeTargetCapitalBlocksOnlyNewExposure` |
| Capital negative | Reject invalid snapshot instead of normalizing it | input rejected | validation error | NEW | no | warn | B | A | AUTO same test |
| Capital changes in-flight | Keep claimed payload immutable; recalculate on a new event | old intent unchanged; conflicting replay audited | `BLOCKED_IDEMPOTENCY_PAYLOAD_CONFLICT` | NEW | yes on conflict | yes | B | A | AUTO `sameSourceEventWithDifferentPayloadIsBlocked` and conflict recorder tests |
| Binance balance below configured capital | Fail before exposure increase | no send or rejected reservation | `BLOCKED_INSUFFICIENT_MARGIN` | NEW | no | yes | B | A | AUTO `blocksNewExposureWhenAvailableCapitalIsInsufficient`, adoption validator |
| Capital reserved by other orders | Serialize the account budget and reject the loser | intent `REJECTED`, reservation released | `MICRO_LIVE_TOTAL_MARGIN_EXCEEDED` | NEW | no | warn | B | A | AUTO+PG18 `totalUsedAndReservedMarginCannotExceedOneHundred`, `twoReplicasSerializeWalletBudgetAcrossDifferentStrategies` |
| Multiple source wallets | Keep independent wallet identity and budget scope | one terminal intent per wallet/allocation | decision of each wallet | NEW | per intent | on failure | per wallet | A | AUTO architecture budget scope contracts |
| Multiple strategies for one wallet | Share USD 100 MICRO_LIVE budget atomically | one winner; excess intent rejected | margin or user limit reason | NEW | no | warn | B when full | A | AUTO+PG18 same cross-strategy test |
| Same Binance symbol in different allocations | Compare account aggregate and allocation attribution before entry | target calculated only when aggregate matches | `BLOCKED_EXISTING_EXPOSURE_CONFLICT` on mismatch | NEW | yes | yes | B on mismatch | A | AUTO `sharedSymbolUsesAllocationAttributionWhileValidatingTheAccountAggregate` |
| Manual Binance position | Never attribute it to copy trading or add exposure | no send | `BLOCKED_EXISTING_EXPOSURE_CONFLICT` | NEW | yes | yes | B | A | AUTO `manualBinanceExposureBlocksEntryWithoutAssigningItToThePortfolio` |
| Fees/funding reduce free margin | Use current available margin; economics remain separate | blocked entry or persisted fee/funding facts | margin reason | NEW | WARM economics | yes if threshold | B when insufficient | A | AUTO funding/fee assemblers; CANARY_REQUIRED for real margin pressure |
| Liquidation or margin call | Do not classify as normal close; reconcile authoritative position | quarantined source fact and/or target conflict | explicit liquidation/ADL policy or exposure conflict | LOOKUP | yes | critical | B | A/reconcile | AUTO `liquidationAndAdlCannotBecomeNormalCloseWithoutExplicitPolicy`; CANARY_REQUIRED |

## Sizing

| Case | Expected behavior | Durable final | reasonCode | Retry | Reconcile | Alert | Entry | Exit | Evidence |
|---|---|---|---|---|---|---|---|---|---|
| Equity missing | Preserve open target and block exposure increase | no send | `BLOCKED_SOURCE_EQUITY_MISSING` | NEW | no | yes | B | A | AUTO `missingOrStaleEquityBlocksExposureIncrease` |
| Equity zero/negative | Reject invalid denominator; never infer a close | no send | `BLOCKED_SOURCE_EQUITY_INVALID` | NEW | no | yes | B | A | AUTO `invalidEquityDoesNotTurnAStillOpenSourceLegIntoAnInferredClose` |
| Equity stale/future | Fail closed by observed time | no send | stale or invalid equity code | NEW | no | yes | B | A | AUTO stale/future equity tests |
| Equity snapshot mismatch | Do not mix position and equity versions | no send | `BLOCKED_SOURCE_SNAPSHOT_MISMATCH` | NEW | no | yes | B | A | AUTO source authority policy and matrix unavailable tests |
| Exposure above 100% | Preserve actual ratio, then apply one common margin scale | calculated or skipped by filters | `TARGET_CALCULATED` | NEW | no | no | A if executable | A | AUTO `sourceExposureAboveOneHundredPercentUsesOneCommonScaleFactor` |
| Different source leverage | Size from source notional/equity, not source margin | calculated | `TARGET_CALCULATED` | NEW | no | no | A | A | AUTO `sourceLeverageDoesNotReplaceNotionalOverEquityExposure` |
| Very large position | Scale by capital; reject maxQty or insufficient depth | calculated/omitted | filter code or liquidity unavailable | NEW | no | warn | B if not executable | A | AUTO maxQty and million-capital liquidity tests |
| Very small position | Never round upward to make it tradable | omitted | minQty/minNotional code | NEW | no | no | B for leg | A | AUTO rounding/minimum tests |
| Delta below stepSize | Preserve current position; wait for larger target delta | selected no-op | `SKIPPED_DELTA_TOO_SMALL` | NEW | no | no | B for delta | A | AUTO calculator delta/rounding properties |
| Quantity rounds to zero | Omit only that leg | omitted | `SKIPPED_ROUNDED_TO_ZERO` | NEW | no | no | B for leg | A | AUTO calculator minimum tests |
| Below minNotional | Never increase quantity to meet Binance minimum | omitted | `SKIPPED_BELOW_MIN_NOTIONAL` | NEW | no | no | B for leg | A | AUTO `roundsDownAndNeverRaisesQuantityToMeetMinNotional` |
| Price changes before submit | Submit precomputed delta; persist expected/actual price and slippage | intent terminal or rejected by current filter | `EXECUTED` or `REJECTED_BY_BINANCE_FILTER` | no blind retry | if ambiguous | threshold alert | depends | A | AUTO slippage and fill evidence; CANARY_REQUIRED |
| Multiple small deltas | Recompute full target; send only once the rounded delta is positive | no-op intents are not submitted | delta-too-small/already-at-target | NEW | no | no | B until threshold | A | AUTO deterministic and cumulative partial tests |
| Rounding residual accumulation | Recompute from authoritative target/position, never add residual twice | deterministic target | rounding skip code | NEW | no | no | depends | A | AUTO generated rounding property and partial cumulative tests |
| Capital changes during RESIZE | Old intent remains immutable; new source identity recalculates target | persisted old intent or audited conflict | idempotency conflict on same identity | NEW | yes on conflict | yes | B on conflict | A | AUTO payload hash/conflict tests |
| Optional user position limit | Preserve incumbents and count other allocations; block only new positions | omitted new candidates | `SKIPPED_USER_POSITION_LIMIT` | NEW | no | no | B for new | A | AUTO four account-wide incumbent/selection tests |
| Source aliases collide on target+side | Do not subtract the same aggregate existing quantity twice | no entry; existing target preserved | `BLOCKED_TARGET_SYMBOL_COLLISION` | NEW after catalog fix | yes/operator | yes | B | A | AUTO `sourceAliasCollisionFailsClosedWithoutClosingTheExistingTarget` |

## Binance

| Case | Expected behavior | Durable final | reasonCode | Retry | Reconcile | Alert | Entry | Exit | Evidence |
|---|---|---|---|---|---|---|---|---|---|
| Missing alias | Omit leg before submit | no send | `SKIPPED_NO_BINANCE_ALIAS` | NEW | no | warn/catalog | B for leg | A | AUTO calculator and symbol resolver contracts |
| Suspended/unsupported symbol | Omit leg | no send | `SKIPPED_SYMBOL_NOT_SUPPORTED` | NEW | no | warn | B for leg | A | AUTO calculator filter tests |
| Filters changed | Revalidate current contract and round down; known rejection is final | omitted or intent `REJECTED` | `REJECTED_BY_BINANCE_FILTER` | NEW | no if definitive | warn | B | A | AUTO rejection taxonomy and max/filter tests; CANARY_REQUIRED |
| Leverage rejected | Do not retry same intent | intent `REJECTED` | `REJECTED_BY_BINANCE_LEVERAGE` | NEW | no | yes | B | A | AUTO stable rejection taxonomy |
| Margin mode incompatible | Do not retry same intent | intent `REJECTED` | `REJECTED_BY_BINANCE_MARGIN_MODE` | NEW | no | yes | B | A | AUTO stable rejection taxonomy |
| One-way vs hedge | Map signed BOTH and explicit LONG/SHORT correctly | authoritative snapshot | mismatch code if contradictory | NEW | yes | yes on conflict | B on conflict | A | AUTO position endpoint and snapshot mapping tests |
| Partial fill | Persist cumulative fill once and keep reconciliation open | `PARTIALLY_FILLED` | `PARTIALLY_FILLED` | LOOKUP | yes | warn if old | B same intent | A | AUTO normalizer, persistence cumulative, PG partial tests |
| Fill without avgPrice | Accept economic effect, mark price pending, never resend | FILLED/PERSISTENCE_PENDING until price resolution | `EXECUTED` with pending price | LOOKUP | yes | warn if exhausted | B duplicate | A | AUTO null avgPrice and price resolution tests |
| Transport timeout | Treat outcome as unknown | `RECONCILING` | `EXECUTION_TIMEOUT_RECONCILING` | LOOKUP/AUTH | yes | yes | B same intent | A | AUTO direct and wrapped timeout tests |
| HTTP 429 | Conservatively treat post-call outcome as ambiguous | `RECONCILING` | `EXECUTION_AMBIGUOUS_RECONCILING` | LOOKUP/AUTH | yes | warn | B same intent | A | same coordinator policy; CANARY_REQUIRED |
| HTTP 5xx | Never blindly resend | `RECONCILING` | `EXECUTION_AMBIGUOUS_RECONCILING` | LOOKUP/AUTH | yes | yes | B same intent | A | AUTO `httpFiveHundredAfterPossibleSendNeverRetriesBlindly` |
| Invalid timestamp | Refresh time through server-time policy; a known reject is terminal | rejected or reconciling if response lost | Binance taxonomy code | NEW after sync | when ambiguous | warn | B | A | server-time configuration; CANARY_REQUIRED |
| Duplicate clientOrderId | Query and reuse existing order | reused/reconciling/persisted | duplicate intent reason | LOOKUP | yes | only on payload conflict | B duplicate | A | AUTO same clientOrderId and restart identity tests |
| Order canceled | Zero fill is rejected; executed quantity remains an economic fill | `REJECTED` or fill state | Binance unknown/reconciliation code | NEW only if definitive | if any fill | warn | B old intent | A | AUTO canceled order normalizer/worker tests |
| Manual external close | Persist idempotent reconciliation event; do not create a fake fill | local operation closed, no Binance intent | `RECONCILED_CLOSE` / external-flat reason | no | position reconcile | warn | B until synced | A | AUTO target exit policy, ledger and architecture tests |
| Lost response | Keep intent and query by identifiers | `RECONCILING` | `EXECUTION_AMBIGUOUS_RECONCILING` | LOOKUP/AUTH | yes | yes | B same intent | A | AUTO malformed/lost response tests |
| Position exists without intent | Classify as manual/unmanaged exposure | no entry | `BLOCKED_EXISTING_EXPOSURE_CONFLICT` | NEW | yes | critical | B | A | AUTO manual exposure test |
| Intent exists without position | Query order first, then position; never synthesize a fill | reconciling/manual review or external-flat reconciliation | lookup/manual reason | LOOKUP | yes | yes if exhausted | B | A | AUTO reconciliation worker and external-flat tests |
| Position differs from expected | Fail closed against actual/managed/attributed aggregates | no entry | `BLOCKED_EXISTING_EXPOSURE_CONFLICT` | NEW | yes | critical | B | A | AUTO conflict, partial external close and shared allocation tests |

## Events

| Case | Expected behavior | Durable final | reasonCode | Retry | Reconcile | Alert | Entry | Exit | Evidence |
|---|---|---|---|---|---|---|---|---|---|
| Duplicate OPEN | Same intent causes at most one submit | reused/no-op | duplicate intent reason | LOOKUP | when non-terminal | no | B duplicate | A | AUTO duplicate, 100-event performance and two-replica tests |
| CLOSE before OPEN | Do not create a synthetic completed trade | skipped/quarantined | close-without-open reason | NEW | source reconcile | warn | B | A when position exists | AUTO cycle engine gap tests |
| INCREASE before OPEN persistence | Recover/finish OPEN before applying cumulative increase | persistence pending/reconciling | persistence reason | LOOKUP | yes | yes | B | A | AUTO crash and cumulative increase tests |
| REDUCE after CLOSE | Idempotent no-op; never make quantity negative | terminal operation unchanged | already applied/stale reason | no | no | no | B | A/no-op | AUTO close recovery and accounting tests |
| Repeated FLIP | Close once and do not open opposite before flat | close pending/reconciling | flip/stale reason | LOOKUP | yes | warn | B opposite | A | AUTO duplicate flip and authoritative-zero contracts |
| Out-of-order events | Partition by wallet+symbol and reject stale transitions | no-op/quarantine | stale/out-of-order reason | NEW | source snapshot | warn | B stale entry | A with current state | AUTO partition and stale shadow tests |
| Contradictory snapshot | Preserve target and block new exposure | no send | snapshot mismatch/conflict | NEW | yes | yes | B | A | AUTO source authority and target conflict tests |
| Stale event | Do not increase from old source state | no send | `BLOCKED_SOURCE_EVENT_STALE` policy | NEW | no | warn | B | A | stale source/resize tests and runtime guard |
| sourceEventId reused with changed payload | Persist hashes/diff; no send | intent/manual review + conflict row | `BLOCKED_IDEMPOTENCY_PAYLOAD_CONFLICT` | no | yes | critical | B | A | AUTO coordinator and PostgreSQL conflict tests |
| Kafka replay | Stable identity yields one economic effect | no-op/reused | duplicate event/intent reason | LOOKUP | if non-terminal | no | B duplicate | A | AUTO canonical event and dispatch replay tests |
| Two pods | Database uniqueness/locks choose one sender | one sending intent | duplicate/reconcile reason for loser | LOOKUP | loser queries | no | B loser | A | AUTO+PG18 race and SKIP LOCKED tests |
| Duplicate outbox | Consumer identity produces one effect | no-op | duplicate event reason | no | no | no | B duplicate | A | AUTO canonical Kafka/DB identity tests |
| Concurrent equity update | Require matching snapshot version | no send | `BLOCKED_SOURCE_SNAPSHOT_MISMATCH` | NEW | no | yes | B | A | AUTO authoritative equity/version tests |

## Recovery

| Case | Expected behavior | Durable final | reasonCode | Retry | Reconcile | Alert | Entry | Exit | Evidence |
|---|---|---|---|---|---|---|---|---|---|
| Crash before reservation | Transaction rolls back; Binance untouched | no intent or unreserved created row recovered | database failure | NEW | no | yes | B | A | AUTO database timeout before claim |
| Crash after reservation, before submit | On restart query clientOrderId before any possible submit retry | stale `DISPATCHING` -> reconciling/manual | reconciliation reason | LOOKUP/AUTH | yes | yes | B | A | AUTO restart worker/state policy; PG18 stale claims |
| Crash after submit | Never resend without lookup | `RECONCILING` | ambiguous execution reason | LOOKUP/AUTH | yes | critical | B | A | AUTO timeout and restart worker tests |
| Crash during partial fill | Resume from cumulative executed quantity | `PARTIALLY_FILLED` | `PARTIALLY_FILLED` | LOOKUP | yes | warn | B same intent | A | AUTO cumulative partial + PG18 progress uniqueness |
| Crash after fill | Recover known order and persist it once | FILLED -> PERSISTED | `EXECUTED` or timeout recovery | LOOKUP | yes | yes if delayed | B duplicate | A | AUTO process-crash recovery |
| Crash before local persistence | Rebuild operation and required ledger from response | PERSISTENCE_PENDING -> PERSISTED | `EXECUTED` / `RECONCILED_AFTER_TIMEOUT` | LOOKUP | yes | yes | B duplicate | A | AUTO recovery service and PostgreSQL idempotency |
| PostgreSQL temporarily down | Never call Binance if claim failed; after a fill keep intent pending | rollback or PERSISTENCE_PENDING | database/persistence reason | NEW or LOOKUP | yes after fill | critical | B | A | AUTO timeout/pool tests; CANARY_REQUIRED for real outage |
| Kafka down | Durable source/outbox remains pending; no duplicate economic effect | pending outbox/dead-letter | transport error | broker retry | no Binance blind retry | yes | depends on source delivery | A | outbox/DLQ tests; CANARY_REQUIRED |
| Binance down | Keep durable intent and enter reconciliation | `RECONCILING` | timeout/ambiguous code | LOOKUP/AUTH | yes | critical | B | A | AUTO gateway timeout; CANARY_REQUIRED |
| Reconciliation incomplete | Bound attempts then require operator review | `MANUAL_REVIEW`, reservation retained if ambiguous | exhausted/manual-review reason | LOOKUP only | yes | critical | B | A | AUTO exhausted order/price resolution tests |
| Partial fill on restart | Apply only `cumulative - persisted` delta and retain one ledger progress row per cumulative quantity | PARTIALLY_FILLED or PERSISTED | `PARTIALLY_FILLED`/`EXECUTED` | LOOKUP | yes | warn | B same intent | A | AUTO cumulative open/increase/reduce and PG18 partial tests |

## Additional Bugs Found And Corrected

1. Real target sizing used an empty target-position list. It now consumes the
   signed Binance position snapshot, all managed allocations, and exact
   allocation attribution.
2. An external manual close could leave local state open or send a redundant
   close. It now writes a required idempotent reconciliation event and no fake
   economic fill.
3. Entry eligibility erased the capital needed to calculate reductions. Entry
   gating and exit sizing are now independent.
4. A process crash between operation persistence and ledger persistence could
   apply a fill twice or fail to recover a closed operation. Recovery now uses
   cumulative fill minus already-applied state and can load the latest closed
   operation.
5. Runtime ambiguity wrote historical reason aliases, while promotion evidence
   queried only those aliases. Writers now use the V3 taxonomy and readiness
   reads both V3 and historical rows.
6. `userMaxConcurrentPositions` could evict an incumbent, ignore positions in
   other allocations, or waste a slot on an untradable candidate. It now
   blocks new positions only and the PostgreSQL reservation remains final.
7. Two source aliases mapping to one target side could subtract the same
   existing quantity twice. The collision is now fail-closed and preserves the
   incumbent target.

## Certification Consequence

Local deterministic behavior is covered, but rows marked `CANARY_REQUIRED`
are not real exchange evidence. They block `LIVE_CERTIFIED`. The highest honest
system gate remains `LOCAL_TEST_READY` until the a445 authoritative equity
replay, real MICRO_LIVE calibration, and controlled Binance/infrastructure
canaries are complete.
