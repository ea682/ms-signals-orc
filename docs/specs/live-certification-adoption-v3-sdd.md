# SDD - LIVE Certification And User Adoption V3

Status: `SPEC_APPROVED_FOR_IMPLEMENTATION`

This specification closes the COLD certification path and the HOT LIVE entry
gate. It extends the proportional portfolio V3 specification. It never enables
LIVE feature flags, deploys services, or submits an order as part of
certification.

## 1. Unit And Exact Identity

The global certification unit is not a wallet. Its immutable identity is:

```text
walletId + strategyCode + strategyVersion + scopeType + scopeValue
+ capitalBandMin + capitalBandMax + targetLeverage + exchange + quoteAsset
+ sizingPolicyVersion + symbolMappingVersion + feeModelVersion
+ fundingModelVersion + slippageModelVersion + liquidityModelVersion
```

Every string is trimmed and normalized before persistence. Capital and
leverage comparisons are numeric. An approval for one band, quote, leverage,
scope, strategy version, or model version cannot authorize another one.

## 2. Invariants

- Initial and subsequent promotion is manual by default.
- `copy.live-promotion.enabled` and its scheduled job default to `false`.
- A manual transition requires actor, reason, expected prior status and a
  non-empty evidence snapshot.
- Transition history is append-only and idempotent by `transitionKey`.
- A stale expected version or prior state cannot overwrite a newer decision.
- `LIVE_APPROVED` authorizes only exact identities and only after a valid user
  adoption exists for the same certification and allocation.
- One rejected user adoption never changes global certification evidence.
- User adoption is fail-closed when missing, rejected, expired or inconsistent.
- LIVE OPEN, INCREASE, RESIZE and the opening leg of FLIP require certification
  and adoption. REDUCE, CLOSE and the closing leg of FLIP never do.
- Runtime switches still apply to entry. Certification never turns a feature
  flag on.
- LIVE has no SHADOW or simulation fallback.

## 3. Preconditions

Global certification creation requires every identity field, a valid capital
band, one of the documented evidence levels and one of the documented states.
Creation cannot start in `LIVE_APPROVED`, `LIVE_DEGRADED`, `SUSPENDED` or
`REVOKED`; approval must be a separate audited transition.

A transition requires:

```text
certificationId
expectedVersion
expectedPriorStatus
nextStatus
actor
reason
evidenceSnapshot
transitionKey
```

User adoption validation requires authoritative account observations for:
balance, assigned capital, leverage, quote asset, margin mode, API permissions,
manual-position compatibility and personal risk policy. Missing observations
are failures, not assumed success.

## 4. States And Transitions

States:

```text
SOURCE_SHADOW_VALIDATING
EXECUTABLE_SHADOW_VALIDATING
MICRO_LIVE_VALIDATING
LIVE_APPROVED
LIVE_DEGRADED
SUSPENDED
REVOKED
```

Forward validation transitions are:

```text
SOURCE_SHADOW_VALIDATING -> EXECUTABLE_SHADOW_VALIDATING
EXECUTABLE_SHADOW_VALIDATING -> MICRO_LIVE_VALIDATING
MICRO_LIVE_VALIDATING -> LIVE_APPROVED
LIVE_DEGRADED -> LIVE_APPROVED
SUSPENDED -> LIVE_APPROVED
```

Safety transitions from any non-revoked state are:

```text
* -> LIVE_DEGRADED
* -> SUSPENDED
* -> REVOKED
```

`REVOKED` is terminal. Re-approval after revocation requires a new
certification identity/version; mutation of the revoked row is forbidden.

## 5. Postconditions

A successful transition atomically updates one certification row and appends
one immutable audit row containing actor, reason, prior/next state, evidence,
timestamp and transition key. Repeating the same transition key returns the
already applied outcome without a second audit row.

A successful adoption stores every individual validation flag, reason codes,
observation time and expiry. Adoption is usable only while all flags are true,
status is `VALID`, and `validatedAt <= now < expiresAt`.

Manual MICRO_LIVE to LIVE activation reuses the adopted allocation id. It is
allowed only with zero open copy positions and zero non-terminal dispatch
intents. This prevents a USD 100 at 5x economic cycle from changing policy in
the middle of the cycle. The mode update and immutable activation audit are one
transaction and idempotent by operator key.

The HOT gate returns one stable reason code. It does not perform remote calls;
it reads already validated local certification/adoption state.

## 6. Failures

Minimum reason codes:

```text
LIVE_CERTIFICATION_MISSING
LIVE_CERTIFICATION_NOT_APPROVED
LIVE_CERTIFICATION_IDENTITY_MISMATCH
LIVE_CERTIFICATION_VERSION_CONFLICT
LIVE_CERTIFICATION_ILLEGAL_TRANSITION
LIVE_CERTIFICATION_MANUAL_ACTOR_REQUIRED
LIVE_ADOPTION_MISSING
LIVE_ADOPTION_REJECTED
LIVE_ADOPTION_EXPIRED
LIVE_ADOPTION_IDENTITY_MISMATCH
LIVE_NOT_EXECUTED
```

Database unavailability at the HOT gate fails closed for entry with
`LIVE_CERTIFICATION_UNAVAILABLE`. It does not block REDUCE/CLOSE.

## 7. Edge Cases

- Overlapping capital bands must not yield an arbitrary certification; more
  than one exact match is a data error and fails closed.
- Boundary capital is inclusive on both ends.
- Capital changed after adoption invalidates the adoption when it leaves the
  certified band.
- Leverage, quote, strategy or model version changes require re-adoption or a
  different certification.
- Clock skew cannot make a future `validatedAt` usable.
- Manual/external positions are allowed only when the recorded compatibility
  check explicitly passed.
- A degraded/suspended/revoked certification blocks new exposure immediately
  but permits de-risking.
- A missing allocation identity cannot accidentally match a certification.

Payload conflict at the durable dispatch boundary stores prior hash, incoming
hash and a field-level diff built from a sanitized canonical payload. API keys
and secrets are never included. Non-terminal intents move to `MANUAL_REVIEW`;
terminal intents remain terminal. Every conflict blocks send and opens an
operator alert fact. Replays of the same conflicting hash increment one row and
cannot create another Binance submit.

## 8. Tests RED

- configuration and scheduled worker default to disabled;
- no certification blocks a LIVE entry;
- exact approved certification plus valid adoption allows an entry when all
  runtime feature flags and canary constraints pass;
- wrong scope, version, band, leverage, quote or model version blocks;
- missing, rejected, future or expired adoption blocks;
- degraded, suspended and revoked certification block entry;
- REDUCE/CLOSE remains allowed with no certification and with entry switches
  disabled, provided real dispatch is available for de-risking;
- transition compare-and-set rejects a stale writer;
- duplicate transition key creates one audit fact;
- revoked is terminal;
- one user's rejected adoption does not mutate global certification;
- database failure fails closed for entry and open for de-risking.
- duplicate idempotency key with a different request hash persists sanitized
  prior/incoming payloads plus field diff and never authorizes send;
- a conflicting non-terminal intent moves to `MANUAL_REVIEW` without releasing
  its reservation, while a terminal intent remains terminal;
- repeating the same incoming conflict hash increments the existing alert fact
  instead of creating duplicate rows.

## 9. Acceptance Criteria

- No scheduled or bulk service can create LIVE allocations by default.
- The production entry path calls the exact local certification/adoption gate
  before any real Binance entry submit.
- All LIVE entry decisions are explainable by stable reason code.
- No certification test or API activates the global LIVE switches.
- Existing SHADOW and MICRO_LIVE behavior is unchanged.
