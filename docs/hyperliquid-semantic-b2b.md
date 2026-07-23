# Hyperliquid semantic B2B local

This local harness verifies the cross-repository contract without booting the
complete system, connecting `USER_FILL`, or sending external orders.

The executable chain is:

`production fixture -> Sentinel detector -> Sentinel payload contract -> Signals
DTO/mapper -> final classification -> operation movement entity -> Shadow and
metric-outbox contracts`.

Run from `ms-signals-orc`:

```powershell
.\scripts\run-hyperliquid-semantic-b2b.ps1
```

The harness runs the producer-side Sentinel tests first and the Signals
consumer, ledger, Shadow, and outbox tests second. It fails immediately if
either repository breaks the contract and prints the coverage map for
`B2B-01` through `B2B-12`.

The `B2B-07` test only proves that a future authoritative `USER_FILL` can carry
the required two-leg FLIP economics. It does not connect or activate that
publisher.
