# SDD - TargetPortfolioCalculator V3

The integration source of truth is
`ms-signals-orc/docs/specs/copy-trading-proportional-portfolio-v3-sdd.md`.

This module is pure Java 21. It has no framework, clock, network, database or
queue dependency. Callers must provide `calculatedAt`, source observations,
target account state, exchange filters and model versions explicitly.

The calculator is deterministic and side-effect free. All output legs are
sorted by target symbol, side and source leg identity. Every omitted leg has a
specific reason. The module cannot create orders or reserve capital.
