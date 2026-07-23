# Binance dual-account B2B - actualización 2026-07-22

Estado: `DUAL_ACCOUNT_B2B_CERTIFIED`.

El informe histórico MICRO_LIVE fue reemplazado por la validación dual secuencial exigida. El detalle completo se encuentra en `docs/reports/copy-trading-environment-readiness-20260722.md`.

## Resultado final

- MICRO_LIVE: 6 órdenes, 6 ACK, 6 fills, dos ciclos, final `0 posiciones / 0 órdenes`.
- LIVE: 6 órdenes, 6 ACK, 6 fills, dos ciclos, final `0 posiciones / 0 órdenes`.
- INCREASE bajo mínimo: bloqueado localmente en ambos modos, sin HTTP.
- REDUCE/CLOSE: `reduceOnly=true`.
- margen global máximo: `9.24110400 USDC`.
- notional global máximo: `46.20552000 USDC`.
- leverage máximo: `x5`.
- posiciones simultáneas: no.
- aislamiento: `B2B_MASTER_SUBACCOUNT_RELATIONSHIP_PROVEN`.
- LIVE restauró su modo original `HEDGE`; MICRO_LIVE permaneció `ONE_WAY`.
- postflight firmado independiente: ambas cuentas planas, cero órdenes, exposición global cero.

Evidencia local ignorada por Git: `.b2b-evidence/b2b-2565c5cc-6572-4977-b676-7f88778576f4.json`.

## Automatización

- Harness: 13 tests, 0 fallos.
- `ms-signals-orc`: 787 tests, 0 fallos, 0 errores, 5 skips opt-in.
- `ms-binance-engine`: 40 tests, 0 fallos, 0 errores, 0 skips.

## Frontera

Este resultado certifica el harness B2B dual y las cuentas ejercitadas. No certifica todavía el hot path productivo de `ms-signals-orc`: su verificador Java sigue tratando el alias Futures compartido `uX` como cuentas no aisladas y aún no consume la prueba SAPI principal/subcuenta.

Las credenciales compartidas en el chat deben rotarse después de la prueba.
