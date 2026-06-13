# Copy trading runtime strategy validation

## Objetivo

El runtime ahora trata una copia como:

```text
usuario + wallet/origen + side + strategyCode
```

Esto permite copiar la misma wallet por caminos distintos, por ejemplo:

```text
0x... + SHORT_ONLY
0x... + MOVEMENT_ALL
0x... + RECENT_30D
```

sin mezclar estados ni idempotency keys.

## Logs principales para Loki

- `event=copy_route.targets_resolved stage=target_selection`
  - Explica a que usuarios/estrategias se va a intentar copiar.
  - Campos clave: `routeKind`, `wallet`, `side`, `deltaType`, `targetUsers`, `targetStrategies`.

- `event=copy_guard.blocked stage=target_selection`
  - La estrategia existe, pero el guard la pauso por PnL/metricas/cooldown.
  - Campos clave: `allocationId`, `userId`, `wallet`, `strategy`, `status`, `reason`, `cooldownUntil`.

- `event=hyperliquid.direct_copy.business_skip category=copy`
  - El evento llego, pero no se copio por lifecycle: sin copia abierta, ajuste sin estado, guard, etc.
  - Campos clave: `reasonCode`, `copyImpact`, `deltaType`, `source`.

- `event=copy.open.order.send category=copy`
  - Se envia apertura real/shadow.
  - Campos clave: `traceId`, `userId`, `wallet`, `symbol`, `strategy`, `allocationId`, `qty`, `clientOrderId`.

- `event=copy.close.order.send category=copy`
  - Se envia cierre/reduccion reduceOnly.
  - Campos clave: `strategy`, `allocationId`, `copyImpact`, `clientOrderId`.

- `event=rebalance.copy.* category=rebalance`
  - RESIZE/FLIP/reconciliacion de basket.
  - `open_send`, `increase_send`, `reduce_send`, `reduce_to_close_ok`, `reopen_send` tienen semantica diferente.

## Validaciones SQL

Ejecutar:

```text
src/main/resources/db/validation/copy_strategy_runtime_validation.sql
```

La primera consulta debe devolver cero filas. La segunda puede devolver filas y solo indica que una misma wallet se copia con varias estrategias, que ahora es permitido.
