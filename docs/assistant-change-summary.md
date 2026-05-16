# Cambios aplicados

## Proteccion lifecycle Hyperliquid origin_store

- Solo `OPEN` puede crear un lifecycle nuevo en `activeOriginIds`.
- `RESIZE`, `UPDATE` y `FLIP` intentan recuperar un lifecycle activo desde cache o desde `futures_position` abierta.
- Si no existe lifecycle activo, se registra skip y no se persiste una nueva `futures_position OPEN`.
- Se agregaron metricas/logs para `late_adjustment_without_active_origin` y `late_adjustment_without_active_origin_after_queue`.

## Kafka legacy / job ingest

- El log `copy.execution.enqueued` ahora incluye `symbol`, `action`, `copyIntent` y `deltaType`.
- Un `RESIZE` que internamente siga llegando como `tipo=ABIERTA` queda observable como `copyIntent=ADJUST deltaType=RESIZE`.

## Excepciones globales

- Se amplio `GlobalExceptionHandler` con handlers especificos para validacion, parametros, JSON malformado, metodo HTTP, media type, rutas no encontradas y errores de base de datos.
- Se evita exponer stacktrace o mensajes internos en respuestas 5xx.
- Los logs HTTP quedan con `traceId`, `status`, `code`, `method`, `path`, `remoteIp`, `userAgent`, `errClass` y `errMsg`.

## Loki / Grafana

- El patron de consola queda en formato `logfmt`.
- Se agrega `CorrelationIdFilter` para propagar/generar `X-Trace-Id` y poblar MDC.
- Se desactiva ANSI por defecto para evitar caracteres de color en Loki.
- Se agrego `docs/observability-loki.md` con consultas LogQL.

## Configuracion productiva agregada

- `HYPERLIQUID_DIRECT_INGEST_DEDUPE_ENABLED=true` por defecto.
- `HYPERLIQUID_ORIGIN_STORE_SKIP_LATE_ADJUSTMENTS=true` por defecto.
- `server.error.include-message/include-binding-errors/include-stacktrace=never`.

## Validacion local

No se pudo ejecutar Maven en este entorno porque el wrapper necesita descargar Maven desde `repo.maven.apache.org` y la red no esta disponible. Se verifico sintaxis YAML y `git diff --check` sobre los archivos modificados por estos cambios.

## Ajustes finales solicitados

### Logs `ADJUST` sin ambiguedad

- Los logs del direct path y del Kafka legacy ahora muestran `action=ADJUST` o `action=FLIP` cuando el `deltaType` representa un ajuste.
- Se mantiene `engineAction=OPEN` para dejar trazabilidad de la accion interna que sigue usando el job executor sin cambiar el contrato de persistencia.
- Ejemplo esperado:

```text
event=hyperliquid.direct_copy.business_skip action=ADJUST engineAction=OPEN copyIntent=ADJUST deltaType=RESIZE reasonCode=resize_without_open_copy
```

### Dedupe estable para bursts de ajustes

- El direct ingest ya no deduplica ajustes solo por `idempotencyKey` entrante.
- Para `RESIZE`, `UPDATE` y `FLIP`, el `dedupeKey` se construye con posicion, tipo de delta, `sourceTs`, `sizeQty`, notional/margin y precio normalizados.
- Esto evita procesar varias veces el mismo estado de posicion cuando el publisher envia idempotency keys distintas para un mismo burst.
- Los logs `accepted`, `duplicate`, `processed` y `failed` incluyen `dedupeKey` para consultar Loki.

### Hardening prod

- `spring.jpa.open-in-view=false` por defecto.
- SpringDoc y Swagger UI quedan deshabilitados por defecto, habilitables por variables de entorno.
- Las caches Spring `usersCache` y `binanceSymbols` ahora registran stats para Micrometer.
- El timeout REST de precio Binance sube de `350ms` a `1000ms` por defecto para reducir `Request cancelled` en ambientes con latencia.

### Tests agregados

- Se agrego test unitario para validar que dos `RESIZE` con distinta idempotency key pero mismo estado generan el mismo `dedupeKey`.
- Se valida que cambios reales de tamano generan `dedupeKey` distinto.
- Se valida que `OPEN` conserva la idempotency key publicada.
