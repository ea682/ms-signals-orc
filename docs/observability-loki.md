# Observabilidad Loki / Grafana

El servicio emite logs en formato logfmt para que Loki/Grafana pueda parsearlos con `| logfmt`.

## Campos base

El `logging.pattern.console` agrega estos campos al inicio de cada linea:

- `ts`
- `level`
- `app`
- `env`
- `thread`
- `logger`
- `traceId`

Los logs de negocio mantienen el campo `event`, por ejemplo:

- `event=hyperliquid.origin_store.skipped reasonCode=late_adjustment_without_active_origin`
- `event=hyperliquid.direct_copy.business_skip reasonCode=resize_without_open_copy`
- `event=copy.execution.enqueued copyIntent=ADJUST deltaType=RESIZE`
- `event=http.error code=VALIDATION_ERROR status=400`

## Consultas LogQL utiles

```logql
{app="ms-signals-orc"} | logfmt | event="hyperliquid.origin_store.skipped" | reasonCode="late_adjustment_without_active_origin"
```

```logql
{app="ms-signals-orc"} | logfmt | event="hyperliquid.direct_copy.business_skip" | reasonCode="resize_without_open_copy"
```

```logql
{app="ms-signals-orc"} | logfmt | event="copy.execution.enqueued" | copyIntent="ADJUST"
```

```logql
{app="ms-signals-orc"} | logfmt | event="http.error" | status >= 500
```

## Variables recomendadas en produccion

```bash
SPRING_APPLICATION_NAME=ms-signals-orc
APP_ENV=prod
SPRING_OUTPUT_ANSI_ENABLED=NEVER
HYPERLIQUID_DIRECT_INGEST_DEDUPE_ENABLED=true
HYPERLIQUID_ORIGIN_STORE_SKIP_LATE_ADJUSTMENTS=true
```

## Consultas adicionales para los ajustes finales

Duplicados colapsados por dedupe estable:

```logql
{app="ms-signals-orc"} | logfmt | event="hyperliquid.direct_ingest.duplicate" | deltaType="RESIZE"
```

Ajustes bloqueados sin copia activa, con accion legible:

```logql
{app="ms-signals-orc"} | logfmt | event="hyperliquid.direct_copy.business_skip" | action="ADJUST" | reasonCode="resize_without_open_copy"
```

RESIZE huerfano que origin_store no debe persistir como OPEN:

```logql
{app="ms-signals-orc"} | logfmt | event="hyperliquid.origin_store.skipped" | reasonCode=~"late_adjustment_without_active_origin.*"
```
