# ms-signals-orc

Orquestador de copytrading para operaciones Futures. Recibe eventos de operaciones origen, calcula el tamaño que corresponde a cada usuario, abre/cierra posiciones en `ms-binance-engine` y mantiene en memoria el estado caliente para evitar consultas innecesarias en la ruta crítica.

## Cambios importantes de capital Futures

Este proyecto ahora mantiene actualizado el campo `futuros_operaciones.detail_user.capital` usando el saldo disponible real de la cuenta Binance Futures del usuario.

Cada 10 minutos el scheduler revisa usuarios activos con API Binance activa:

1. Lee la moneda configurada en `detail_user.capital_asset`.
2. Solo permite `USDT` o `USDC`.
3. Consulta el saldo disponible de esa moneda en Futures usando `ms-binance-engine`.
4. Consulta cuánto BNB tiene el usuario en Futures.
5. Valora ese BNB contra `BNBUSDT` o `BNBUSDC`.
6. Si el valor de BNB es menor al 3% del saldo disponible, convierte el 10% del saldo disponible a BNB.
7. Si el saldo disponible es menor a `50` en la moneda elegida, no convierte.
8. Siempre actualiza `detail_user.capital` con el saldo disponible final cuando el usuario está activo.
9. Actualiza el capital en memoria para que nuevas operaciones de copytrading usen el monto nuevo sin esperar el TTL del cache.

Ejemplo: si el usuario tiene `100 USDT` disponibles y solo `2.53 USDT` aproximados en BNB, el BNB está bajo el 3%. El proceso convierte `10 USDT` a BNB y luego actualiza el capital.

## Query de base de datos

Flyway ejecuta la migración:

```sql
ALTER TABLE futuros_operaciones.detail_user
    ADD COLUMN IF NOT EXISTS capital_asset varchar(4) NOT NULL DEFAULT 'USDT';

UPDATE futuros_operaciones.detail_user
SET capital_asset = 'USDT'
WHERE capital_asset IS NULL OR trim(capital_asset) = '';

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'chk_detail_user_capital_asset'
    ) THEN
        ALTER TABLE futuros_operaciones.detail_user
            ADD CONSTRAINT chk_detail_user_capital_asset
            CHECK (capital_asset IN ('USDT', 'USDC'));
    END IF;
END $$;

COMMENT ON COLUMN futuros_operaciones.detail_user.capital_asset IS
    'Moneda estable usada para capital, copytrading y mantenimiento automatico de BNB. Valores permitidos: USDT o USDC.';
```

Para cambiar un usuario a USDC:

```sql
UPDATE futuros_operaciones.detail_user
SET capital_asset = 'USDC'
WHERE id_detail_usr = '<uuid-del-detail-user>';
```

## Configuración

Variables principales:

| Variable | Default | Descripción |
|---|---:|---|
| `FUTURES_CAPITAL_MAINTENANCE_ENABLED` | `true` | Enciende/apaga el scheduler de capital y BNB. |
| `FUTURES_CAPITAL_MAINTENANCE_INITIAL_DELAY_MS` | `30000` | Espera inicial antes del primer ciclo. |
| `FUTURES_CAPITAL_MAINTENANCE_FIXED_DELAY_MS` | `600000` | Frecuencia del ciclo. `600000` = 10 minutos. |
| `FUTURES_CAPITAL_MAINTENANCE_MIN_AVAILABLE` | `50` | Saldo mínimo para permitir conversión a BNB. |
| `FUTURES_CAPITAL_MAINTENANCE_BNB_MIN_RATIO` | `0.03` | Mínimo de BNB requerido respecto al saldo disponible. |
| `FUTURES_CAPITAL_MAINTENANCE_CONVERSION_RATIO` | `0.10` | Porcentaje del saldo disponible a convertir si falta BNB. |
| `FUTURES_CAPITAL_MAINTENANCE_BNB_PRICE_BASE_URL` | `https://fapi.binance.com` | URL pública para obtener precio BNB. |
| `FUTURES_CAPITAL_MAINTENANCE_BNB_PRICE_TIMEOUT_MS` | `1000` | Timeout de consulta de precio BNB. |
| `FUTURES_CAPITAL_MAINTENANCE_BNB_PRICE_CACHE_TTL_MS` | `60000` | Cache local del precio BNB. |

## Copytrading y moneda elegida

El copytrading usa `detail_user.capital_asset` para elegir el par Futures:

- Usuario con `capital_asset = 'USDT'`: intenta operar pares `...USDT`.
- Usuario con `capital_asset = 'USDC'`: intenta operar pares `...USDC`.

Si el símbolo no existe en Binance para la moneda elegida, la operación se salta con una excepción controlada `SkipExecutionException` y queda logueada con `reasonCode=symbol_alias_not_found` o una regla equivalente. No se hace fallback silencioso a otra moneda porque eso usaría un capital distinto al configurado por el usuario.

### FLIP con Hyperliquid y Binance

Para eventos `FLIP`, el sistema compara por **wallet + activo base**, no por símbolo completo. Esto evita que un cambio de lado se pierda cuando Hyperliquid envía `BTCUSD` y la copia guardada en Binance está como `BTCUSDT` o `BTCUSDC`.

Ejemplo seguro:

```text
Evento Hyperliquid: BTCUSD SHORT
Copia activa usuario USDC: BTCUSDC LONG
Resultado: se reconoce como el mismo activo base BTC, se cierra LONG y se abre SHORT.
```

Esta comparación se aplica tanto en la ruta directa como en el fallback de jobs, para que ambos caminos tengan la misma regla de negocio.

## Logs

Los logs tienen formato simple y explican cada paso con `friendlyStep`:

```text
event=futures.capital_maintenance.start friendlyStep=voy_a_revisar_saldos_y_bnb_de_los_usuarios_activos

event=futures.capital_maintenance.convert.start friendlyStep=el_bnb_es_muy_bajito_y_convierto_el_10_por_ciento

event=futures.capital_maintenance.capital.updated friendlyStep=guarde_el_capital_actual_para_que_copytrading_lo_use_rapido_en_memoria
```

No se imprime `api_key` ni `secret` en logs.

## Endpoints usados en ms-binance-engine

El scheduler llama a estos endpoints internos:

```http
GET /api/binance/futures/wallet/asset-balance?asset=USDT
GET /api/binance/futures/wallet/asset-balance?asset=USDC
GET /api/binance/futures/wallet/asset-balance?asset=BNB
POST /api/binance/futures/wallet/convert-to-bnb
```

Los endpoints requieren:

```http
X-BINANCE-APIKEY: <api_key>
X-BINANCE-SECRET: <api_secret>
```

## Producción

Checklist recomendado:

- Ejecutar migraciones Flyway antes de levantar la versión nueva.
- Confirmar que `ms-binance-engine` tenga disponibles los endpoints de wallet y conversión.
- Activar solo API keys con permisos Futures necesarios.
- Mantener Swagger desactivado por defecto en producción.
- Monitorear `event=futures.capital_maintenance.done` y `event=futures.capital_maintenance.user.fail`.
- Validar que usuarios USDC solo operen símbolos USDC disponibles en Binance Futures.

## Build

```bash
./mvnw clean verify
```

En entornos sin acceso a Maven Central, el wrapper no podrá descargar Maven. En CI/CD se recomienda usar una imagen con Maven preinstalado o cache interno de dependencias.
