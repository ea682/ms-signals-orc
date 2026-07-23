# Informe de estado del flujo de copy trading - 2026-07-22

## Conclusión ejecutiva

El flujo funcional solicitado está implementado en `ms-signals-orc`:

`SHADOW -> MICRO_LIVE -> CERTIFICACION GLOBAL -> LIVE POR USUARIO -> DEGRADACION -> RECERTIFICACION`.

La suite completa está verde y el B2B real secuencial de las cuentas `MICRO_LIVE` y `LIVE` terminó correctamente. Sin embargo, el sistema completo todavía no debe declararse listo para producción porque el verificador Java de aislamiento no entiende aún la relación Binance cuenta principal -> subcuenta cuando ambas credenciales devuelven el mismo `accountAlias=uX`. El harness B2B sí lo demuestra mediante SAPI firmado, pero esa prueba no está integrada al hot path del runtime.

Estado global: `FUNCIONALMENTE IMPLEMENTADO, AUTOMATIZADO Y B2B DUAL VALIDADO; PRODUCCION BLOQUEADA POR AISLAMIENTO DE IDENTIDAD EN RUNTIME`.

## Estado por ambiente

| Ambiente | Qué hace | Avance | Evidencia | Frontera actual |
|---|---|---|---|---|
| SHADOW | Replica ciclos y contabilidad sin enviar dinero real. | Implementado y probado. | Coverage rolling, admisión de ciclos, perfiles independientes, locks distribuidos y promoción están cubiertos. | Falta observación de una ventana operativa desplegada; no es un bloqueo de código. |
| MICRO_LIVE | Prueba voluntaria por usuario con reserva de 100 USDC, x5 y sizing proporcional. | Implementado, probado y B2B real certificado. | 6 órdenes reales finales, 6 ACK, 6 fills, posición y órdenes finales en cero. | El runtime Java bloqueará el par real mientras no consuma la prueba principal/subcuenta. |
| CERTIFICACION GLOBAL | Una evidencia Micro-live real válida crea/aprueba una certificación por wallet/estrategia/versiones. | Implementado y probado automáticamente. | Servicio automático, transición CAS, auditoría inmutable y PostgreSQL 16. | No se creó una certificación productiva con el B2B directo; el harness no escribe el catálogo productivo. |
| LIVE | Adopción individual, asignación nueva, gate permanente y sizing proporcional al capital del usuario. | Implementado, probado y B2B real certificado a nivel de cuenta. | 6 órdenes LIVE reales, segundo ciclo, emergency stop, derisk y restauración de HEDGE. | No habilitar el runtime productivo hasta corregir la identidad principal/subcuenta. |
| DEGRADACION | Convierte Live afectada a `EXIT_ONLY`; bloquea OPEN/INCREASE y permite REDUCE/CLOSE. | Implementado y probado automáticamente. | Propagación transaccional, gate de ejecución y pruebas de transición. | Sin prueba de larga duración en un despliegue real. |
| RECERTIFICACION | Encola usuarios elegibles con prioridad y los readmite cuando existe capacidad. | Implementado y probado automáticamente. | Cola durable, prioridad, reserva de slots, preemption `IDLE_ONLY`, locks y no duplicación de LIVE. | Requiere habilitar workers y monitorear la primera operación productiva. |

## Cómo funciona el avance

### 1. Ingesta y SHADOW

Los eventos fuente se conservan para auditoría, métricas y reconstrucción. La capa de copy, en cambio, solo inicia un ciclo si recibe un `OPEN` posterior a `activation_at`. Un `INCREASE`, `REDUCE` o `CLOSE` sin OPEN admitido queda como NOOP con `OPEN_NOT_TRACKED_FOR_ALLOCATION_MODE` y no crea exposición local ficticia.

SHADOW procesa por perfil de estrategia y sin enviar órdenes a Binance. Mantiene perfiles independientes, usa coverage rolling de hasta 14 días y exige muestra suficiente, posiciones cerradas, wallet/usuario activos, decisión completa, copy guard abierto, símbolos resolubles e idempotencia. El worker está protegido con ShedLock para evitar promoción duplicada entre réplicas.

### 2. Promoción SHADOW -> MICRO_LIVE

Solo se considera al usuario que:

- habilitó `participateInMicroLive`;
- no bloqueó la wallet;
- está activo y tiene API `MICRO_LIVE` válida;
- tiene capital suficiente para respaldar la reserva completa;
- pasó la política Shadow y la decisión materializada.

La allocation nueva se enlaza de forma inmutable a `exchange_account_id` con purpose `MICRO_LIVE`. No existe fallback hacia la credencial LIVE.

La capacidad se calcula dinámicamente:

`floor((walletBalance - safetyBuffer) / 100 USDC)`

`MICRO_LIVE_MAX_CONCURRENT_ALLOCATIONS=0` significa que no hay un techo fijo oculto. Las pruebas confirman 500 -> 5 slots y 1.000 -> 10 slots. Con el balance firmado posterior al B2B de `499.61546555 USDC`, buffer cero y budget 100, la capacidad teórica actual sería 4 slots. El harness directo no crea reservas en la base productiva, por lo que no se infiere aquí cuántos slots están ocupados.

### 3. Ejecución MICRO_LIVE

El presupuesto de la allocation es 100 USDC y el leverage objetivo es x5. No abre siempre 100 USDC: replica el porcentaje de margen usado por la wallet origen.

`sourceUsage = sourceMargin / sourceEquity`

`targetMargin = 100 * sourceUsage`

`targetNotional = targetMargin * 5`

Antes del HTTP real se aplican reglas Binance fail-closed: estado TRADING, `MARKET_LOT_SIZE` antes de `LOT_SIZE`, step size, min/max quantity, min/max notional, leverage bracket, margen y modos. OPEN/INCREASE por debajo del mínimo se bloquean sin inflar cantidad. REDUCE/CLOSE se limitan a la menor cantidad entre lo pedido, lo poseído por la allocation y lo observado en Binance.

Ownership se mantiene por cuenta, símbolo, allocation y ciclo. Un aumento omitido no se recupera durante un REDUCE/CLOSE. FLIP se ejecuta como saga: cerrar pierna anterior, confirmar flat, crear identidad de ciclo nueva y solo entonces abrir el lado nuevo.

### 4. Evidencia y certificación global

La promoción Micro-live exige órdenes realmente enviadas, fills y operaciones cerradas; eventos genéricos de runtime no son suficientes. La calidad de ejecución se calcula por round trip completo usando retornos origen vs copia. La diferencia directa de precio entre exchanges no actúa como gate rígido y una muestra incompleta se excluye.

Una prueba Micro-live real y válida puede aprobar la certificación global. La identidad incluye wallet, estrategia, versión, scope, exchange, quote, leverage y versiones de sizing/mapping/costos. Una aprobación administrativa no puede saltarse la evidencia técnica.

### 5. Adopción LIVE por usuario

Cuando la certificación pasa a `LIVE_APPROVED`:

- los participantes Micro-live que desean continuar cierran o quedan `EXIT_ONLY` hasta estar flat, conservan su historia y reciben una nueva allocation LIVE;
- los usuarios sin Micro-live pueden recibir una allocation LIVE pausada si tienen `autoFollowCertifiedLive=true` y no bloquearon la wallet;
- la reconciliación observa balance, posiciones, API, distribución y adopción antes de activar;
- la adopción se asocia al ID de la allocation LIVE, no al ID Micro-live.

Cada OPEN/INCREASE LIVE vuelve a validar switches, canary/whitelist, usuario, preferencia, API, allocation, adopción y certificación exacta. REDUCE/CLOSE no dependen del gate de entrada para no impedir derisking.

### 6. Degradación y recertificación

`LIVE_DEGRADED`, `SUSPENDED` o `REVOKED` propagan inmediatamente `EXIT_ONLY` a las allocations asociadas. Esto bloquea OPEN/INCREASE, permite REDUCE/CLOSE y evita forzar cierres solo para liberar capacidad.

Los usuarios voluntarios elegibles entran en una cola durable `PENDING_CAPACITY`. La prioridad es recertificación degradada, luego Micro-live ya admitida y finalmente promoción nueva desde Shadow. La política `IDLE_ONLY` puede liberar una allocation Micro inactiva, pero nunca una con posición u órdenes pendientes. Al volver a `LIVE_APPROVED`, cada usuario se revalida; no existe reactivación ciega ni creación de una segunda LIVE.

## B2B real dual Binance

Evidencia autoritativa ignorada por Git: `.b2b-evidence/b2b-2565c5cc-6572-4977-b676-7f88778576f4.json`.

### MICRO_LIVE

- `execution_account_id`: `7f255183-c762-478f-99da-416ba831e2f8`
- identidad no sensible: alias Futures `uX`, probado como subcuenta de LIVE mediante SAPI firmado;
- símbolo: `ETHUSDC`;
- modo final: `ONE_WAY / CROSS / x5`;
- órdenes / ACK / fills: `6 / 6 / 6`;
- INCREASE de `0.001 ETH`: bloqueado localmente como `INCREASE_BELOW_MIN_NOTIONAL_NOOP`, sin HTTP;
- REDUCE y ambos CLOSE: `reduceOnly=true`;
- dos ciclos con identidades distintas;
- emergency stop: OPEN/INCREASE bloqueados, REDUCE/CLOSE permitidos;
- posición final / órdenes abiertas: `0 / 0`;
- contraparte LIVE intacta durante esta prueba.

### LIVE

- `execution_account_id`: `23972ef0-1fd7-4313-a9ee-05be70b6b961`
- identidad no sensible: cuenta principal Binance, alias Futures `uX`;
- símbolo: `ETHUSDC`;
- modo de prueba: `ONE_WAY / CROSS / x5`;
- modo final restaurado: `HEDGE / CROSS / x5`;
- órdenes / ACK / fills: `6 / 6 / 6`;
- INCREASE de `0.001 ETH`: bloqueado localmente como `INCREASE_BELOW_MIN_NOTIONAL_NOOP`, sin HTTP;
- REDUCE y ambos CLOSE: `reduceOnly=true`;
- dos ciclos con identidades distintas;
- emergency stop: OPEN/INCREASE bloqueados, REDUCE/CLOSE permitidos;
- posición final / órdenes abiertas: `0 / 0`;
- contraparte MICRO_LIVE intacta durante esta prueba.

### Límites globales observados

- margen máximo conjunto: `9.24110400 USDC`;
- notional máximo conjunto: `46.20552000 USDC`;
- leverage máximo: `x5`;
- posiciones simultáneas entre cuentas: no;
- posición final conjunta / órdenes abiertas: `0 / 0`;
- relación principal/subcuenta: `B2B_MASTER_SUBACCOUNT_RELATIONSHIP_PROVEN`;
- postflight independiente: ambas cuentas planas, cero órdenes, cero margen y cero notional.

## Bugs reproducidos con TDD

### HEDGE impedía iniciar el B2B LIVE

- Rojo: `DualAccountB2bHarnessContractTest.hedgeAccountIsTemporarilyNormalizedAndRestoredOnlyWhileFlat` falló dentro de una suite 13/13 esperada.
- Riesgo: LIVE nunca podía ser probado porque la cuenta principal está en HEDGE.
- Corrección: normalización temporal a ONE_WAY únicamente con cuenta plana/sin órdenes; restauración de HEDGE al final; `Get-Position` entiende snapshots HEDGE.
- Verde: 13 pruebas del harness, 0 fallos.

### Restauración repetida devolvía Binance `-4059`

- Evidencia real: `No need to change position side` después de completar/limpiar el flujo.
- Rojo: la regresión exigió que la restauración detectara que el modo original ya estaba activo.
- Corrección: `Restore-AccountPositionMode` es idempotente y no llama a Binance si el snapshot ya coincide.
- Verde: 13 pruebas del harness, 0 fallos; siguiente B2B dual completo exitoso.

### Regresiones anteriores conservadas

- lookup inmediatamente posterior al ACK podía devolver `-2013`; ahora reintenta el mismo `clientOrderId` sin reenviar;
- Binance podía informar margen inicial cero con posición no cero; el harness usa conservadoramente `abs(qty * markPrice) / leverage`;
- aliases Futures iguales solo son aceptados por el harness tras prueba SAPI principal/subcuenta;
- `MicroLiveOnly` no puede invocar ni limpiar la cuenta LIVE.

## Brecha de producción confirmada

`ExecutionAccountIsolationVerifier` consulta el balance Futures y usa `accountAlias` como referencia externa. Si LIVE y MICRO_LIVE devuelven el mismo alias, responde `EXECUTION_ACCOUNTS_NOT_ISOLATED`. En las cuentas reales ambas devuelven `uX`, aunque SAPI firmado demostró que una es principal y la otra subcuenta.

Además, `V202607180006__binance_execution_account_isolation.sql` crea una unicidad sobre `exchange_account_ref`; guardar `uX` para ambas también colisionaría.

Impacto: el harness directo está certificado, pero una orden real que atraviese `ms-signals-orc -> ExecutionAccountResolver -> ExecutionAccountIsolationVerifier -> ms-binance-engine` quedaría bloqueada antes del submit. No se debilitó este guard aceptando solo claves distintas, porque dos API keys diferentes pueden pertenecer a la misma cuenta.

Corrección necesaria para producción:

1. exponer desde `ms-binance-engine` una verificación SAPI firmada principal/subcuenta;
2. consumirla desde `ExecutionAccountIsolationVerifier` cuando el alias Futures coincida;
3. persistir una identidad estable, purpose-aware y respaldada por esa prueba, sin depender del alias compartido;
4. ajustar la constraint/migración para la nueva identidad;
5. sellar rojo/verde, PostgreSQL 16 y B2B a través del runtime de servicios, no solo el harness directo.

## Pruebas ejecutadas

### ms-signals-orc

- Java: Temurin 21.0.8;
- Maven `package`: éxito;
- tests: `787`;
- failures: `0`;
- errors: `0`;
- skips: `5`;
- PostgreSQL: `16.14` mediante Testcontainers;
- baseline Flyway productivo completo: verde;
- artifact: `target/ms-signals-orc-1.4.28.jar`.

Skips:

1. benchmark de concurrencia CPU: requiere `copy.benchmark.enabled`;
2. plan read-only de dead-letter: requiere `copy.postgres.test.jdbc-url` externo;
3. contrato HTTP real Metric V2/Nest: requiere `metric.v2.http.base`;
4. benchmark Metric V2: requiere `metric.v2.benchmark.enabled=true`;
5. integración PostgreSQL aislada Metric V2: requiere `metric.v2.pg.url`.

Las integraciones relevantes para este flujo no fueron omitidas: Flyway completo, capacidad/cuenta, certificación, coverage y preflight Binance pasaron con PostgreSQL/Testcontainers.

### ms-binance-engine

- tests: `40`;
- failures/errors/skips: `0 / 0 / 0`.

### Seguridad e higiene

- parse PowerShell: verde;
- `git diff --check`: verde;
- coincidencias de las credenciales compartidas dentro de archivos versionados: `0`;
- `.env.b2b`, `.env.execution-accounts`, `.b2b-run-state.json` y `.b2b-evidence/`: ignorados por Git;
- secretos versionados: no detectados;
- worktree: contiene el conjunto amplio de cambios aún sin commit y una eliminación staged previa de `.env.prod`.

## Migraciones agregadas

| Migración | Objetivo |
|---|---|
| `V202607170001` | Preferencias, certificación global, adopción y activación LIVE. |
| `V202607170002` | Calidad de ejecución round-trip. |
| `V202607180001` | Unicidad de ciclos abiertos por allocation/modo. |
| `V202607180002` | Purpose/cuenta de ejecución, capacidad, ownership, lineage y saga FLIP. |
| `V202607180003` | Validación diferida de constraints de cuenta. |
| `V202607180004` | Tabla/locks distribuidos para workers críticos. |
| `V202607180005` | Capacidad dinámica y cola de recertificación. |
| `V202607180006` | Referencia externa e inmutabilidad de cuenta Binance; requiere evolución para principal/subcuenta con alias igual. |

Son migraciones forward-only. La recuperación operativa debe hacerse desplegando primero una versión compatible y aplicando una migración correctiva; no se recomienda intentar rollback SQL destructivo sin snapshot de base.

## Configuración y despliegue recomendado

Los defaults son fail-closed: submit real apagado, Micro-live apagado, Live apagado/dry-run, jobs críticos apagados y B2B apagado. `.env.prod.example` documenta 100 USDC/x5, capacidad dinámica, coverage 14 días, workers y switches.

Orden de avance recomendado:

1. desplegar migraciones y SHADOW con órdenes reales apagadas;
2. observar coverage/ciclos/locks y promociones sin habilitar LIVE;
3. corregir y desplegar la prueba de identidad principal/subcuenta en el runtime;
4. habilitar MICRO_LIVE para un canary y verificar reservas, órdenes y reconciliación;
5. permitir certificación global automática solo con evidencia real completa;
6. habilitar adopción LIVE en modo dry-run y whitelist;
7. pasar LIVE canary a submit real con límites y monitoreo;
8. probar degradación/recertificación operativa antes de escalar.

## Veredicto

- SHADOW: listo para despliegue controlado.
- MICRO_LIVE: lógica y B2B real certificados; runtime productivo bloqueado por identidad principal/subcuenta.
- CERTIFICACION/ADOPCION: implementadas y verdes en automatización/PostgreSQL.
- LIVE: B2B real dual certificado; no habilitar el runtime productivo aún.
- DEGRADACION/RECERTIFICACION: implementadas y verdes; requieren smoke operacional tras despliegue.
- Producción completa: `NO` hasta cerrar el guard de identidad en runtime y ejecutar un B2B de servicio a servicio.

Las claves pegadas en el chat deben revocarse y rotarse al terminar esta validación.
