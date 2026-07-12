package com.apunto.engine.shared.util;

import java.util.Locale;

/**
 * Catalogo central de diagnosticos humanos para copy trading.
 *
 * <p>Objetivo: que los logs expliquen causa, impacto y accion sin duplicar strings
 * en cada servicio. Para agregar un caso nuevo solo se agrega un case en {@link #advice(String, Context)}.
 */
public final class CopyLogAdvice {

    private CopyLogAdvice() {
    }

    public record Context(
            Integer activeCopyUsers,
            Integer eligibleUsers,
            Integer submittedTasks,
            Integer businessSkipped,
            Integer queueDepth,
            Boolean cacheActive,
            Integer activeCacheSize,
            String source
    ) {
        public static Context empty() {
            return new Context(null, null, null, null, null, null, null, null);
        }
    }

    public record Advice(
            String diagnosticCode,
            String severity,
            String humanMessage,
            String cause,
            String impact,
            String action,
            boolean expectedWhenNoActiveWallet,
            boolean shouldAlert
    ) {
        public String fields() {
            return "diagnosticCode=" + safeToken(diagnosticCode)
                    + " diagnosticArea=" + safeToken(diagnosticArea(diagnosticCode))
                    + " diagnosticSeverity=" + safeToken(severity)
                    + " humanMessage=" + quote(humanMessage)
                    + " cause=" + quote(cause)
                    + " impact=" + quote(impact)
                    + " recommendedAction=" + quote(action)
                    + " expectedWhenNoActiveWallet=" + expectedWhenNoActiveWallet
                    + " shouldAlert=" + shouldAlert;
        }
    }

    public static Context context(
            Integer activeCopyUsers,
            Integer eligibleUsers,
            Integer submittedTasks,
            Integer businessSkipped,
            Integer queueDepth,
            Boolean cacheActive,
            Integer activeCacheSize,
            String source
    ) {
        return new Context(activeCopyUsers, eligibleUsers, submittedTasks, businessSkipped, queueDepth, cacheActive, activeCacheSize, source);
    }

    public static String fields(String reasonCode) {
        return fields(reasonCode, Context.empty());
    }

    public static String fields(String reasonCode, Context context) {
        return advice(reasonCode, context).fields();
    }

    public static Advice advice(String reasonCode, Context context) {
        String code = normalize(reasonCode);
        Context ctx = context == null ? Context.empty() : context;
        boolean noActiveWallet = noActiveWallet(ctx);
        boolean queueBacklog = number(ctx.queueDepth()) >= 100;
        return switch (code) {
            case "distributed_duplicate_suppressed" -> new Advice(
                    code,
                    "INFO",
                    "Otra instancia adquirio primero la clave de idempotencia.",
                    "Otra instancia adquirio primero la clave de idempotencia para el mismo evento y payload.",
                    "Este proceso no enviara una segunda orden.",
                    "Ninguna accion, salvo que la tasa supere el umbral esperado.",
                    true,
                    false
            );
            case "dedupe_guard_unavailable" -> new Advice(
                    code,
                    "CRITICAL",
                    "No se pudo validar la idempotencia distribuida del evento.",
                    "El storage distribuido de deduplicacion no respondio o rechazo la operacion.",
                    "Si la politica es fail-open, existe riesgo de una orden duplicada; fail-closed bloquea la copia.",
                    "Revisar PostgreSQL y conectividad; confirmar la politica activa y reconciliar por clientOrderId.",
                    false,
                    true
            );
            case "summary_not_final_live_blocked" -> new Advice(
                    code,
                    "WARN",
                    "La decision SUMMARY todavia no es final para habilitar LIVE.",
                    "El resumen no materializo una decision equivalente a la simulacion FULL.",
                    "SHADOW puede continuar, pero no se abre ni aumenta exposicion LIVE con evidencia incompleta.",
                    "Esperar la simulacion FULL y revisar summaryConsistency si el bloqueo persiste.",
                    true,
                    false
            );
            case "negative_required_window_2w", "negative_required_window_1mo",
                 "non_positive_required_window_2w", "non_positive_required_window_1mo" -> new Advice(
                    code,
                    "WARN",
                    "Una ventana de rendimiento obligatoria no es positiva.",
                    "El PnL de 2w o 1mo no cumple el gate de promocion de la estrategia.",
                    "No se permite nueva exposicion real; SHADOW sigue reuniendo evidencia y los exits no se bloquean.",
                    "Esperar una nueva ventana valida; investigar solo si el PnL publicado contradice la auditoria.",
                    true,
                    false
            );
            case "data_stale_blocks_live", "data_stale_blocks_real_open" -> new Advice(
                    code,
                    "WARN",
                    "Los datos de la estrategia estan vencidos para autorizar una nueva entrada real.",
                    "La antiguedad o cobertura de la evidencia supera el limite permitido por readiness.",
                    "No se abre ni aumenta exposicion hasta refrescar la evidencia; reductions y closes siguen disponibles.",
                    "Verificar el refresco de metricas y cobertura si el estado stale persiste.",
                    true,
                    false
            );
            case "simulation_audit_failed" -> new Advice(
                    code,
                    "ERROR",
                    "La simulacion no supero su auditoria de consistencia.",
                    "Uno o mas invariantes de PnL, ventanas, sizing o reconciliacion no coinciden.",
                    "La promocion real queda bloqueada para evitar decidir con resultados inconsistentes.",
                    "Revisar simulationAudit.errors y corregir el calculo antes de promover la estrategia.",
                    false,
                    true
            );
            case "strategy_scope_not_matched" -> new Advice(
                    code,
                    "INFO",
                    "El evento no pertenece al scope configurado para esta estrategia.",
                    "El simbolo, lado o tipo de movimiento no coincide con la allocation.",
                    "Esta estrategia no enviara una orden para el evento; otras estrategias se evaluan por separado.",
                    "Ninguna accion, salvo que el scope configurado no sea el esperado.",
                    true,
                    false
            );
            case "micro_live_symbol_not_allowed" -> new Advice(
                    code,
                    "INFO",
                    "La allocation MICRO_LIVE no admite este simbolo.",
                    "El simbolo esta fuera del scope o no tiene un contrato Binance valido para la moneda del usuario.",
                    "No se enviara una nueva orden para este simbolo.",
                    "Revisar el symbol resolver y la configuracion de la estrategia solo si el simbolo deberia copiarse.",
                    true,
                    false
            );
            case "micro_live_guard_blocked" -> new Advice(
                    code,
                    "WARN",
                    "El guard de riesgo bloqueo una nueva entrada MICRO_LIVE.",
                    "La evidencia materializada de la estrategia contiene una condicion de riesgo activa.",
                    "No se abre ni aumenta exposicion; reductions y closes conservan su flujo independiente.",
                    "Consultar guardReasonCode y esperar nueva evidencia o revision de riesgo.",
                    true,
                    false
            );
            case "micro_live_min_notional_not_reached" -> new Advice(
                    code,
                    "INFO",
                    "La orden calculada no alcanza el minimo nocional permitido por Binance.",
                    "El step size, minQty, precio o presupuesto impiden formar una cantidad valida.",
                    "No se envia una orden que Binance rechazaria.",
                    "Ninguna accion, salvo revisar sizing si ocurre con frecuencia.",
                    true,
                    false
            );
            case "resize_without_open_copy", "update_without_open_copy", "adjustment_without_open_copy" -> adjustmentWithoutCopy(code, noActiveWallet, ctx);
            case "flip_without_open_copy" -> flipWithoutCopy(code, noActiveWallet);
            case "flip_without_open_copy_same_base_asset" -> flipWithoutCopySameBaseAsset(code, noActiveWallet, ctx);
            case "close_without_open_copy" -> closeWithoutCopy(code, noActiveWallet, ctx);
            case "late_adjustment_without_active_origin" -> lateAdjustmentWithoutOrigin(code, noActiveWallet, false, queueBacklog);
            case "late_adjustment_without_active_origin_after_queue" -> lateAdjustmentWithoutOrigin(code, noActiveWallet, true, queueBacklog);
            case "close_without_active_origin" -> closeWithoutActiveOrigin(code, noActiveWallet);
            case "copy_job_payload_missing", "delta_type_missing_for_copy_job", "delta_type_unknown_for_copy_job" -> new Advice(
                    code,
                    "REVIEW",
                    "Se bloqueo un job de copia antiguo porque no trae el tipo exacto de movimiento.",
                    "El flujo Kafka/legacy no trae deltaType confiable; con flujo directo activo podria duplicar una copia o copiar historico fuera de orden.",
                    "No se envia orden a Binance. El sistema prefiere no operar antes que abrir una posicion insegura.",
                    "Usar el flujo directo como fuente de copia. Rehabilitar legacy solo con engine.copy.allow-legacy-unknown-delta-jobs=true si sabes que no hay doble flujo.",
                    false,
                    true
            );

            case "binance_symbol_unsupported" -> new Advice(
                    code,
                    "INFO",
                    "No se puede copiar este par porque Binance no lo soporta o no existe el alias.",
                    "El simbolo de Hyperliquid no fue resuelto en el catalogo de futuros de Binance.",
                    "No se envia orden a Binance y el movimiento queda solo como auditoria.",
                    "Agregar alias si el par existe en Binance; si no existe, excluirlo de copy trading.",
                    true,
                    false
            );

            case "flip_starts_new_side" -> new Advice(
                    code,
                    "INFO",
                    "FLIP detectado: se prepara una nueva posicion del lado contrario.",
                    "Hyperliquid cambio de LONG a SHORT o de SHORT a LONG.",
                    "El estado original queda separado en cierre del lado anterior y apertura del lado nuevo.",
                    "Correcto en produccion; verificar solo si no aparece el cierre del lado anterior cuando existia posicion previa.",
                    false,
                    false
            );
            case "flip_closed_previous_side", "flip_close_previous_side" -> new Advice(
                    code,
                    "INFO",
                    "FLIP cerro primero el lado anterior antes de abrir el nuevo.",
                    "Para copiar un FLIP de forma segura se debe cerrar la posicion previa y luego abrir la direccion contraria.",
                    "Evita quedar con posiciones opuestas o con riesgo duplicado en Binance.",
                    "Correcto; revisar solo si luego falla la apertura del nuevo lado para reconciliar la posicion manualmente.",
                    false,
                    false
            );
            case "flip_open_new_side" -> new Advice(
                    code,
                    "INFO",
                    "FLIP abrio la nueva posicion del lado contrario.",
                    "Despues de cerrar la copia anterior, el sistema envio la apertura de la nueva direccion.",
                    "La copia queda alineada con la nueva direccion de la wallet original.",
                    "Correcto; si falla, reconciliar Binance antes de reintentar para evitar duplicados.",
                    false,
                    false
            );
            case "flip_replaces_previous_side" -> new Advice(
                    code,
                    "INFO",
                    "El FLIP reemplaza el lado anterior en el basket de reconciliacion.",
                    "La fuente actual trae una posicion nueva del lado contrario y la pierna opuesta ya no debe seguir como objetivo.",
                    "Fuerza cierre de la copia antigua y apertura de la nueva, en vez de mantener ambas.",
                    "Correcto; revisar si aparecen dos copias activas del mismo simbolo y wallet.",
                    false,
                    false
            );


            case "flip_preflight_blocked", "flip_preflight_no_new_target", "flip_preflight_metric_blocked", "flip_preflight_budget_blocked" -> new Advice(
                    code,
                    "CRITICAL",
                    "FLIP bloqueado por validacion previa antes de abrir el lado nuevo.",
                    "El sistema no pudo construir un objetivo seguro para la nueva direccion del FLIP, por metricas, presupuesto o datos insuficientes.",
                    "Puede quedar sin copiar el cambio de direccion o cerrar solo el lado anterior segun politica de seguridad configurada.",
                    "Revisar metricas, presupuesto, simbolo, targetQty y posicion real en Binance antes de reintentar.",
                    false,
                    true
            );
            case "flip_open_blocked_after_close" -> new Advice(
                    code,
                    "CRITICAL",
                    "FLIP dejo el lado nuevo bloqueado despues de la validacion.",
                    "La politica de seguridad permitio cerrar el lado anterior, pero no habia condiciones para abrir la direccion nueva.",
                    "La cuenta puede quedar plana respecto a la wallet original hasta que se recupere la apertura.",
                    "Reconciliar Binance y reintentar apertura del lado nuevo con el mismo clientOrderId/idempotencia.",
                    false,
                    true
            );
            case "flip_open_retry_allowed" -> new Advice(
                    code,
                    "REVIEW",
                    "Se permite reintentar la apertura pendiente de un FLIP.",
                    "Habia un estado pending/uncertain para el lado nuevo, pero el retry usa el mismo clientOrderId idempotente.",
                    "Ayuda a recuperar un FLIP que cerro el lado anterior y fallo o quedó dudoso al abrir el nuevo.",
                    "Correcto si el retry viene de reconciliacion; revisar solo si se repite muchas veces.",
                    false,
                    false
            );
            case "trigger_is_close" -> new Advice(
                    code,
                    "INFO",
                    "Un cierre disparo reconciliacion y no se abren posiciones nuevas.",
                    "El trigger era de cierre; abrir otras posiciones desde este evento podria copiar historico fuera de orden.",
                    "No se envia orden de apertura/aumento.",
                    "Correcto; revisar solo si esperabas una apertura por evento OPEN/FLIP.",
                    false,
                    false
            );
            case "non_trigger_position" -> new Advice(
                    code,
                    "INFO",
                    "La posicion no era el evento que disparo el rebalance.",
                    "Para evitar copiar historico atrasado, el sistema solo abre/aumenta la posicion del trigger actual.",
                    "No se envia orden para esa pierna del basket.",
                    "Correcto; si se necesita hidratar una wallet nueva, usar flujo explicito de hidratacion inicial.",
                    false,
                    false
            );
            case "source_update_time_missing" -> new Advice(
                    code,
                    "REVIEW",
                    "No se aumento porque la fuente no trae fecha confiable.",
                    "Sin timestamp no se puede saber si el resize es reciente o antiguo.",
                    "Evita aumentar una copia con un estado viejo.",
                    "Validar parser de timestamps de Hyperliquid y revisar el evento por traceId.",
                    false,
                    true
            );
            case "stale_source_update" -> new Advice(
                    code,
                    "INFO",
                    "No se aumento porque el estado fuente estaba fuera de tiempo.",
                    "El resize llego tarde o fue procesado despues del limite aceptado.",
                    "Evita ajustar la copia con datos viejos.",
                    "Correcto; si ocurre en wallets activas con frecuencia, revisar cola origin_store/websocket.",
                    false,
                    false
            );

            case "adjustment_reconcile_required" -> new Advice(
                    code,
                    "INFO",
                    "El ajuste se resuelve por reconciliacion del basket, no como apertura directa.",
                    "RESIZE, UPDATE y FLIP necesitan comparar estado objetivo contra copia actual para evitar duplicados o tamanos viejos.",
                    "La copia se ajusta al tamano correcto usando una sola ruta controlada.",
                    "Correcto; revisar solo si no aparece luego un increase, reduce, close u open esperado.",
                    false,
                    false
            );
            case "rebalance_open" -> new Advice(
                    code,
                    "INFO",
                    "Se abre una copia faltante durante la reconciliacion.",
                    "La wallet original tiene una posicion copiable que aun no existe en Binance para el usuario.",
                    "Se envia una apertura controlada y se registra en copy_operation_event.",
                    "Correcto si la wallet esta asignada; revisar presupuesto y filtros si no aparece la orden.",
                    false,
                    false
            );
            case "rebalance_full_close" -> new Advice(
                    code,
                    "INFO",
                    "Se cierra una copia que ya no existe como objetivo en la wallet original.",
                    "La reconciliacion detecto que Binance mantiene una copia que la fuente ya no debe tener activa.",
                    "Reduce riesgo de quedar con posiciones colgadas.",
                    "Correcto; verificar solo si se reabre inmediatamente por datos fuera de orden.",
                    false,
                    false
            );
            case "rebalance_reduce" -> new Advice(
                    code,
                    "INFO",
                    "Se reduce una copia para igualar el nuevo tamano objetivo.",
                    "La wallet original bajo exposicion y Binance debe reducir con reduceOnly.",
                    "Disminuye posicion sin abrir riesgo contrario.",
                    "Correcto; revisar si Binance rechaza reduceOnly o si el tamano final no coincide.",
                    false,
                    false
            );
            case "rebalance_increase" -> new Advice(
                    code,
                    "INFO",
                    "Se aumenta una copia para igualar el nuevo tamano objetivo.",
                    "La wallet original subio exposicion y el presupuesto permite aumentar.",
                    "La posicion copiada queda mas cerca del sizing objetivo.",
                    "Correcto; revisar si falta margen, minimo notional o filtro de riesgo.",
                    false,
                    false
            );

            case "direct_close" -> new Advice(
                    code,
                    "INFO",
                    "Se cierra una copia activa en Binance.",
                    "La wallet original cerro o la reconciliacion determino que la copia ya no debe seguir abierta.",
                    "La orden se envia con reduceOnly para evitar abrir exposicion contraria.",
                    "Correcto; revisar solo si Binance rechaza el cierre o la copia queda activa despues del evento.",
                    false,
                    false
            );

            case "delta_not_lifecycle_start" -> new Advice(
                    code,
                    "INFO",
                    "El evento no inicia una posicion original y no se puede usar como apertura.",
                    "Llego un delta que no es OPEN/REOPEN y no existe lifecycle activo previo.",
                    "No se inventa una posicion base para evitar estado falso.",
                    "Ignorar si es una wallet no seleccionada; revisar si se esperaba ver el OPEN inicial.",
                    true,
                    !noActiveWallet
            );

            case "allocation_empty", "no_active_allocation" -> new Advice(
                    code,
                    "INFO",
                    "No hay usuarios con esta wallet seleccionada para copiar.",
                    "La wallet no tiene asignacion activa o no pasa filtros para ningun usuario.",
                    "El sistema observa el movimiento, pero no envia orden a Binance.",
                    "Ignorar si estas monitoreando wallets; activar asignacion solo cuando la wallet sea copiable.",
                    true,
                    false
            );
            case "allocation_wallet_missing", "wallet_missing" -> new Advice(
                    code,
                    "REVIEW",
                    "No se pudo buscar usuarios elegibles porque falta la wallet origen.",
                    "El evento llego incompleto o el mapper no seteo idCuenta/wallet.",
                    "No se puede decidir copia por asignacion de wallet.",
                    "Revisar mapper de Hyperliquid y payload original; este caso no deberia repetirse en volumen alto.",
                    false,
                    true
            );
            case "queue_full" -> new Advice(
                    code,
                    "CRITICAL",
                    "La cola esta llena y el servicio no pudo aceptar el evento.",
                    "Entraron mas eventos de los que los workers pudieron procesar.",
                    "Se puede perder trazabilidad o retrasar la copia si sigue ocurriendo.",
                    "Aumentar workers/capacidad, bajar trabajo bloqueante y revisar llamadas lentas externas.",
                    false,
                    true
            );
            case "executor_rejected" -> new Advice(
                    code,
                    "ERROR",
                    "El executor de copia rechazo una tarea.",
                    "La cola o pool de ejecucion de copias no tenia capacidad disponible.",
                    "La copia directa no salio por hot path y puede caer a fallback si esta habilitado.",
                    "Revisar tamaño del pool, queue capacity y latencia de ms-binance-engine/Binance.",
                    false,
                    true
            );
            case "direct_execution_failed" -> new Advice(
                    code,
                    "ERROR",
                    "La ejecucion directa de la copia fallo.",
                    "Binance, ms-binance-engine, persistencia o validacion rechazaron la orden.",
                    "Puede requerir fallback/retry/reconciliacion para evitar posicion desfasada.",
                    "Revisar errClass/errMsg, clientOrderId e idempotencia antes de reintentar manualmente.",
                    false,
                    true
            );
            case "copy_state_pending_or_uncertain" -> new Advice(
                    code,
                    "INFO",
                    "No se abre otra copia porque ya existe un estado pendiente o incierto.",
                    "La proteccion de idempotencia evita duplicar una orden mientras el estado no esta confirmado.",
                    "No se envia una segunda orden para evitar duplicados.",
                    "Revisar solo si queda pendiente mucho tiempo; reconciliar contra Binance y cache activa.",
                    false,
                    false
            );
            case "user_missing" -> new Advice(
                    code,
                    "ERROR",
                    "El candidato de copia no tiene userId valido.",
                    "El cache/listado de usuarios devolvio un usuario incompleto.",
                    "No se puede construir trazabilidad ni enviar orden para ese usuario.",
                    "Corregir carga de usuarios/cache y validar datos antes de publicar candidatos.",
                    false,
                    true
            );
            case "ledger_duplicate_ignored", "movement_already_recorded" -> new Advice(
                    code,
                    "INFO",
                    "El ledger ya tenia este movimiento y lo ignoro para no duplicar.",
                    "La idempotencia detecto un movementKey repetido.",
                    "No afecta el copiado; protege metricas y PnL de duplicados.",
                    "No hacer nada si es bajo; revisar movementKey si aparece en volumen alto.",
                    true,
                    false
            );

            case "origin_upsert_failed" -> new Advice(
                    code,
                    "ERROR",
                    "No se pudo actualizar futures_position para la posicion original.",
                    "La DB, el lock, el precio normalizado o la validacion fallaron durante el upsert del estado original.",
                    "El hot path de copia no deberia bloquearse, pero futures_position puede quedar atrasado.",
                    "Revisar errClass/errMsg, queueDelayMs y si existen locks o llamadas Binance lentas.",
                    false,
                    true
            );
            case "ledger_insert_failed" -> new Advice(
                    code,
                    "ERROR",
                    "No se pudo guardar el movimiento en el ledger.",
                    "La base de datos rechazo o fallo al persistir operation_movement_event.",
                    "La copia no se bloquea, pero se pierde trazabilidad/metrica de ese movimiento.",
                    "Revisar schema, indices, particiones, constraints y conectividad Postgres.",
                    false,
                    true
            );
            case "binance_price_failed" -> new Advice(
                    code,
                    "WARN",
                    "No se pudo obtener precio desde Binance para normalizar el estado original.",
                    "Binance respondio lento/error o hubo problema de red/parser.",
                    "Puede afectar notional/margen de futures_position, pero no deberia bloquear hot path.",
                    "Verificar fallbackSource/priceUsed; si no hay fallback, revisar red o desactivar price fetch en caliente.",
                    true,
                    false
            );

            case "database_error" -> new Advice(
                    code,
                    "ERROR",
                    "La base de datos rechazo o no pudo completar la operacion.",
                    "Hubo error de conexion, constraint, timeout, lock o consulta invalida en Postgres.",
                    "La solicitud no queda confirmada y se debe revisar antes de reintentar si involucra ordenes.",
                    "Revisar errMsg, tabla/constraint, locks y estado de la transaccion antes de repetir la accion.",
                    false,
                    true
            );
            case "request_invalid", "validation_error" -> new Advice(
                    code,
                    "INFO",
                    "La solicitud recibida no cumple el formato esperado.",
                    "Faltan campos obligatorios o algun valor tiene tipo/formato invalido.",
                    "No se procesa el evento para evitar guardar o ejecutar datos incompletos.",
                    "Corregir payload/parametros y validar contrato del endpoint que llamo al servicio.",
                    false,
                    false
            );
            case "business_error" -> new Advice(
                    code,
                    "REVIEW",
                    "Una regla de negocio bloqueo la operacion.",
                    "El request era valido tecnicamente, pero no cumple una condicion segura del flujo de copia.",
                    "No se ejecuta la accion para evitar descuadres o riesgo no permitido.",
                    "Revisar reasonCode especifico y estado de copy_operation/cache antes de forzar cambios.",
                    false,
                    true
            );
            case "binance_rate_limit" -> new Advice(
                    code,
                    "ERROR",
                    "Binance limito temporalmente las solicitudes.",
                    "Se alcanzo un limite de requests o peso de API.",
                    "Las ordenes/reintentos pueden demorarse y deben respetar backoff para no duplicar ni banear la cuenta.",
                    "Activar backoff, revisar volumen por API key y esperar reconciliacion antes de repetir manualmente.",
                    false,
                    true
            );
            case "copy_order_rejected" -> new Advice(
                    code,
                    "ERROR",
                    "Binance rechazo la orden de copia.",
                    "La orden no cumplio una regla del exchange: simbolo, cantidad, notional, modo de posicion, margen o parametro invalido.",
                    "No hay que asumir que la posicion quedo copiada; se requiere reconciliacion.",
                    "Revisar binanceCode/binanceMsg/clientOrderId y consultar estado real en Binance antes de reintentar.",
                    false,
                    true
            );
            case "copy_binance_client_error", "external_service_error" -> new Advice(
                    code,
                    "ERROR",
                    "El servicio externo que ejecuta la copia fallo o no respondio correctamente.",
                    "ms-binance-engine, red o Binance devolvieron error no seguro para asumir exito.",
                    "La copia puede quedar pendiente o incierta y debe reconciliarse contra Binance.",
                    "Buscar clientOrderId en Binance, revisar retry/fallback y no reenviar a ciegas.",
                    false,
                    true
            );
            case "internal_error" -> new Advice(
                    code,
                    "CRITICAL",
                    "El servicio tuvo un error interno no recuperado.",
                    "Una excepcion no controlada llego al manejador global.",
                    "Puede dejar una accion sin confirmar; si involucra copia, requiere reconciliacion antes de reintentar.",
                    "Revisar stacktrace, traceId y estado real de Binance/DB; agregar reasonCode especifico si se repite.",
                    false,
                    true
            );

            case "unknown" -> new Advice(
                    code,
                    "REVIEW",
                    "El servicio no entrego un motivo especifico para este caso.",
                    "Falta propagar reasonCode o agregarlo al catalogo de diagnosticos.",
                    "El evento puede ser correcto, pero el dashboard no puede explicar la causa.",
                    "Agregar reasonCode en el punto de decision y mapearlo en CopyLogAdvice/Grafana.",
                    false,
                    true
            );
            default -> new Advice(
                    code,
                    "REVIEW",
                    "Caso revisable no clasificado por el catalogo de logs.",
                    "El reasonCode existe, pero aun no tiene diagnostico humano especifico.",
                    "No necesariamente rompe copia, pero dificulta soporte y monitoreo.",
                    "Agregar este reasonCode a CopyLogAdvice con causa, impacto y accion.",
                    false,
                    true
            );
        };
    }

    private static Advice adjustmentWithoutCopy(String code, boolean noActiveWallet, Context ctx) {
        boolean cacheActive = Boolean.TRUE.equals(ctx.cacheActive());
        return new Advice(
                code,
                noActiveWallet ? "INFO" : "REVIEW",
                "Signals recibio un ajuste de la wallet original, pero no hay una posicion copiada activa en Binance para modificar.",
                noActiveWallet
                        ? "No hay usuarios elegibles o la wallet no esta asignada para copiar; por eso nunca se abrio una copia de usuario."
                        : cacheActive
                        ? "Existe cache de copias, pero no para este originId; puede haber desfase de lifecycle de copia."
                        : "El OPEN inicial no se copio, el bot empezo con la posicion ya abierta o se perdio estado de copia.",
                "Solo se evita enviar un ajuste a Binance; esto no significa que haya fallado guardar el movimiento original de Hyperliquid.",
                noActiveWallet
                        ? "Ignorar si esta wallet no debia copiarse; revisar asignaciones solo si esperabas copiar esa wallet."
                        : "Reconciliar copy_operation/cache, validar que el OPEN fue copiado y revisar idempotencia/clientOrderId.",
                noActiveWallet,
                !noActiveWallet
        );
    }

    private static Advice flipWithoutCopy(String code, boolean noActiveWallet) {
        return new Advice(
                code,
                noActiveWallet ? "INFO" : "REVIEW",
                "Signals recibio un FLIP de la wallet original, pero no hay una posicion copiada activa para cerrar antes de darla vuelta.",
                noActiveWallet
                        ? "La wallet no esta asignada o no hay usuarios elegibles; por eso nunca hubo copia abierta para ese usuario."
                        : "El sistema no encontro la copia previa; pudo faltar OPEN, haber cache desfasada o cierre anterior inconsistente.",
                "No se envia FLIP a Binance para evitar abrir la direccion contraria sin cerrar primero una copia real.",
                noActiveWallet
                        ? "Ignorar si no estas copiando esa wallet; revisar asignacion si esperabas copiarla."
                        : "Reconciliar posicion activa en Binance vs copy_operation antes de permitir FLIP automatico.",
                noActiveWallet,
                !noActiveWallet
        );
    }


    private static Advice flipWithoutCopySameBaseAsset(String code, boolean noActiveWallet, Context ctx) {
        return new Advice(
                code,
                noActiveWallet ? "INFO" : "REVIEW",
                "Signals recibio un FLIP y busco una copia activa del mismo activo base, pero no encontro ninguna para dar vuelta.",
                noActiveWallet
                        ? "No hay usuarios elegibles o no existia una copia abierta del lado anterior para esa wallet/activo."
                        : "Habia contexto de copia, pero no calzo por activo base/lado; puede requerir reconciliacion.",
                "No se abre la direccion contraria en Binance para evitar riesgo duplicado o una posicion sin cierre previo.",
                noActiveWallet
                        ? "Ignorar si esa wallet no debia copiarse; revisar asignacion si esperabas copiarla."
                        : "Revisar copy_operation activa por wallet/baseAsset y posicion real en Binance antes de permitir FLIP automatico.",
                noActiveWallet,
                !noActiveWallet
        );
    }

    private static Advice closeWithoutCopy(String code, boolean noActiveWallet, Context ctx) {
        boolean hadSubmitted = number(ctx.submittedTasks()) > 0;
        return new Advice(
                code,
                noActiveWallet && !hadSubmitted ? "INFO" : "REVIEW",
                "Signals recibio un cierre de la wallet original, pero no hay una posicion copiada activa de usuario que cerrar en Binance.",
                noActiveWallet
                        ? "La wallet no esta asignada o el usuario no era elegible, entonces no se abrio copia previamente."
                        : "El OPEN no se copio, la copia ya se cerro, o el estado runtime/DB no calza.",
                "No se envia cierre a Binance porque podria cerrar una posicion equivocada o inexistente.",
                noActiveWallet
                        ? "Ignorar si activeCopyUsers=0; revisar asignacion solo si esperabas copiar esa wallet."
                        : "Revisar copy_operation activa, cache y posicion real en Binance; ejecutar reconciliacion si habia copia real.",
                noActiveWallet,
                !noActiveWallet || hadSubmitted
        );
    }

    private static Advice lateAdjustmentWithoutOrigin(String code, boolean noActiveWallet, boolean afterQueue, boolean queueBacklog) {
        String human = afterQueue
                ? "Origin Store puso el ajuste en cola, pero al guardar seguia sin encontrar la posicion original de Hyperliquid."
                : "Origin Store recibio un ajuste, pero no tiene registrada la posicion original abierta de Hyperliquid para esa wallet/simbolo/lado.";
        String cause = noActiveWallet
                ? "Normalmente pasa cuando el servicio empieza a mirar una wallet que ya tenia posiciones abiertas, o cuando esa wallet no estaba asignada para copiar al momento del OPEN."
                : queueBacklog
                ? "Puede ser cola origin_store atrasada, evento fuera de orden o OPEN original no registrado."
                : "Puede ser OPEN original no visto, evento fuera de orden, cache fria o posicion ya cerrada.";
        return new Advice(
                code,
                noActiveWallet ? "INFO" : (queueBacklog ? "REVIEW" : "INFO"),
                human,
                cause,
                "No se crea/actualiza la posicion original para no inventar una base falsa de PnL o lifecycle; esto es distinto a la copia de usuario en Binance.",
                noActiveWallet
                        ? "Si esa wallet se acaba de agregar, hidratar/sincronizar sus posiciones abiertas antes de esperar ajustes limpios."
                        : "Revisar queueDelayMs/queueDepth, hidratacion de posiciones abiertas y orden de eventos del websocket.",
                noActiveWallet,
                !noActiveWallet && queueBacklog
        );
    }

    private static Advice closeWithoutActiveOrigin(String code, boolean noActiveWallet) {
        return new Advice(
                code,
                noActiveWallet ? "INFO" : "REVIEW",
                "Origin Store recibio un cierre, pero no tiene una posicion original abierta registrada para cerrar.",
                noActiveWallet
                        ? "La wallet no estaba asignada o el sistema empezo despues de la apertura original."
                        : "El OPEN original no fue visto, la cache se perdio o la DB no tenia la posicion activa.",
                "El historial original puede quedar incompleto; esto es distinto a cerrar una copia de usuario en Binance.",
                noActiveWallet
                        ? "Ignorar si no copias esa wallet; si acabas de activarla, hidratar posiciones abiertas."
                        : "Revisar futures_position, hidratacion al inicio y reconciliacion de posiciones abiertas.",
                noActiveWallet,
                !noActiveWallet
        );
    }


    private static String diagnosticArea(String diagnosticCode) {
        String code = normalize(diagnosticCode);
        if (code.contains("idempot") || code.contains("dedupe") || code.contains("distributed_duplicate")) {
            return "idempotency";
        }
        if (code.contains("guard_blocked")) {
            return "risk_guard";
        }
        if (code.contains("summary_not_final")
                || code.contains("required_window")
                || code.contains("data_stale_blocks")
                || code.contains("simulation_audit")) {
            return "promotion_readiness";
        }
        if (code.contains("scope_not_matched") || code.contains("symbol_not_allowed")) {
            return "strategy_scope";
        }
        if (code.contains("min_notional")) {
            return "sizing";
        }
        if (code.contains("active_origin") || code.contains("origin") || code.startsWith("late_adjustment")) {
            return "origin_store_original_position";
        }
        if (code.contains("open_copy") || code.contains("without_copy") || code.startsWith("copy_") || code.startsWith("delta_type_")) {
            return "user_copy_binance_position";
        }
        if (code.contains("binance") || code.contains("symbol")) {
            return "binance_execution_or_symbol";
        }
        return "general";
    }

    private static boolean noActiveWallet(Context ctx) {
        int active = number(ctx.activeCopyUsers());
        int eligible = number(ctx.eligibleUsers());
        int submitted = number(ctx.submittedTasks());
        return active <= 0 && eligible <= 0 && submitted <= 0;
    }

    private static int number(Integer value) {
        return value == null ? 0 : value;
    }

    private static String normalize(String reasonCode) {
        if (reasonCode == null || reasonCode.isBlank()) {
            return "unknown";
        }
        return reasonCode.trim().toLowerCase(Locale.ROOT);
    }

    private static String safeToken(String value) {
        if (value == null || value.isBlank()) {
            return "NA";
        }
        return value.trim()
                .replace(' ', '_')
                .replace('=', '_')
                .replace('"', '_')
                .replace('\n', '_')
                .replace('\r', '_')
                .replace('\t', '_');
    }

    private static String quote(String value) {
        if (value == null) {
            return "\"NA\"";
        }
        String clean = value
                .replace("\\", "/")
                .replace("\n", " ")
                .replace("\r", " ")
                .replace("\t", " ")
                .replace("\"", "'");
        if (clean.length() > 1000) {
            clean = clean.substring(0, 1000);
        }
        return "\"" + clean + "\"";
    }
}
