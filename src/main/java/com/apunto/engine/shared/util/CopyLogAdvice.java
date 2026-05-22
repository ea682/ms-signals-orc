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
            case "resize_without_open_copy", "update_without_open_copy", "adjustment_without_open_copy" -> adjustmentWithoutCopy(code, noActiveWallet, ctx);
            case "flip_without_open_copy" -> flipWithoutCopy(code, noActiveWallet, ctx);
            case "close_without_open_copy" -> closeWithoutCopy(code, noActiveWallet, ctx);
            case "late_adjustment_without_active_origin" -> lateAdjustmentWithoutOrigin(code, noActiveWallet, false, queueBacklog);
            case "late_adjustment_without_active_origin_after_queue" -> lateAdjustmentWithoutOrigin(code, noActiveWallet, true, queueBacklog);
            case "close_without_active_origin" -> closeWithoutActiveOrigin(code, noActiveWallet);
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
                "La wallet original ajusto una posicion, pero no existe una copia activa para modificar.",
                noActiveWallet
                        ? "No hay usuario/wallet elegible para copiar este movimiento o la wallet no esta seleccionada."
                        : cacheActive
                        ? "Hay cache activa, pero no para este originId; puede ser lifecycle desfasado."
                        : "El OPEN inicial no se copio, el bot inicio con la posicion ya abierta o se perdio estado.",
                "No se envia ajuste a Binance para evitar abrir/aumentar algo que no existe como copia.",
                noActiveWallet
                        ? "Ignorar mientras activeCopyUsers/eligibleUsers sea 0; revisar solo si esta wallet debia copiarse."
                        : "Reconciliar copy_operation/cache, validar que el OPEN fue copiado y revisar idempotencia/clientOrderId.",
                noActiveWallet,
                !noActiveWallet
        );
    }

    private static Advice flipWithoutCopy(String code, boolean noActiveWallet, Context ctx) {
        return new Advice(
                code,
                noActiveWallet ? "INFO" : "REVIEW",
                "La wallet original hizo FLIP, pero no existe una copia activa para cerrar y dar vuelta.",
                noActiveWallet
                        ? "La wallet no esta seleccionada o no hay usuario elegible, por eso nunca hubo copia abierta."
                        : "El sistema no encontro la copia previa; pudo faltar OPEN, haber cache desfasada o cierre anterior inconsistente.",
                "No se envia FLIP a Binance para evitar abrir direccion contraria sin cerrar una copia real.",
                noActiveWallet
                        ? "Ignorar si no estas copiando esa wallet."
                        : "Reconciliar posicion activa en Binance vs copy_operation antes de permitir FLIP automatico.",
                noActiveWallet,
                !noActiveWallet
        );
    }

    private static Advice closeWithoutCopy(String code, boolean noActiveWallet, Context ctx) {
        boolean hadSubmitted = number(ctx.submittedTasks()) > 0;
        return new Advice(
                code,
                noActiveWallet && !hadSubmitted ? "INFO" : "REVIEW",
                "Llego un cierre, pero no hay una copia activa que cerrar.",
                noActiveWallet
                        ? "La wallet no esta seleccionada o el usuario no era elegible, entonces no se abrio copia previamente."
                        : "El OPEN no se copio, la copia ya se cerro, o el estado runtime/DB no calza.",
                "No se envia cierre a Binance porque podria cerrar una posicion equivocada o inexistente.",
                noActiveWallet
                        ? "Ignorar si activeCopyUsers=0; revisar solo si esperabas copiar esa wallet."
                        : "Revisar copy_operation activa, cache y posicion real en Binance; ejecutar reconciliacion si habia copia real.",
                noActiveWallet,
                !noActiveWallet || hadSubmitted
        );
    }

    private static Advice lateAdjustmentWithoutOrigin(String code, boolean noActiveWallet, boolean afterQueue, boolean queueBacklog) {
        String human = afterQueue
                ? "El ajuste entro a cola, pero al guardar seguia sin posicion original activa."
                : "Llego un ajuste, pero el servicio no tiene una posicion original activa para esa wallet/simbolo/lado.";
        String cause = noActiveWallet
                ? "Normalmente ocurre porque la wallet no esta seleccionada o el servicio empezo a observar una posicion ya abierta."
                : queueBacklog
                ? "Puede ser cola origin_store atrasada, evento fuera de orden o OPEN inicial no registrado."
                : "Puede ser OPEN inicial no visto, evento fuera de orden, cache fria o posicion ya cerrada.";
        return new Advice(
                code,
                noActiveWallet ? "INFO" : (queueBacklog ? "REVIEW" : "INFO"),
                human,
                cause,
                "No se crea/actualiza posicion original para no inventar una base falsa de PnL o lifecycle.",
                noActiveWallet
                        ? "Ignorar si no copias esa wallet; al activar una wallet, hidratar posiciones abiertas antes de copiar."
                        : "Revisar queueDelayMs/queueDepth, hidratacion de posiciones abiertas y orden de eventos del websocket.",
                noActiveWallet,
                !noActiveWallet && queueBacklog
        );
    }

    private static Advice closeWithoutActiveOrigin(String code, boolean noActiveWallet) {
        return new Advice(
                code,
                noActiveWallet ? "INFO" : "REVIEW",
                "Llego un cierre, pero no existe posicion original activa registrada.",
                noActiveWallet
                        ? "La wallet no esta seleccionada o el sistema empezo despues de la apertura original."
                        : "El OPEN original no fue visto, la cache se perdio o la DB no tenia la posicion activa.",
                "Se usa fallback de lifecycle para no romper el flujo, pero el historial puede quedar incompleto.",
                noActiveWallet
                        ? "Ignorar si no copias esa wallet."
                        : "Revisar futures_position, hidratacion al inicio y reconciliacion de posiciones abiertas.",
                noActiveWallet,
                !noActiveWallet
        );
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
