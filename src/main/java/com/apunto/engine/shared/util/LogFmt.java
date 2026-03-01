package com.apunto.engine.shared.util;

import java.math.BigDecimal;

/**
 * Utilidades simples para construir strings estilo logfmt y sanitizar valores.
 *
 * <p>Nota: la mayoría de nuestros logs usan el patrón key=value. Loki/Grafana puede
 * parsear con "| logfmt".
 */
public final class LogFmt {

    private LogFmt() {}

    /**
     * Construye una secuencia "k=v k2=v2 ..." a partir de pares (key, value).
     *
     * <p>Diseñado para usarse dentro de reason="..." o last_error_message.
     */
    public static String kv(Object... pairs) {
        if (pairs == null || pairs.length == 0) return "";
        StringBuilder sb = new StringBuilder(128);

        int n = pairs.length - (pairs.length % 2); // si viene impar, ignoramos el último
        for (int i = 0; i < n; i += 2) {
            String k = String.valueOf(pairs[i]);
            Object v = pairs[i + 1];

            if (k == null) continue;
            k = sanitizeKey(k);
            if (k.isBlank()) continue;

            if (sb.length() > 0) sb.append(' ');
            sb.append(k).append('=');
            sb.append(sanitizeValue(stringify(v)));
        }

        return sb.toString();
    }

    /**
     * Sanitiza un string para log: sin saltos de línea ni tabs ni comillas dobles.
     */
    public static String sanitize(String s) {
        if (s == null) return null;
        String out = s
                .replace("\n", " ")
                .replace("\r", " ")
                .replace("\t", " ")
                .replace('"', '\'');
        return truncate(out, 4000);
    }

    private static String stringify(Object v) {
        if (v == null) return "null";
        if (v instanceof BigDecimal bd) return bd.toPlainString();
        if (v instanceof Double d) {
            if (!Double.isFinite(d)) return "null";
            return Double.toString(d);
        }
        if (v instanceof Float f) {
            if (!Float.isFinite(f)) return "null";
            return Float.toString(f);
        }
        return String.valueOf(v);
    }

    private static String sanitizeKey(String k) {
        if (k == null) return "";
        // keys sin espacios ni '='
        return k.trim()
                .replace(' ', '_')
                .replace('\t', '_')
                .replace('\n', '_')
                .replace('\r', '_')
                .replace('=', '_');
    }

    private static String sanitizeValue(String v) {
        if (v == null) return "null";
        // Value va dentro de reason="..." o last_error_message, así que permitimos espacios,
        // pero evitamos romper el logfmt simple.
        String out = v
                .replace("\n", " ")
                .replace("\r", " ")
                .replace("\t", " ")
                .replace('"', '\'');
        return truncate(out, 4000);
    }

    private static String truncate(String s, int max) {
        if (s == null) return null;
        if (max <= 0) return "";
        return s.length() <= max ? s : s.substring(0, max);
    }
}
