package com.apunto.engine.shared.util;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Trace id determinístico para seguir una copia a través de Loki.
 *
 * <p>Se calcula con originId + userId + wallet + symbol para que todos los logs del mismo flujo
 * tengan el mismo identificador aunque pasen por hilos distintos.</p>
 */
public final class CopyTraceIdUtil {

    private static final int HASH_HEX_LEN = 16;

    private CopyTraceIdUtil() {
    }

    public static String copyTraceId(String originId, String userId, String walletId, String symbol) {
        String input = String.valueOf(originId)
                + "|" + String.valueOf(userId)
                + "|" + String.valueOf(walletId)
                + "|" + String.valueOf(symbol);
        return "trc_" + hashHex(input);
    }

    private static String hashHex(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(input.getBytes(StandardCharsets.UTF_8));
            char[] out = new char[HASH_HEX_LEN];
            for (int i = 0; i < HASH_HEX_LEN / 2; i++) {
                int b = digest[i] & 0xff;
                out[i * 2] = Character.forDigit((b >>> 4) & 0x0f, 16);
                out[i * 2 + 1] = Character.forDigit(b & 0x0f, 16);
            }
            return new String(out);
        } catch (NoSuchAlgorithmException ex) {
            return String.format("%016x", input.hashCode());
        }
    }
}
