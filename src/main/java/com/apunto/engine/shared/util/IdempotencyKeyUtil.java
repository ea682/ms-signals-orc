package com.apunto.engine.shared.util;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Generates deterministic idempotency keys that comply with Binance Futures constraints
 * for newClientOrderId (max 36 chars, allowed [A-Za-z0-9._-]).
 *
 * Format:
 *  - Open:  cpO_ + 32 hex chars  => 36 chars total
 *  - Close: cpC_ + 32 hex chars  => 36 chars total
 */
public final class IdempotencyKeyUtil {

    private static final int HASH_HEX_LEN = 32; // 16 bytes => 32 hex chars

    private IdempotencyKeyUtil() {
    }

    public static String openClientOrderId(String originId, String userId, String walletId) {
        return "cpO_" + hash32Hex(originId, userId, walletId);
    }

    public static String closeClientOrderId(String originId, String userId, String walletId) {
        return "cpC_" + hash32Hex(originId, userId, walletId);
    }

    public static String rebalanceIncreaseClientOrderId(String triggerOriginId, String targetOriginId, String userId, String walletId, String targetQty) {
        return "cpI_" + hash32Hex(triggerOriginId, targetOriginId, userId, walletId, targetQty, "inc");
    }

    public static String rebalanceReduceClientOrderId(String triggerOriginId, String targetOriginId, String userId, String walletId, String targetQty) {
        return "cpR_" + hash32Hex(triggerOriginId, targetOriginId, userId, walletId, targetQty, "red");
    }

    public static String rebalanceReopenClientOrderId(String triggerOriginId, String targetOriginId, String userId, String walletId, String targetQty) {
        return "cpN_" + hash32Hex(triggerOriginId, targetOriginId, userId, walletId, targetQty, "reopen");
    }

    private static String hash32Hex(String originId, String userId, String walletId) {
        String input = String.valueOf(originId) + "|" + String.valueOf(userId) + "|" + String.valueOf(walletId);
        return hash32HexRaw(input);
    }

    private static String hash32Hex(String a, String b, String c, String d, String e, String f) {
        String input = String.valueOf(a) + "|" + String.valueOf(b) + "|" + String.valueOf(c) + "|" + String.valueOf(d) + "|" + String.valueOf(e) + "|" + String.valueOf(f);
        return hash32HexRaw(input);
    }

    private static String hash32HexRaw(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(input.getBytes(StandardCharsets.UTF_8));

            char[] out = new char[HASH_HEX_LEN];
            for (int i = 0; i < 16; i++) {
                int b = digest[i] & 0xff;
                out[i * 2] = Character.forDigit((b >>> 4) & 0x0f, 16);
                out[i * 2 + 1] = Character.forDigit(b & 0x0f, 16);
            }
            return new String(out);
        } catch (NoSuchAlgorithmException ex) {
            int h = input.hashCode();
            return String.format("%032x", h);
        }
    }
}
