package com.apunto.engine.shared.util;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

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

    private static String hash32Hex(String originId, String userId, String walletId) {
        String input = String.valueOf(originId) + "|" + String.valueOf(userId) + "|" + String.valueOf(walletId);
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(input.getBytes(StandardCharsets.UTF_8));

            // Use first 16 bytes (128 bits) => 32 hex chars.
            StringBuilder sb = new StringBuilder(HASH_HEX_LEN);
            for (int i = 0; i < 16; i++) {
                sb.append(String.format("%02x", digest[i]));
            }
            return sb.toString();
        } catch (Exception e) {
            // Extremely unlikely; fallback to a deterministic-ish safe string.
            int h = input.hashCode();
            return String.format("%032x", h);
        }
    }
}
