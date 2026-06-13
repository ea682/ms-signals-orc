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
        return openClientOrderId(originId, userId, walletId, null);
    }

    public static String openClientOrderId(String originId, String userId, String walletId, String strategyCode) {
        return "cpO_" + hash32Hex(originId, userId, walletId, normalizeStrategy(strategyCode));
    }

    public static String closeClientOrderId(String originId, String userId, String walletId) {
        return closeClientOrderId(originId, userId, walletId, null);
    }

    public static String closeClientOrderId(String originId, String userId, String walletId, String strategyCode) {
        return "cpC_" + hash32Hex(originId, userId, walletId, normalizeStrategy(strategyCode));
    }

    public static String rebalanceIncreaseClientOrderId(String triggerOriginId, String targetOriginId, String userId, String walletId, String targetQty) {
        return rebalanceIncreaseClientOrderId(triggerOriginId, targetOriginId, userId, walletId, targetQty, null);
    }

    public static String rebalanceIncreaseClientOrderId(String triggerOriginId, String targetOriginId, String userId, String walletId, String targetQty, String strategyCode) {
        return "cpI_" + hash32Hex(triggerOriginId, targetOriginId, userId, walletId, targetQty, "inc", normalizeStrategy(strategyCode));
    }

    public static String rebalanceReduceClientOrderId(String triggerOriginId, String targetOriginId, String userId, String walletId, String targetQty) {
        return rebalanceReduceClientOrderId(triggerOriginId, targetOriginId, userId, walletId, targetQty, null);
    }

    public static String rebalanceReduceClientOrderId(String triggerOriginId, String targetOriginId, String userId, String walletId, String targetQty, String strategyCode) {
        return "cpR_" + hash32Hex(triggerOriginId, targetOriginId, userId, walletId, targetQty, "red", normalizeStrategy(strategyCode));
    }

    public static String rebalanceReopenClientOrderId(String triggerOriginId, String targetOriginId, String userId, String walletId, String targetQty) {
        return rebalanceReopenClientOrderId(triggerOriginId, targetOriginId, userId, walletId, targetQty, null);
    }

    public static String rebalanceReopenClientOrderId(String triggerOriginId, String targetOriginId, String userId, String walletId, String targetQty, String strategyCode) {
        return "cpN_" + hash32Hex(triggerOriginId, targetOriginId, userId, walletId, targetQty, "reopen", normalizeStrategy(strategyCode));
    }

    private static String hash32Hex(String... parts) {
        StringBuilder input = new StringBuilder();
        if (parts != null) {
            for (String part : parts) {
                if (!input.isEmpty()) {
                    input.append('|');
                }
                input.append(String.valueOf(part));
            }
        }
        return hash32HexRaw(input.toString());
    }

    private static String normalizeStrategy(String strategyCode) {
        if (strategyCode == null || strategyCode.isBlank()) {
            return "MOVEMENT_ALL";
        }
        return strategyCode.trim().replace('-', '_').toUpperCase(java.util.Locale.ROOT);
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
