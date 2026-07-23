package com.apunto.engine.hyperliquid.identity;

import java.util.Locale;

public final class HyperliquidSourceTradeIdentity {

    private static final String ZERO_HASH = "0x" + "0".repeat(64);

    private HyperliquidSourceTradeIdentity() {
    }

    public static Evidence fromExternalId(
            String externalId,
            String wallet,
            String symbol
    ) {
        if (externalId == null || externalId.isBlank()) {
            return Evidence.missing();
        }
        String sourceIdentity = externalId.trim();
        String[] wrapper = sourceIdentity.split("\\|", 4);
        if (wrapper.length == 4 && isDeltaType(wrapper[2])) {
            if (!equalsIgnoreCase(wrapper[0], wallet)
                    || !normalizedSymbol(wrapper[1]).equals(normalizedSymbol(symbol))) {
                return Evidence.missing();
            }
            sourceIdentity = wrapper[3].trim();
        }
        String[] parts = sourceIdentity.split("\\|");
        if (parts.length < 3) {
            return Evidence.missing();
        }
        Long tid = positiveLong(parts[parts.length >= 4 ? parts.length - 2 : parts.length - 1]);
        String rawHash = parts.length >= 4 ? parts[parts.length - 1] : null;
        return new Evidence(tid, normalizeHash(rawHash), isZeroHash(rawHash));
    }

    public static String normalizeHash(String rawHash) {
        if (rawHash == null || rawHash.isBlank() || isZeroHash(rawHash)) {
            return null;
        }
        return rawHash.trim().toLowerCase(Locale.ROOT);
    }

    public static boolean isZeroHash(String rawHash) {
        return rawHash != null
                && ZERO_HASH.equalsIgnoreCase(rawHash.trim());
    }

    public static String canonicalTradeKey(String wallet, long tid) {
        if (tid <= 0L) {
            throw new IllegalArgumentException("Hyperliquid tid must be positive");
        }
        return "hyperliquid:trade:" + normalizedWallet(wallet) + ':' + tid;
    }

    public static String fallbackTradeKey(
            String wallet,
            String symbol,
            String sourceIdentity
    ) {
        return "hyperliquid:trade:" + normalizedWallet(wallet)
                + ':' + normalizedSymbol(symbol)
                + ':' + required(sourceIdentity, "sourceIdentity");
    }

    public static String recoveryKey(
            String wallet,
            String symbol,
            String sourceIdentity
    ) {
        return "hyperliquid:recovery:" + normalizedWallet(wallet)
                + ':' + normalizedSymbol(symbol)
                + ':' + required(sourceIdentity, "sourceIdentity");
    }

    private static Long positiveLong(String value) {
        try {
            long parsed = Long.parseLong(value);
            return parsed > 0L ? parsed : null;
        } catch (RuntimeException ignored) {
            return null;
        }
    }

    private static boolean equalsIgnoreCase(String left, String right) {
        return left != null && right != null && left.trim().equalsIgnoreCase(right.trim());
    }

    private static String normalizedWallet(String value) {
        return required(value, "wallet").toLowerCase(Locale.ROOT);
    }

    private static String required(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " is required");
        }
        return value.trim();
    }

    private static String normalizedSymbol(String value) {
        if (value == null) {
            return "";
        }
        return value.trim().toUpperCase(Locale.ROOT)
                .replace("-", "")
                .replace("_", "")
                .replace("/", "")
                .replace(".", "")
                .replace(" ", "");
    }

    private static boolean isDeltaType(String value) {
        if (value == null || value.isBlank()) {
            return false;
        }
        return switch (value.trim().toUpperCase(Locale.ROOT)) {
            case "OPEN", "CLOSE", "RESIZE", "FLIP", "UPDATE", "NO_CHANGE" -> true;
            default -> false;
        };
    }

    public record Evidence(Long tid, String hash, boolean zeroHash) {
        private static Evidence missing() {
            return new Evidence(null, null, false);
        }
    }
}
