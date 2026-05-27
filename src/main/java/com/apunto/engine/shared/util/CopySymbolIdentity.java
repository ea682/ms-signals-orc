package com.apunto.engine.shared.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Identidad de símbolo para copy trading.
 *
 * <p>Hyperliquid y Binance no siempre hablan con el mismo nombre:
 * Hyperliquid puede enviar BTCUSD y Binance opera BTCUSDT o BTCUSDC según la moneda del usuario.
 * Para lifecycle de copy trading, sobre todo FLIP, necesitamos saber si es el mismo activo base
 * sin confundir la moneda de cotización.</p>
 */
public final class CopySymbolIdentity {

    private static final List<String> KNOWN_QUOTES = List.of(
            "USDT", "USDC", "FDUSD", "BUSD", "USDE", "TUSD", "USDP", "DAI", "USD", "BTC", "ETH"
    );
    private static final List<String> LEADING_MULTIPLIERS = List.of(
            "1000000000", "1000000", "1000"
    );
    private static final Pattern VERSION_SUFFIX = Pattern.compile("V\\d+$");

    private CopySymbolIdentity() {
    }

    public static boolean sameWallet(String leftWallet, String rightWallet) {
        String left = normalizeId(leftWallet);
        String right = normalizeId(rightWallet);
        return left != null && left.equals(right);
    }

    public static boolean sameBaseAsset(String leftSymbol, String rightSymbol) {
        List<String> leftBases = baseAssetCandidates(leftSymbol);
        List<String> rightBases = baseAssetCandidates(rightSymbol);
        if (leftBases.isEmpty() || rightBases.isEmpty()) {
            return false;
        }
        return leftBases.stream().anyMatch(rightBases::contains);
    }

    public static boolean sameWalletAndBaseAsset(String leftWallet,
                                                  String rightWallet,
                                                  String leftSymbol,
                                                  String rightSymbol) {
        return sameWallet(leftWallet, rightWallet) && sameBaseAsset(leftSymbol, rightSymbol);
    }

    public static String primaryBaseAsset(String symbol) {
        List<String> candidates = baseAssetCandidates(symbol);
        return candidates.isEmpty() ? null : candidates.get(0);
    }

    public static List<String> baseAssetCandidates(String symbol) {
        String normalized = normalizeSymbol(symbol);
        if (normalized == null) {
            return List.of();
        }

        String withoutQuote = stripKnownQuote(normalized);
        if (withoutQuote == null || withoutQuote.isBlank()) {
            return List.of();
        }

        String withoutVersion = stripVersionSuffix(withoutQuote);
        List<String> candidates = new ArrayList<>();
        addDistinct(candidates, withoutVersion);
        addDistinct(candidates, withoutQuote);

        for (String multiplier : LEADING_MULTIPLIERS) {
            for (String candidate : List.of(withoutVersion, withoutQuote)) {
                if (candidate != null && candidate.startsWith(multiplier) && candidate.length() > multiplier.length()) {
                    addDistinct(candidates, candidate.substring(multiplier.length()));
                }
            }
        }

        return List.copyOf(candidates);
    }

    public static String normalizeSymbol(String raw) {
        if (raw == null) {
            return null;
        }
        String normalized = raw.trim()
                .toUpperCase(Locale.ROOT)
                .replace("-", "")
                .replace("_", "")
                .replace("/", "")
                .replace(".", "")
                .replace(" ", "");
        return normalized.isBlank() ? null : normalized;
    }

    public static String normalizeId(String raw) {
        if (raw == null) {
            return null;
        }
        String normalized = raw.trim().toUpperCase(Locale.ROOT);
        return normalized.isBlank() ? null : normalized;
    }

    private static String stripKnownQuote(String symbol) {
        if (symbol == null || symbol.isBlank()) {
            return null;
        }
        for (String quote : KNOWN_QUOTES) {
            if (symbol.endsWith(quote) && symbol.length() > quote.length()) {
                return symbol.substring(0, symbol.length() - quote.length());
            }
        }
        return symbol;
    }

    private static String stripVersionSuffix(String value) {
        if (value == null || value.isBlank()) {
            return value;
        }
        return VERSION_SUFFIX.matcher(value).replaceFirst("");
    }

    private static void addDistinct(List<String> values, String value) {
        if (value != null && !value.isBlank() && values.stream().noneMatch(existing -> Objects.equals(existing, value))) {
            values.add(value);
        }
    }
}
