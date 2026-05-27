package com.apunto.engine.shared.enums;

import com.apunto.engine.shared.exception.SkipExecutionException;

import java.util.Locale;

public enum FuturesCapitalAsset {
    USDT,
    USDC;

    public static FuturesCapitalAsset defaultAsset() {
        return USDT;
    }

    public static FuturesCapitalAsset fromNullable(String raw) {
        if (raw == null || raw.isBlank()) {
            return defaultAsset();
        }
        String value = raw.trim().toUpperCase(Locale.ROOT);
        for (FuturesCapitalAsset asset : values()) {
            if (asset.name().equals(value)) {
                return asset;
            }
        }
        throw new SkipExecutionException(
                "capital_asset_invalid",
                "La moneda de capital solo puede ser USDT o USDC",
                com.apunto.engine.shared.util.LogFmt.kv("capitalAsset", raw)
        );
    }

    public static boolean isAllowedStable(String raw) {
        if (raw == null || raw.isBlank()) {
            return false;
        }
        String value = raw.trim().toUpperCase(Locale.ROOT);
        return USDT.name().equals(value) || USDC.name().equals(value);
    }
}
