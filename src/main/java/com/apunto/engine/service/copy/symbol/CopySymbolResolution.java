package com.apunto.engine.service.copy.symbol;

public record CopySymbolResolution(
        boolean resolved,
        String sourceSymbol,
        String targetSymbol,
        String baseAsset,
        String quoteAsset,
        String capitalAsset,
        String reasonCode,
        boolean cacheStale
) {
    public static CopySymbolResolution resolved(
            String sourceSymbol,
            String targetSymbol,
            String baseAsset,
            String quoteAsset,
            String capitalAsset,
            boolean cacheStale
    ) {
        return new CopySymbolResolution(
                true,
                sourceSymbol,
                targetSymbol,
                baseAsset,
                quoteAsset,
                capitalAsset,
                "SYMBOL_RESOLVED",
                cacheStale
        );
    }

    public static CopySymbolResolution skipped(
            String sourceSymbol,
            String targetSymbol,
            String baseAsset,
            String quoteAsset,
            String capitalAsset,
            String reasonCode
    ) {
        return new CopySymbolResolution(
                false,
                sourceSymbol,
                targetSymbol,
                baseAsset,
                quoteAsset,
                capitalAsset,
                reasonCode,
                false
        );
    }
}
