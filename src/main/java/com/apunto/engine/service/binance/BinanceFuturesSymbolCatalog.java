package com.apunto.engine.service.binance;

import com.apunto.engine.dto.client.BinanceFuturesSymbolInfoClientDto;

import java.util.Optional;

public interface BinanceFuturesSymbolCatalog {

    Optional<SymbolResolution> resolve(String rawSymbol);

    boolean isSupported(String rawSymbol);

    int cachedSymbols();

    record SymbolResolution(
            String rawSymbol,
            String canonicalSymbol,
            BinanceFuturesSymbolInfoClientDto symbolInfo,
            boolean cacheStale
    ) {
    }
}
