package com.apunto.engine.service.copy.symbol;

import com.apunto.engine.dto.client.BinanceFuturesSymbolInfoClientDto;
import com.apunto.engine.service.binance.BinanceFuturesSymbolCatalog;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BinanceTargetSymbolResolverTest {

    @Test
    void resolvesUsdcTargetFromUsdtSourceWhenTargetExists() {
        BinanceTargetSymbolResolver resolver = new BinanceTargetSymbolResolver(
                catalog(Map.of("BTCUSDC", symbol("BTCUSDC", "BTC", "USDC")))
        );

        CopySymbolResolution resolution = resolver.resolve("BTCUSDT", "USDC");

        assertTrue(resolution.resolved());
        assertEquals("BTCUSDT", resolution.sourceSymbol());
        assertEquals("BTCUSDC", resolution.targetSymbol());
        assertEquals("BTC", resolution.baseAsset());
        assertEquals("USDC", resolution.quoteAsset());
        assertEquals("SYMBOL_RESOLVED", resolution.reasonCode());
    }

    @Test
    void keepsUsdtTargetForUsdtUser() {
        BinanceTargetSymbolResolver resolver = new BinanceTargetSymbolResolver(
                catalog(Map.of("BTCUSDT", symbol("BTCUSDT", "BTC", "USDT")))
        );

        CopySymbolResolution resolution = resolver.resolve("BTCUSDT", "USDT");

        assertTrue(resolution.resolved());
        assertEquals("BTCUSDT", resolution.targetSymbol());
        assertEquals("USDT", resolution.capitalAsset());
    }

    @Test
    void skipsUnavailableUsdcTargetWithoutBlockingCaller() {
        BinanceTargetSymbolResolver resolver = new BinanceTargetSymbolResolver(catalog(Map.of()));

        CopySymbolResolution resolution = resolver.resolve("HYPEUSDT", "USDC");

        assertFalse(resolution.resolved());
        assertEquals("HYPEUSDC", resolution.targetSymbol());
        assertEquals("SYMBOL_TARGET_NOT_AVAILABLE", resolution.reasonCode());
    }

    @Test
    void resolvesAfterCatalogRefreshAddsTarget() {
        AtomicReference<Map<String, BinanceFuturesSymbolInfoClientDto>> symbols =
                new AtomicReference<>(Map.of());
        BinanceTargetSymbolResolver resolver = new BinanceTargetSymbolResolver(catalog(symbols));

        assertFalse(resolver.resolve("HYPEUSDT", "USDC").resolved());

        symbols.set(Map.of("HYPEUSDC", symbol("HYPEUSDC", "HYPE", "USDC")));

        CopySymbolResolution resolution = resolver.resolve("HYPEUSDT", "USDC");

        assertTrue(resolution.resolved());
        assertEquals("HYPEUSDC", resolution.targetSymbol());
    }

    private static BinanceFuturesSymbolCatalog catalog(Map<String, BinanceFuturesSymbolInfoClientDto> symbols) {
        AtomicReference<Map<String, BinanceFuturesSymbolInfoClientDto>> ref = new AtomicReference<>(symbols);
        return catalog(ref);
    }

    private static BinanceFuturesSymbolCatalog catalog(AtomicReference<Map<String, BinanceFuturesSymbolInfoClientDto>> symbols) {
        return new BinanceFuturesSymbolCatalog() {
            @Override
            public Optional<SymbolResolution> resolve(String rawSymbol) {
                BinanceFuturesSymbolInfoClientDto symbol = symbols.get().get(rawSymbol);
                return symbol == null
                        ? Optional.empty()
                        : Optional.of(new SymbolResolution(rawSymbol, symbol.getSymbol(), symbol, false));
            }

            @Override
            public boolean isSupported(String rawSymbol) {
                return symbols.get().containsKey(rawSymbol);
            }

            @Override
            public int cachedSymbols() {
                return symbols.get().size();
            }
        };
    }

    private static BinanceFuturesSymbolInfoClientDto symbol(String symbol, String base, String quote) {
        BinanceFuturesSymbolInfoClientDto dto = new BinanceFuturesSymbolInfoClientDto();
        dto.setSymbol(symbol);
        dto.setBaseAsset(base);
        dto.setQuoteAsset(quote);
        dto.setMarginAsset(quote);
        dto.setStatus("TRADING");
        dto.setContractType("PERPETUAL");
        dto.setOrderTypes(java.util.List.of("MARKET"));
        return dto;
    }
}
