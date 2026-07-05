package com.apunto.engine.service.copy.symbol;

import com.apunto.engine.dto.client.BinanceFuturesSymbolInfoClientDto;
import com.apunto.engine.service.binance.BinanceFuturesSymbolCatalog;
import com.apunto.engine.shared.enums.FuturesCapitalAsset;
import com.apunto.engine.shared.exception.SkipExecutionException;
import com.apunto.engine.shared.util.CopySymbolIdentity;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Locale;
import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class BinanceTargetSymbolResolver implements CopySymbolResolver {

    private final BinanceFuturesSymbolCatalog symbolCatalog;

    @Override
    public CopySymbolResolution resolve(String sourceSymbol, String capitalAsset) {
        final String normalizedSource = CopySymbolIdentity.normalizeSymbol(sourceSymbol);
        final FuturesCapitalAsset safeCapitalAsset;
        try {
            safeCapitalAsset = FuturesCapitalAsset.fromNullable(capitalAsset);
        } catch (SkipExecutionException ex) {
            return CopySymbolResolution.skipped(
                    normalizedSource,
                    null,
                    null,
                    null,
                    normalize(capitalAsset),
                    "SYMBOL_CAPITAL_ASSET_INVALID"
            );
        }

        final String capital = safeCapitalAsset.name();
        final String baseAsset = CopySymbolIdentity.primaryBaseAsset(normalizedSource);
        if (normalizedSource == null || baseAsset == null || baseAsset.isBlank()) {
            return CopySymbolResolution.skipped(
                    normalizedSource,
                    null,
                    baseAsset,
                    capital,
                    capital,
                    "SYMBOL_SOURCE_INVALID"
            );
        }

        final String targetSymbol = baseAsset + capital;
        final Optional<BinanceFuturesSymbolCatalog.SymbolResolution> resolved = symbolCatalog.resolve(targetSymbol);
        if (resolved.isEmpty()) {
            log.warn(
                    "copy.symbol_resolver.skipped sourceSymbol={} targetSymbol={} baseAsset={} capitalAsset={} reason=SYMBOL_TARGET_NOT_AVAILABLE",
                    safeLog(normalizedSource),
                    safeLog(targetSymbol),
                    safeLog(baseAsset),
                    capital
            );
            return CopySymbolResolution.skipped(
                    normalizedSource,
                    targetSymbol,
                    baseAsset,
                    capital,
                    capital,
                    "SYMBOL_TARGET_NOT_AVAILABLE"
            );
        }

        final BinanceFuturesSymbolCatalog.SymbolResolution catalogResolution = resolved.get();
        final BinanceFuturesSymbolInfoClientDto symbolInfo = catalogResolution.symbolInfo();
        final String quoteAsset = normalize(symbolInfo == null ? null : symbolInfo.getQuoteAsset());
        final String marginAsset = normalize(symbolInfo == null ? null : symbolInfo.getMarginAsset());
        final String canonical = normalize(catalogResolution.canonicalSymbol());

        if (!capital.equals(quoteAsset) || !capital.equals(marginAsset)) {
            log.warn(
                    "copy.symbol_resolver.skipped sourceSymbol={} targetSymbol={} canonicalSymbol={} capitalAsset={} quoteAsset={} marginAsset={} reason=SYMBOL_TARGET_QUOTE_ASSET_MISMATCH",
                    safeLog(normalizedSource),
                    safeLog(targetSymbol),
                    safeLog(canonical),
                    capital,
                    safeLog(quoteAsset),
                    safeLog(marginAsset)
            );
            return CopySymbolResolution.skipped(
                    normalizedSource,
                    canonical == null ? targetSymbol : canonical,
                    baseAsset,
                    quoteAsset,
                    capital,
                    "SYMBOL_TARGET_QUOTE_ASSET_MISMATCH"
            );
        }

        final String finalTarget = canonical == null ? targetSymbol : canonical;
        log.info(
                "copy.symbol_resolver.resolved sourceSymbol={} targetSymbol={} baseAsset={} capitalAsset={} quoteAsset={} cacheStale={}",
                safeLog(normalizedSource),
                safeLog(finalTarget),
                safeLog(baseAsset),
                capital,
                quoteAsset,
                catalogResolution.cacheStale()
        );
        return CopySymbolResolution.resolved(
                normalizedSource,
                finalTarget,
                baseAsset,
                quoteAsset,
                capital,
                catalogResolution.cacheStale()
        );
    }

    private static String normalize(String raw) {
        if (raw == null || raw.isBlank()) return null;
        String value = raw.trim().toUpperCase(Locale.ROOT);
        return value.isBlank() ? null : value;
    }

    private static String safeLog(String raw) {
        if (raw == null || raw.isBlank()) return "NA";
        return raw.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ');
    }
}
