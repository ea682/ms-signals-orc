package com.apunto.engine.service.binance;

import com.apunto.engine.dto.client.BinanceFuturesSymbolInfoClientDto;
import com.apunto.engine.service.ProcesBinanceService;
import com.apunto.engine.shared.exception.CopySymbolMetadataException;
import com.apunto.engine.shared.exception.EngineException;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
@Service
public class BinanceFuturesSymbolCatalogService implements BinanceFuturesSymbolCatalog {

    private static final List<String> KNOWN_QUOTES = List.of("USDT", "USDC", "FDUSD", "BUSD", "BTC", "ETH");
    private static final Pattern LEADING_MULTIPLIER_PATTERN = Pattern.compile("^(\\d+)([A-Z0-9]+)$");

    private final ProcesBinanceService procesBinanceService;
    private final String symbolsApiKey;
    private final long ttlMs;
    private final boolean warmupOnStart;
    private final boolean requireTradingStatus;
    private final AtomicBoolean refreshInFlight = new AtomicBoolean(false);
    private final Object refreshLock = new Object();
    private volatile CatalogSnapshot snapshot = CatalogSnapshot.empty();

    public BinanceFuturesSymbolCatalogService(
            ProcesBinanceService procesBinanceService,
            @Value("${binance.symbols.api-key:}") String symbolsApiKey,
            @Value("${binance.symbols.cache-ttl-ms:60000}") long ttlMs,
            @Value("${binance.symbols.warmup-on-start:true}") boolean warmupOnStart,
            @Value("${binance.symbols.require-trading-status:true}") boolean requireTradingStatus
    ) {
        this.procesBinanceService = procesBinanceService;
        this.symbolsApiKey = symbolsApiKey;
        this.ttlMs = Math.max(10_000L, ttlMs);
        this.warmupOnStart = warmupOnStart;
        this.requireTradingStatus = requireTradingStatus;
        log.info("event=binance.symbol_catalog.config ttlMs={} warmupOnStart={} requireTradingStatus={}",
                this.ttlMs, warmupOnStart, requireTradingStatus);
    }

    @PostConstruct
    public void warmup() {
        if (!warmupOnStart) {
            return;
        }
        try {
            refreshSync("startup");
        } catch (RuntimeException ex) {
            log.warn("event=binance.symbol_catalog.warmup_failed phase=startup action=continue_startup errClass={} errMsg=\"{}\"",
                    ex.getClass().getSimpleName(), safeLog(ex.getMessage()));
        }
    }

    @Scheduled(fixedDelayString = "${binance.symbols.cache-ttl-ms:60000}")
    public void scheduledRefresh() {
        refreshAsync("scheduled");
    }

    @Override
    public Optional<SymbolResolution> resolve(String rawSymbol) {
        String normalized = normalize(rawSymbol);
        if (normalized == null || normalized.isBlank()) {
            return Optional.empty();
        }
        CatalogSnapshot current = getSnapshot();
        for (String candidate : buildCandidates(normalized)) {
            BinanceFuturesSymbolInfoClientDto direct = current.bySymbol().get(candidate);
            if (isTradable(direct)) {
                return Optional.of(new SymbolResolution(rawSymbol, candidate, direct, current.stale()));
            }
            if (current.ambiguousAliases().contains(candidate)) {
                log.info("event=binance.symbol_catalog.unsupported rawSymbol={} candidate={} reasonCode=symbol_alias_ambiguous cacheSize={} stale={}",
                        safeLog(rawSymbol), safeLog(candidate), current.bySymbol().size(), current.stale());
                return Optional.empty();
            }
            String canonical = current.aliasToCanonical().get(candidate);
            BinanceFuturesSymbolInfoClientDto aliased = canonical == null ? null : current.bySymbol().get(canonical);
            if (isTradable(aliased)) {
                return Optional.of(new SymbolResolution(rawSymbol, canonical, aliased, current.stale()));
            }
        }
        log.debug("event=binance.symbol_catalog.unsupported rawSymbol={} normalized={} reasonCode=symbol_not_found cacheSize={} stale={}",
                safeLog(rawSymbol), safeLog(normalized), current.bySymbol().size(), current.stale());
        return Optional.empty();
    }

    @Override
    public boolean isSupported(String rawSymbol) {
        return resolve(rawSymbol).isPresent();
    }

    @Override
    public int cachedSymbols() {
        return snapshot.bySymbol().size();
    }

    private CatalogSnapshot getSnapshot() {
        CatalogSnapshot current = snapshot;
        long now = System.currentTimeMillis();
        if (!current.bySymbol().isEmpty() && current.expiresAtMs() > now) {
            return current.withStale(false);
        }
        if (!current.bySymbol().isEmpty()) {
            refreshAsync("stale_hit");
            return current.withStale(true);
        }
        synchronized (refreshLock) {
            CatalogSnapshot locked = snapshot;
            if (!locked.bySymbol().isEmpty()) {
                refreshAsync("post_lock_stale_hit");
                return locked.withStale(locked.expiresAtMs() <= System.currentTimeMillis());
            }
            return refreshSync("empty_cache").withStale(false);
        }
    }

    private void refreshAsync(String phase) {
        if (!refreshInFlight.compareAndSet(false, true)) {
            return;
        }
        Thread.ofVirtual().name("binance-symbol-catalog-refresh-", 0).start(() -> {
            try {
                refreshSync(phase);
            } catch (EngineException | RestClientException | IllegalStateException ex) {
                log.warn("event=binance.symbol_catalog.refresh_failed phase={} action=keep_stale cacheSize={} errClass={} errMsg=\"{}\"",
                        safeLog(phase), snapshot.bySymbol().size(), ex.getClass().getSimpleName(), safeLog(ex.getMessage()));
            } finally {
                refreshInFlight.set(false);
            }
        });
    }

    private CatalogSnapshot refreshSync(String phase) {
        long startedNs = System.nanoTime();
        try {
            List<BinanceFuturesSymbolInfoClientDto> symbols = procesBinanceService.getSymbols(symbolsApiKey);
            CatalogSnapshot loaded = buildSnapshot(symbols, System.currentTimeMillis() + ttlMs);
            snapshot = loaded;
            log.info("event=binance.symbol_catalog.refresh_ok phase={} symbols={} aliases={} ambiguousAliases={} ttlMs={} elapsedMs={}",
                    safeLog(phase), loaded.bySymbol().size(), loaded.aliasToCanonical().size(), loaded.ambiguousAliases().size(), ttlMs, elapsedMs(startedNs));
            return loaded;
        } catch (EngineException | RestClientException | IllegalStateException ex) {
            CatalogSnapshot stale = snapshot;
            if (!stale.bySymbol().isEmpty()) {
                log.warn("event=binance.symbol_catalog.refresh_failed phase={} action=use_stale cacheSize={} errClass={} errMsg=\"{}\" elapsedMs={}",
                        safeLog(phase), stale.bySymbol().size(), ex.getClass().getSimpleName(), safeLog(ex.getMessage()), elapsedMs(startedNs));
                return stale.withStale(true);
            }
            throw new CopySymbolMetadataException(
                    "No se pudo cargar catalogo de símbolos Binance Futures",
                    ex,
                    Map.of("phase", phase, "elapsedMs", elapsedMs(startedNs))
            );
        }
    }

    private CatalogSnapshot buildSnapshot(List<BinanceFuturesSymbolInfoClientDto> symbols, long expiresAtMs) {
        Map<String, BinanceFuturesSymbolInfoClientDto> bySymbol = symbols == null
                ? Collections.emptyMap()
                : symbols.stream()
                .filter(Objects::nonNull)
                .filter(s -> s.getSymbol() != null && !s.getSymbol().isBlank())
                .filter(this::isTradable)
                .collect(Collectors.toMap(
                        s -> normalize(s.getSymbol()),
                        s -> s,
                        (a, b) -> a
                ));
        if (bySymbol.isEmpty()) {
            throw new CopySymbolMetadataException("ms-binance devolvió 0 símbolos Binance Futures operables", Map.of("symbolsNull", symbols == null));
        }

        Map<String, String> aliasToCanonical = new HashMap<>();
        Set<String> ambiguousAliases = new HashSet<>();
        for (String canonical : bySymbol.keySet()) {
            registerAlias(aliasToCanonical, ambiguousAliases, canonical, canonical);
            for (String alias : deriveAliases(canonical)) {
                registerAlias(aliasToCanonical, ambiguousAliases, alias, canonical);
            }
        }
        return new CatalogSnapshot(expiresAtMs, Map.copyOf(bySymbol), Map.copyOf(aliasToCanonical), Set.copyOf(ambiguousAliases), false);
    }

    private boolean isTradable(BinanceFuturesSymbolInfoClientDto symbolInfo) {
        if (symbolInfo == null) {
            return false;
        }
        if (!requireTradingStatus) {
            return true;
        }
        String status = symbolInfo.getStatus();
        return status != null && "TRADING".equalsIgnoreCase(status.trim());
    }

    private void registerAlias(Map<String, String> aliasToCanonical, Set<String> ambiguousAliases, String alias, String canonical) {
        String aliasKey = normalize(alias);
        String canonicalKey = normalize(canonical);
        if (aliasKey == null || canonicalKey == null) {
            return;
        }
        String existing = aliasToCanonical.putIfAbsent(aliasKey, canonicalKey);
        if (existing != null && !existing.equals(canonicalKey)) {
            aliasToCanonical.remove(aliasKey);
            ambiguousAliases.add(aliasKey);
        }
    }

    private List<String> deriveAliases(String canonicalSymbol) {
        List<String> aliases = new ArrayList<>();
        String symbol = normalize(canonicalSymbol);
        if (symbol == null) {
            return aliases;
        }
        String quote = extractQuote(symbol);
        if (quote == null) {
            return aliases;
        }
        String base = symbol.substring(0, symbol.length() - quote.length());
        Matcher matcher = LEADING_MULTIPLIER_PATTERN.matcher(base);
        if (matcher.matches()) {
            String multiplier = matcher.group(1);
            String strippedBase = matcher.group(2);
            aliases.add(strippedBase + quote);
            String compact = compactMultiplier(multiplier);
            if (compact != null) {
                aliases.add(compact + strippedBase + quote);
                aliases.add(strippedBase + compact + quote);
            }
        }
        return aliases;
    }

    private List<String> buildCandidates(String rawSymbol) {
        LinkedHashSet<String> candidates = new LinkedHashSet<>();
        String normalized = normalize(rawSymbol);
        if (normalized == null) {
            return List.of();
        }
        candidates.add(normalized);
        if (normalized.endsWith("USD") && !normalized.endsWith("USDT") && !normalized.endsWith("USDC") && !normalized.endsWith("BUSD")) {
            candidates.add(normalized.substring(0, normalized.length() - 3) + "USDT");
        }
        candidates.add("1000" + normalized);
        if (normalized.startsWith("1000") && normalized.length() > 4) {
            candidates.add(normalized.substring(4));
        }
        return new ArrayList<>(candidates);
    }

    private String extractQuote(String symbol) {
        if (symbol == null || symbol.isBlank()) {
            return null;
        }
        for (String quote : KNOWN_QUOTES) {
            if (symbol.endsWith(quote)) {
                return quote;
            }
        }
        return null;
    }

    private String compactMultiplier(String multiplierRaw) {
        try {
            BigDecimal value = new BigDecimal(multiplierRaw);
            BigDecimal thousand = new BigDecimal("1000");
            BigDecimal million = new BigDecimal("1000000");
            BigDecimal billion = new BigDecimal("1000000000");
            if (value.compareTo(billion) >= 0 && value.remainder(billion).compareTo(BigDecimal.ZERO) == 0) {
                BigDecimal units = value.divide(billion).stripTrailingZeros();
                return units.compareTo(BigDecimal.ONE) == 0 ? "B" : units.toPlainString() + "B";
            }
            if (value.compareTo(million) >= 0 && value.remainder(million).compareTo(BigDecimal.ZERO) == 0) {
                BigDecimal units = value.divide(million).stripTrailingZeros();
                return units.compareTo(BigDecimal.ONE) == 0 ? "M" : units.toPlainString() + "M";
            }
            if (value.compareTo(thousand) >= 0 && value.remainder(thousand).compareTo(BigDecimal.ZERO) == 0) {
                BigDecimal units = value.divide(thousand).stripTrailingZeros();
                return units.compareTo(BigDecimal.ONE) == 0 ? "K" : units.toPlainString() + "K";
            }
            return multiplierRaw;
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    private String normalize(String raw) {
        if (raw == null) {
            return null;
        }
        String value = raw.trim()
                .toUpperCase(Locale.ROOT)
                .replace("-", "")
                .replace("_", "")
                .replace("/", "")
                .replace(".", "")
                .replace(" ", "");
        return value.isBlank() ? null : value;
    }

    private long elapsedMs(long startedNs) {
        return Duration.ofNanos(System.nanoTime() - startedNs).toMillis();
    }

    private String safeLog(String value) {
        if (value == null || value.isBlank()) {
            return "NA";
        }
        String clean = value.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ').replace('"', '\'');
        return clean.length() > 500 ? clean.substring(0, 500) : clean;
    }

    private record CatalogSnapshot(
            long expiresAtMs,
            Map<String, BinanceFuturesSymbolInfoClientDto> bySymbol,
            Map<String, String> aliasToCanonical,
            Set<String> ambiguousAliases,
            boolean stale
    ) {
        static CatalogSnapshot empty() {
            return new CatalogSnapshot(0L, Map.of(), Map.of(), Set.of(), false);
        }

        CatalogSnapshot withStale(boolean newStale) {
            return new CatalogSnapshot(expiresAtMs, bySymbol, aliasToCanonical, ambiguousAliases, newStale);
        }
    }
}
