package com.apunto.engine.hyperliquid.service.impl;

import com.apunto.engine.dto.client.BinanceFuturesMarketPriceClientDto;
import com.apunto.engine.service.ProcesBinanceService;
import com.apunto.engine.service.binance.BinanceFuturesSymbolCatalog;
import com.apunto.engine.shared.util.CopyLogAdvice;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestClientResponseException;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.http.HttpClient;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
@Service
public class BinanceFuturesPriceNormalizerService {

    private static final BigDecimal ZERO = BigDecimal.ZERO;
    private static final BigDecimal ONE = BigDecimal.ONE;
    private static final int CALC_SCALE = 18;
    private static final Pattern LEADING_MULTIPLIER_PATTERN = Pattern.compile("^(\\d+)([A-Z0-9]+)$");
    private static final List<String> KNOWN_QUOTES = List.of("USDT", "USDC", "FDUSD", "BUSD", "USD", "BTC", "ETH");
    private static final List<String> CONTRACT_MULTIPLIERS = List.of("1000000000", "1000000", "1000");

    private final boolean enabled;
    private final long cacheTtlMs;
    private final RestClient restClient;
    private final ObjectMapper objectMapper;
    private final BinanceFuturesSymbolCatalog symbolCatalog;
    private final ProcesBinanceService procesBinanceService;
    private final boolean enginePriceEnabled;
    private final boolean enginePriceAllowStale;
    private final Cache<String, BinancePriceReference> cache;
    private final Cache<String, Boolean> missCache;

    public BinanceFuturesPriceNormalizerService(
            ObjectMapper objectMapper,
            BinanceFuturesSymbolCatalog symbolCatalog,
            ProcesBinanceService procesBinanceService,
            @Value("${hyperliquid.direct-ingest.origin-store.binance-price-enabled:true}") boolean enabled,
            @Value("${hyperliquid.direct-ingest.origin-store.binance-price-engine-enabled:true}") boolean enginePriceEnabled,
            @Value("${hyperliquid.direct-ingest.origin-store.binance-price-engine-allow-stale:true}") boolean enginePriceAllowStale,
            @Value("${hyperliquid.direct-ingest.origin-store.binance-price-base-url:https://fapi.binance.com}") String baseUrl,
            @Value("${hyperliquid.direct-ingest.origin-store.binance-price-timeout-ms:350}") int timeoutMs,
            @Value("${hyperliquid.direct-ingest.origin-store.binance-price-cache-ttl-ms:750}") long cacheTtlMs,
            @Value("${hyperliquid.direct-ingest.origin-store.binance-price-cache-size:2048}") long cacheSize,
            @Value("${hyperliquid.direct-ingest.origin-store.binance-price-miss-cache-ttl-ms:60000}") long missCacheTtlMs,
            @Value("${hyperliquid.direct-ingest.origin-store.binance-price-miss-cache-size:4096}") long missCacheSize
    ) {
        this.objectMapper = objectMapper;
        this.symbolCatalog = symbolCatalog;
        this.procesBinanceService = procesBinanceService;
        this.enabled = enabled;
        this.enginePriceEnabled = enginePriceEnabled;
        this.enginePriceAllowStale = enginePriceAllowStale;
        this.cacheTtlMs = Math.max(1L, cacheTtlMs);
        HttpClient httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(Math.max(50, timeoutMs)))
                .build();
        JdkClientHttpRequestFactory requestFactory = new JdkClientHttpRequestFactory(httpClient);
        requestFactory.setReadTimeout(Duration.ofMillis(Math.max(50, timeoutMs)));
        this.restClient = RestClient.builder()
                .requestFactory(requestFactory)
                .baseUrl(baseUrl)
                .build();
        this.cache = Caffeine.newBuilder()
                .expireAfterWrite(Duration.ofMillis(Math.max(1L, cacheTtlMs)))
                .maximumSize(Math.max(128L, cacheSize))
                .build();
        this.missCache = Caffeine.newBuilder()
                .expireAfterWrite(Duration.ofMillis(Math.max(1000L, missCacheTtlMs)))
                .maximumSize(Math.max(128L, missCacheSize))
                .build();
        log.info("event=hyperliquid.origin_store.price_normalizer.config enabled={} enginePriceEnabled={} enginePriceAllowStale={} baseUrl={} timeoutMs={} cacheTtlMs={} cacheSize={} missCacheTtlMs={} missCacheSize={}",
                enabled, enginePriceEnabled, enginePriceAllowStale, safeLog(baseUrl), Math.max(50, timeoutMs), this.cacheTtlMs, Math.max(128L, cacheSize), Math.max(1000L, missCacheTtlMs), Math.max(128L, missCacheSize));
    }

    /**
     * Returns a unit-normalized price for the Hyperliquid/origin asset.
     *
     * Example:
     * - origin symbol: PEPEUSD, Binance symbol: 1000PEPEUSDT
     * - Binance ticker price is the price of 1000 PEPE.
     * - This method returns price = tickerPrice / 1000, so origin_store metrics stay on PEPE unit price.
     */
    public Optional<BinancePriceReference> resolve(String symbol) {
        if (!enabled) {
            return Optional.empty();
        }
        Optional<BinanceFuturesSymbolCatalog.SymbolResolution> symbolResolution;
        try {
            symbolResolution = symbolCatalog.resolve(symbol);
        } catch (RuntimeException ex) {
            log.warn("event=hyperliquid.origin_store.binance_price.skipped symbol={} reasonCode=binance_symbol_catalog_unavailable cacheSize={} copyImpact=origin_metrics_only action=continue_without_binance_price errClass={} errMsg=\"{}\"",
                    safeLog(symbol), safeCachedSymbols(), ex.getClass().getSimpleName(), safeLog(ex.getMessage()));
            return Optional.empty();
        }
        if (symbolResolution.isEmpty()) {
            log.debug("event=hyperliquid.origin_store.binance_price.skipped symbol={} reasonCode=binance_symbol_unsupported cacheSize={} humanMessage=no_encontre_un_contrato_binance_equivalente_pero_guardare_la_posicion_original_igual",
                    safeLog(symbol), safeCachedSymbols());
            return Optional.empty();
        }

        String canonical = symbolResolution.get().canonicalSymbol();
        BigDecimal contractMultiplier = contractMultiplier(symbol, canonical);
        String cacheKey = normalize(symbol) + "::" + canonical + "::" + contractMultiplier.stripTrailingZeros().toPlainString();

        BinancePriceReference cached = cache.getIfPresent(cacheKey);
        if (cached != null) {
            return Optional.of(cached.withSource("binance_cache"));
        }
        if (Boolean.TRUE.equals(missCache.getIfPresent(cacheKey))) {
            return Optional.empty();
        }

        if (Boolean.TRUE.equals(missCache.getIfPresent(canonical))) {
            missCache.put(cacheKey, Boolean.TRUE);
            return Optional.empty();
        }

        Optional<BinancePriceReference> fetched = fetch(canonical, symbol, contractMultiplier);
        if (fetched.isPresent()) {
            BinancePriceReference price = fetched.get();
            cache.put(cacheKey, price);
            return Optional.of(price);
        }
        return Optional.empty();
    }

    private int safeCachedSymbols() {
        try {
            return symbolCatalog.cachedSymbols();
        } catch (RuntimeException ex) {
            return -1;
        }
    }

    private Optional<BinancePriceReference> fetch(String canonicalSymbol, String rawSymbol, BigDecimal contractMultiplier) {
        long startedNs = System.nanoTime();
        Optional<BinancePriceReference> enginePrice = fetchFromEngine(canonicalSymbol, rawSymbol, contractMultiplier, startedNs);
        if (enginePrice.isPresent()) {
            return enginePrice;
        }
        try {
            String body = restClient.get()
                    .uri(uriBuilder -> uriBuilder.path("/fapi/v1/ticker/price")
                            .queryParam("symbol", canonicalSymbol)
                            .build())
                    .retrieve()
                    .body(String.class);
            if (body == null || body.isBlank()) {
                return Optional.empty();
            }
            JsonNode node = objectMapper.readTree(body);
            String rawPrice = node.path("price").asText(null);
            BigDecimal contractPrice = rawPrice == null ? ZERO : new BigDecimal(rawPrice);
            if (contractPrice.compareTo(ZERO) <= 0) {
                return Optional.empty();
            }
            BigDecimal safeMultiplier = contractMultiplier == null || contractMultiplier.compareTo(ZERO) <= 0 ? ONE : contractMultiplier;
            BigDecimal unitPrice = contractPrice.divide(safeMultiplier, CALC_SCALE, RoundingMode.HALF_UP).stripTrailingZeros();
            OffsetDateTime ts = OffsetDateTime.now(ZoneOffset.UTC);
            long elapsedMs = elapsedMs(startedNs);
            log.debug("event=hyperliquid.origin_store.binance_price.ok rawSymbol={} canonicalSymbol={} contractMultiplier={} contractPrice={} unitPrice={} elapsedMs={}",
                    safeLog(rawSymbol), safeLog(canonicalSymbol), safeMultiplier.toPlainString(), contractPrice.toPlainString(), unitPrice.toPlainString(), elapsedMs);
            return Optional.of(new BinancePriceReference(
                    canonicalSymbol,
                    unitPrice,
                    "binance_futures_ticker",
                    ts,
                    elapsedMs,
                    0L,
                    contractPrice,
                    safeMultiplier,
                    rawSymbol,
                    canonicalSymbol
            ));
        } catch (RestClientResponseException ex) {
            if (isInvalidSymbol(ex)) {
                missCache.put(canonicalSymbol, Boolean.TRUE);
                log.debug("event=hyperliquid.origin_store.binance_price.invalid_symbol symbol={} httpStatus={} elapsedMs={}",
                        safeLog(canonicalSymbol), ex.getStatusCode().value(), elapsedMs(startedNs));
                return Optional.empty();
            }
            log.warn("event=hyperliquid.origin_store.binance_price.failed reasonCode=binance_price_failed symbol={} fallbackUsed=false fallbackSource=none priceUsed=null copyImpact=origin_metrics_only errClass={} httpStatus={} errMsg=\"{}\" elapsedMs={} {}",
                    safeLog(canonicalSymbol), ex.getClass().getSimpleName(), ex.getStatusCode().value(), safeLog(ex.getMessage()), elapsedMs(startedNs),
                    CopyLogAdvice.fields("binance_price_failed", CopyLogAdvice.context(null, null, null, null, null, null, null, "binance_price")));
            return Optional.empty();
        } catch (RestClientException | IllegalStateException | IllegalArgumentException | ArithmeticException | JsonProcessingException ex) {
            log.warn("event=hyperliquid.origin_store.binance_price.failed reasonCode=binance_price_failed symbol={} fallbackUsed=false fallbackSource=none priceUsed=null copyImpact=origin_metrics_only errClass={} errMsg=\"{}\" elapsedMs={} {}",
                    safeLog(canonicalSymbol), ex.getClass().getSimpleName(), safeLog(ex.getMessage()), elapsedMs(startedNs),
                    CopyLogAdvice.fields("binance_price_failed", CopyLogAdvice.context(null, null, null, null, null, null, null, "binance_price")));
            return Optional.empty();
        }
    }

    private Optional<BinancePriceReference> fetchFromEngine(String canonicalSymbol,
                                                            String rawSymbol,
                                                            BigDecimal contractMultiplier,
                                                            long startedNs) {
        if (!enginePriceEnabled) {
            return Optional.empty();
        }
        try {
            Optional<BinanceFuturesMarketPriceClientDto> response =
                    procesBinanceService.getMarketPrice(canonicalSymbol, "PNL", enginePriceAllowStale);
            if (response.isEmpty()) {
                return Optional.empty();
            }
            BinanceFuturesMarketPriceClientDto marketPrice = response.get();
            BigDecimal contractPrice = marketPrice.getPrice();
            if (!marketPrice.isAvailable() || contractPrice == null || contractPrice.compareTo(ZERO) <= 0) {
                log.debug("event=hyperliquid.origin_store.binance_price.engine_unavailable rawSymbol={} canonicalSymbol={} available={} reasonCode={} source={} ageMs={}",
                        safeLog(rawSymbol), safeLog(canonicalSymbol), marketPrice.isAvailable(), safeLog(marketPrice.getReasonCode()), safeLog(marketPrice.getSource()), marketPrice.getAgeMs());
                return Optional.empty();
            }

            BigDecimal safeMultiplier = contractMultiplier == null || contractMultiplier.compareTo(ZERO) <= 0 ? ONE : contractMultiplier;
            BigDecimal unitPrice = contractPrice.divide(safeMultiplier, CALC_SCALE, RoundingMode.HALF_UP).stripTrailingZeros();
            OffsetDateTime ts = marketPrice.getReceivedAtMs() == null
                    ? OffsetDateTime.now(ZoneOffset.UTC)
                    : OffsetDateTime.ofInstant(Instant.ofEpochMilli(marketPrice.getReceivedAtMs()), ZoneOffset.UTC);
            long elapsedMs = elapsedMs(startedNs);
            long referenceDiffMs = marketPrice.getAgeMs() == null ? 0L : Math.max(0L, marketPrice.getAgeMs());
            String source = marketPrice.getSource() == null || marketPrice.getSource().isBlank()
                    ? "binance_engine_market_price"
                    : "binance_engine_" + marketPrice.getSource();

            log.debug("event=hyperliquid.origin_store.binance_price.engine_ok rawSymbol={} canonicalSymbol={} contractMultiplier={} contractPrice={} unitPrice={} source={} ageMs={} elapsedMs={}",
                    safeLog(rawSymbol), safeLog(canonicalSymbol), safeMultiplier.toPlainString(), contractPrice.toPlainString(), unitPrice.toPlainString(), safeLog(source), referenceDiffMs, elapsedMs);

            return Optional.of(new BinancePriceReference(
                    canonicalSymbol,
                    unitPrice,
                    source,
                    ts,
                    elapsedMs,
                    referenceDiffMs,
                    contractPrice,
                    safeMultiplier,
                    rawSymbol,
                    canonicalSymbol
            ));
        } catch (RuntimeException ex) {
            log.warn("event=hyperliquid.origin_store.binance_price.engine_failed symbol={} fallback=rest_public_ticker errClass={} errMsg=\"{}\" elapsedMs={}",
                    safeLog(canonicalSymbol), ex.getClass().getSimpleName(), safeLog(ex.getMessage()), elapsedMs(startedNs));
            return Optional.empty();
        }
    }

    private boolean isInvalidSymbol(RestClientResponseException ex) {
        int status = ex.getStatusCode().value();
        if (status == 404) {
            return true;
        }
        if (status != 400) {
            return false;
        }
        String body = ex.getResponseBodyAsString();
        return body != null && body.contains("-1121");
    }

    private BigDecimal contractMultiplier(String rawSymbol, String canonicalSymbol) {
        String rawBase = baseWithoutQuote(rawSymbol);
        String canonicalBase = baseWithoutQuote(canonicalSymbol);
        if (rawBase == null || canonicalBase == null) {
            return ONE;
        }
        String rawAsset = stripKnownContractMultiplier(rawBase);
        String canonicalAsset = stripKnownContractMultiplier(canonicalBase);
        if (rawAsset == null || canonicalAsset == null || !rawAsset.equals(canonicalAsset)) {
            return ONE;
        }
        BigDecimal rawMultiplier = leadingKnownContractMultiplier(rawBase);
        BigDecimal canonicalMultiplier = leadingKnownContractMultiplier(canonicalBase);
        if (rawMultiplier.compareTo(ZERO) <= 0 || canonicalMultiplier.compareTo(ZERO) <= 0) {
            return ONE;
        }
        return canonicalMultiplier.divide(rawMultiplier, CALC_SCALE, RoundingMode.HALF_UP).stripTrailingZeros();
    }

    private String baseWithoutQuote(String symbol) {
        String normalized = normalize(symbol);
        if (normalized == null) {
            return null;
        }
        for (String quote : KNOWN_QUOTES) {
            if (normalized.endsWith(quote) && normalized.length() > quote.length()) {
                return normalized.substring(0, normalized.length() - quote.length());
            }
        }
        return normalized;
    }

    private String stripKnownContractMultiplier(String base) {
        if (base == null || base.isBlank()) {
            return null;
        }
        for (String multiplier : CONTRACT_MULTIPLIERS) {
            if (base.startsWith(multiplier) && base.length() > multiplier.length()) {
                return base.substring(multiplier.length());
            }
        }
        return base;
    }

    private BigDecimal leadingKnownContractMultiplier(String base) {
        if (base == null || base.isBlank()) {
            return ONE;
        }
        Matcher matcher = LEADING_MULTIPLIER_PATTERN.matcher(base);
        if (!matcher.matches()) {
            return ONE;
        }
        String multiplier = matcher.group(1);
        return CONTRACT_MULTIPLIERS.contains(multiplier) ? new BigDecimal(multiplier) : ONE;
    }

    private String normalize(String raw) {
        if (raw == null) {
            return null;
        }
        String value = raw.trim()
                .toUpperCase(Locale.ROOT)
                .replace("/", "")
                .replace("-", "")
                .replace("_", "")
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

    public record BinancePriceReference(
            String symbol,
            BigDecimal price,
            String source,
            OffsetDateTime referenceTs,
            long fetchElapsedMs,
            long referenceDiffMs,
            BigDecimal contractPrice,
            BigDecimal contractMultiplier,
            String rawSymbol,
            String canonicalSymbol
    ) {
        BinancePriceReference withSource(String newSource) {
            return new BinancePriceReference(symbol, price, newSource, referenceTs, fetchElapsedMs, referenceDiffMs, contractPrice, contractMultiplier, rawSymbol, canonicalSymbol);
        }
    }
}
