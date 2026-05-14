package com.apunto.engine.hyperliquid.service.impl;

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

import java.math.BigDecimal;
import java.net.http.HttpClient;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

@Slf4j
@Service
public class BinanceFuturesPriceNormalizerService {

    private static final BigDecimal ZERO = BigDecimal.ZERO;

    private final boolean enabled;
    private final long cacheTtlMs;
    private final RestClient restClient;
    private final ObjectMapper objectMapper;
    private final Cache<String, BinancePriceReference> cache;

    public BinanceFuturesPriceNormalizerService(
            ObjectMapper objectMapper,
            @Value("${hyperliquid.direct-ingest.origin-store.binance-price-enabled:true}") boolean enabled,
            @Value("${hyperliquid.direct-ingest.origin-store.binance-price-base-url:https://fapi.binance.com}") String baseUrl,
            @Value("${hyperliquid.direct-ingest.origin-store.binance-price-timeout-ms:350}") int timeoutMs,
            @Value("${hyperliquid.direct-ingest.origin-store.binance-price-cache-ttl-ms:750}") long cacheTtlMs,
            @Value("${hyperliquid.direct-ingest.origin-store.binance-price-cache-size:2048}") long cacheSize
    ) {
        this.objectMapper = objectMapper;
        this.enabled = enabled;
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
        log.info("event=hyperliquid.origin_store.price_normalizer.config enabled={} baseUrl={} timeoutMs={} cacheTtlMs={} cacheSize={}",
                enabled, safeLog(baseUrl), Math.max(50, timeoutMs), this.cacheTtlMs, Math.max(128L, cacheSize));
    }

    public Optional<BinancePriceReference> resolve(String symbol) {
        if (!enabled) {
            return Optional.empty();
        }
        String normalized = toBinanceSymbol(symbol);
        if (normalized == null || normalized.isBlank()) {
            return Optional.empty();
        }

        BinancePriceReference cached = cache.getIfPresent(normalized);
        if (cached != null) {
            return Optional.of(cached.withSource("binance_cache"));
        }

        for (String candidate : candidates(normalized)) {
            Optional<BinancePriceReference> fetched = fetch(candidate);
            if (fetched.isPresent()) {
                BinancePriceReference price = fetched.get();
                cache.put(normalized, price);
                cache.put(candidate, price);
                return Optional.of(price);
            }
        }
        return Optional.empty();
    }

    private Optional<BinancePriceReference> fetch(String symbol) {
        long startedNs = System.nanoTime();
        try {
            String body = restClient.get()
                    .uri(uriBuilder -> uriBuilder.path("/fapi/v1/ticker/price")
                            .queryParam("symbol", symbol)
                            .build())
                    .retrieve()
                    .body(String.class);
            if (body == null || body.isBlank()) {
                return Optional.empty();
            }
            JsonNode node = objectMapper.readTree(body);
            String rawPrice = node.path("price").asText(null);
            BigDecimal price = rawPrice == null ? ZERO : new BigDecimal(rawPrice);
            if (price.compareTo(ZERO) <= 0) {
                return Optional.empty();
            }
            OffsetDateTime ts = OffsetDateTime.now(ZoneOffset.UTC);
            long elapsedMs = elapsedMs(startedNs);
            log.debug("event=hyperliquid.origin_store.binance_price.ok symbol={} price={} elapsedMs={}", symbol, price, elapsedMs);
            return Optional.of(new BinancePriceReference(symbol, price, "binance_futures_ticker", ts, elapsedMs, 0L));
        } catch (RestClientException | IllegalStateException | IllegalArgumentException | ArithmeticException | JsonProcessingException ex) {
            log.warn("event=hyperliquid.origin_store.binance_price.failed symbol={} errClass={} errMsg=\"{}\" elapsedMs={}",
                    safeLog(symbol), ex.getClass().getSimpleName(), safeLog(ex.getMessage()), elapsedMs(startedNs));
            return Optional.empty();
        }
    }

    private List<String> candidates(String symbol) {
        List<String> symbols = new ArrayList<>();
        addIfMissing(symbols, symbol);
        addIfMissing(symbols, "1000" + symbol);
        if (symbol.startsWith("1000") && symbol.length() > 4) {
            addIfMissing(symbols, symbol.substring(4));
        }
        return symbols;
    }

    private void addIfMissing(List<String> values, String value) {
        if (value != null && !value.isBlank() && !values.contains(value)) {
            values.add(value);
        }
    }

    private String toBinanceSymbol(String symbol) {
        if (symbol == null) {
            return null;
        }
        String value = symbol.trim()
                .toUpperCase(Locale.ROOT)
                .replace("/", "")
                .replace("-", "")
                .replace("_", "")
                .replace(" ", "");
        if (value.endsWith("USD")
                && !value.endsWith("USDT")
                && !value.endsWith("USDC")
                && !value.endsWith("BUSD")) {
            return value.substring(0, value.length() - 3) + "USDT";
        }
        return value;
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
            long referenceDiffMs
    ) {
        BinancePriceReference withSource(String newSource) {
            return new BinancePriceReference(symbol, price, newSource, referenceTs, fetchElapsedMs, referenceDiffMs);
        }
    }
}
