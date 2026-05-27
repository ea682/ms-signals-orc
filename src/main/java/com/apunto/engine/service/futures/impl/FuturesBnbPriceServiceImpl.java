package com.apunto.engine.service.futures.impl;

import com.apunto.engine.service.futures.FuturesBnbPriceService;
import com.apunto.engine.shared.enums.FuturesCapitalAsset;
import com.apunto.engine.shared.exception.FuturesPriceLookupException;
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
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class FuturesBnbPriceServiceImpl implements FuturesBnbPriceService {

    private static final BigDecimal ZERO = BigDecimal.ZERO;
    private static final String BASE_ASSET = "BNB";

    private final RestClient restClient;
    private final ObjectMapper objectMapper;
    private final Cache<String, BigDecimal> cache;

    public FuturesBnbPriceServiceImpl(
            ObjectMapper objectMapper,
            @Value("${futures.capital-maintenance.bnb-price-base-url:https://fapi.binance.com}") String baseUrl,
            @Value("${futures.capital-maintenance.bnb-price-timeout-ms:1000}") int timeoutMs,
            @Value("${futures.capital-maintenance.bnb-price-cache-ttl-ms:60000}") long cacheTtlMs
    ) {
        this.objectMapper = objectMapper;
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
                .expireAfterWrite(Duration.ofMillis(Math.max(1000L, cacheTtlMs)))
                .maximumSize(8)
                .build();
        log.info("event=futures.bnb_price.config baseUrl={} timeoutMs={} cacheTtlMs={} friendlyStep=dejamos_listo_el_medidor_del_precio_de_bnb",
                safeLog(baseUrl), Math.max(50, timeoutMs), Math.max(1000L, cacheTtlMs));
    }

    @Override
    public BigDecimal getBnbPrice(FuturesCapitalAsset quoteAsset) {
        FuturesCapitalAsset safeQuote = quoteAsset == null ? FuturesCapitalAsset.defaultAsset() : quoteAsset;
        for (String symbol : symbolsFor(safeQuote)) {
            BigDecimal cached = cache.getIfPresent(symbol);
            if (cached != null && cached.compareTo(ZERO) > 0) {
                log.debug("event=futures.bnb_price.cache_hit symbol={} price={} friendlyStep=use_el_precio_guardado_para_ser_rapido", symbol, cached);
                return cached;
            }

            BigDecimal fetched = fetchPrice(symbol);
            if (fetched.compareTo(ZERO) > 0) {
                cache.put(symbol, fetched);
                cache.put(BASE_ASSET + safeQuote.name(), fetched);
                return fetched;
            }
        }
        Map<String, Object> details = new LinkedHashMap<>();
        details.put("quoteAsset", safeQuote.name());
        throw new FuturesPriceLookupException("No se pudo obtener precio BNB para " + safeQuote.name(), details);
    }

    private List<String> symbolsFor(FuturesCapitalAsset quoteAsset) {
        String preferred = BASE_ASSET + quoteAsset.name();
        if (quoteAsset == FuturesCapitalAsset.USDC) {
            return List.of(preferred, BASE_ASSET + FuturesCapitalAsset.USDT.name());
        }
        return List.of(preferred);
    }

    private BigDecimal fetchPrice(String symbol) {
        long startedNs = System.nanoTime();
        try {
            String body = restClient.get()
                    .uri(uriBuilder -> uriBuilder.path("/fapi/v1/ticker/price")
                            .queryParam("symbol", symbol)
                            .build())
                    .retrieve()
                    .body(String.class);
            if (body == null || body.isBlank()) {
                return ZERO;
            }
            JsonNode node = objectMapper.readTree(body);
            String rawPrice = node.path("price").asText(null);
            BigDecimal price = rawPrice == null ? ZERO : new BigDecimal(rawPrice);
            if (price.compareTo(ZERO) <= 0) {
                return ZERO;
            }
            log.info("event=futures.bnb_price.ok symbol={} price={} elapsedMs={} friendlyStep=ya_se_cuanto_vale_bnb_para_comparar_el_3_por_ciento",
                    symbol, price, elapsedMs(startedNs));
            return price;
        } catch (RestClientResponseException ex) {
            if (isInvalidSymbol(ex)) {
                log.info("event=futures.bnb_price.skip symbol={} httpStatus={} friendlyStep=este_par_no_existe_y_pruebo_otro_si_hay",
                        safeLog(symbol), ex.getStatusCode().value());
                return ZERO;
            }
            Map<String, Object> details = new LinkedHashMap<>();
            details.put("symbol", symbol);
            details.put("httpStatus", Integer.toString(ex.getStatusCode().value()));
            details.put("errMsg", safeLog(ex.getMessage()));
            throw new FuturesPriceLookupException("Binance no entregó precio BNB", ex, details);
        } catch (RestClientException | JsonProcessingException | IllegalStateException | IllegalArgumentException | ArithmeticException ex) {
            Map<String, Object> details = new LinkedHashMap<>();
            details.put("symbol", symbol);
            details.put("errClass", ex.getClass().getSimpleName());
            details.put("errMsg", safeLog(ex.getMessage()));
            throw new FuturesPriceLookupException("Error leyendo precio BNB", ex, details);
        }
    }

    private boolean isInvalidSymbol(RestClientResponseException ex) {
        int status = ex.getStatusCode().value();
        if (status == 404) {
            return true;
        }
        return status == 400 && ex.getResponseBodyAsString() != null && ex.getResponseBodyAsString().contains("-1121");
    }

    private long elapsedMs(long startedNs) {
        return Duration.ofNanos(System.nanoTime() - startedNs).toMillis();
    }

    private String safeLog(String value) {
        if (value == null) {
            return "";
        }
        String clean = value.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ').replace('"', '\'');
        return clean.length() > 1000 ? clean.substring(0, 1000) : clean;
    }
}
