package com.apunto.engine.config;

import com.apunto.engine.client.BinanceClient;
import com.apunto.engine.client.MetricWalletsInfoClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.support.RestClientAdapter;
import org.springframework.web.service.invoker.HttpServiceProxyFactory;

import java.net.http.HttpClient;
import java.time.Duration;

@Configuration
@Slf4j
public class RestClientConfig {

    @Bean
    public ClientHttpRequestFactory clientHttpRequestFactory(
            @Value("${rest-client.timeout.connect-ms:2000}") int connectMs,
            @Value("${rest-client.timeout.read-ms:5000}") int readMs
    ) {
        log.info("RestClient timeouts connectMs={} readMs={}", connectMs, readMs);

        HttpClient httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(connectMs))
                .build();

        JdkClientHttpRequestFactory factory = new JdkClientHttpRequestFactory(httpClient);
        factory.setReadTimeout(Duration.ofMillis(readMs));
        return factory;
    }

    @Bean
    public RestClient metricWalletRestClient(
            RestClient.Builder builder,
            ClientHttpRequestFactory requestFactory,
            @Value("${rest-client.metric-wallet.info-base}") String baseUrl
    ) {
        return builder
                .requestFactory(requestFactory)
                .baseUrl(baseUrl)
                .build();
    }

    @Bean
    public RestClient binanceRestClient(
            RestClient.Builder builder,
            ClientHttpRequestFactory requestFactory,
            @Value("${rest-client.binance-service.info-base}") String baseUrl
    ) {
        return builder
                .requestFactory(requestFactory)
                .baseUrl(baseUrl)
                .build();
    }

    @Bean
    public MetricWalletsInfoClient metricWalletsInfoClient(RestClient metricWalletRestClient) {
        HttpServiceProxyFactory factory = HttpServiceProxyFactory
                .builderFor(RestClientAdapter.create(metricWalletRestClient))
                .build();

        return factory.createClient(MetricWalletsInfoClient.class);
    }

    @Bean
    public BinanceClient binanceInfoClient(RestClient binanceRestClient) {
        HttpServiceProxyFactory factory = HttpServiceProxyFactory
                .builderFor(RestClientAdapter.create(binanceRestClient))
                .build();

        return factory.createClient(BinanceClient.class);
    }
}
