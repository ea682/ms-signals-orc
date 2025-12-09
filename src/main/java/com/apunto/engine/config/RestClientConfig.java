package com.apunto.engine.config;

import com.apunto.engine.client.BinanceClient;
import com.apunto.engine.client.MetricWalletsInfoClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestClient;
import org.springframework.web.service.invoker.HttpServiceProxyFactory;
import org.springframework.web.client.support.RestClientAdapter;

@Configuration
public class RestClientConfig {

    @Bean
    public RestClient metricWalletRestClient(
            RestClient.Builder builder,
            @Value("${rest-client.metric-wallet.info-base}") String baseUrl
    ) {
        return builder
                .baseUrl(baseUrl)
                .build();
    }

    @Bean
    public RestClient binanceRestClient(
            RestClient.Builder builder,
            @Value("${rest-client.binance-service.info-base}") String baseUrl
    ) {
        return builder
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
