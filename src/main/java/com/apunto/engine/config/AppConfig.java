package com.apunto.engine.config;

import com.apunto.engine.hyperliquid.config.HyperliquidDirectIngestProperties;
import com.apunto.engine.service.copy.readiness.ShadowLiveReadinessProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties({
        KafkaOperacionProperties.class,
        HyperliquidDirectIngestProperties.class,
        ShadowLiveReadinessProperties.class
})
public class AppConfig { }
