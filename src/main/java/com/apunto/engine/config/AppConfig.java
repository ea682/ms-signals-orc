package com.apunto.engine.config;

import com.apunto.engine.hyperliquid.config.HyperliquidDirectIngestProperties;
import com.apunto.engine.service.copy.coverage.ShadowCoverageWindowProperties;
import com.apunto.engine.service.copy.promotion.LivePromotionProperties;
import com.apunto.engine.service.copy.promotion.ShadowPromotionProperties;
import com.apunto.engine.service.copy.readiness.ShadowLiveReadinessProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties({
        KafkaOperacionProperties.class,
        HyperliquidDirectIngestProperties.class,
        ShadowLiveReadinessProperties.class,
        ShadowPromotionProperties.class,
        ShadowCoverageWindowProperties.class,
        LivePromotionProperties.class
})
public class AppConfig { }
