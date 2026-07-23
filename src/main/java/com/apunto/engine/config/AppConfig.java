package com.apunto.engine.config;

import com.apunto.engine.hyperliquid.config.HyperliquidDirectIngestProperties;
import com.apunto.engine.service.copy.coverage.ShadowCoverageWindowProperties;
import com.apunto.engine.service.copy.certification.LiveCertificationRuntimeProperties;
import com.apunto.engine.service.copy.promotion.LivePromotionProperties;
import com.apunto.engine.service.copy.promotion.ShadowPromotionProperties;
import com.apunto.engine.service.copy.readiness.ShadowLiveReadinessProperties;
import com.apunto.engine.service.copy.dispatch.B2bRealMoneyGuardProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties({
        KafkaOperacionProperties.class,
        HyperliquidDirectIngestProperties.class,
        ShadowLiveReadinessProperties.class,
        ShadowPromotionProperties.class,
        ShadowCoverageWindowProperties.class,
        LivePromotionProperties.class,
        LiveCertificationRuntimeProperties.class,
        B2bRealMoneyGuardProperties.class
})
public class AppConfig { }
