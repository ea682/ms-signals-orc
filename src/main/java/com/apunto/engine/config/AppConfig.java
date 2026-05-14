package com.apunto.engine.config;

import com.apunto.engine.hyperliquid.config.HyperliquidDirectIngestProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties({KafkaOperacionProperties.class, HyperliquidDirectIngestProperties.class})
public class AppConfig { }
