package com.apunto.engine.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

@Configuration
public class BinanceSchedulerConfig {

    @Bean
    public ThreadPoolTaskScheduler binanceTaskScheduler(
            @Value("${binance.dispatch.pool-size:4}") int poolSize) {

        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(poolSize);
        scheduler.setThreadNamePrefix("binance-dispatch-");
        scheduler.initialize();
        return scheduler;
    }
}
