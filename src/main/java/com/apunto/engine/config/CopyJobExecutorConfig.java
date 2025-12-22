package com.apunto.engine.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;

@Configuration
public class CopyJobExecutorConfig {

    @Bean(name = "copyJobExecutor")
    public Executor copyJobExecutor(
            @Value("${copy.job.worker.pool-size:8}") int threads,
            @Value("${copy.job.worker.queue:1000}") int queueCapacity
    ) {
        ThreadPoolTaskExecutor exec = new ThreadPoolTaskExecutor();
        exec.setCorePoolSize(threads);
        exec.setMaxPoolSize(threads);
        exec.setQueueCapacity(queueCapacity);
        exec.setThreadNamePrefix("copy-job-");
        exec.initialize();
        return exec;
    }
}
