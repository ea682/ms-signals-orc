package com.apunto.engine.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.slf4j.MDC;

import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

@Configuration
public class CopyJobExecutorConfig {

    @Bean(name = "copyJobExecutor")
    public ThreadPoolTaskExecutor copyJobExecutor(
            @Value("${copy.job.worker.pool-size:8}") int threads,
            @Value("${copy.job.worker.queue:1000}") int queueCapacity
    ) {
        ThreadPoolTaskExecutor exec = new ThreadPoolTaskExecutor();
        exec.setCorePoolSize(threads);
        exec.setMaxPoolSize(threads);
        exec.setQueueCapacity(queueCapacity);
        exec.setThreadNamePrefix("copy-job-");


        exec.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());

        exec.setTaskDecorator(runnable -> {
            Map<String, String> context = MDC.getCopyOfContextMap();
            return () -> {
                if (context != null) MDC.setContextMap(context);
                try {
                    runnable.run();
                } finally {
                    MDC.clear();
                }
            };
        });

        exec.setWaitForTasksToCompleteOnShutdown(true);
        exec.setAwaitTerminationSeconds(30);

        exec.initialize();
        return exec;
    }
}

