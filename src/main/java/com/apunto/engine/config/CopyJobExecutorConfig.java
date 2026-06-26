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
            @Value("${operation.job.worker.pool-size:${copy.job.worker.pool-size:16}}") int threads,
            @Value("${operation.job.worker.queue:${copy.job.worker.queue:5000}}") int queueCapacity
    ) {
        return buildExecutor(threads, queueCapacity, "copy-job-");
    }

    @Bean(name = "copyPriorityJobExecutor")
    public ThreadPoolTaskExecutor copyPriorityJobExecutor(
            @Value("${operation.job.priority-worker.pool-size:${copy.job.priority-worker.pool-size:16}}") int threads,
            @Value("${operation.job.priority-worker.queue:${copy.job.priority-worker.queue:20000}}") int queueCapacity
    ) {
        return buildExecutor(threads, queueCapacity, "copy-priority-");
    }

    private ThreadPoolTaskExecutor buildExecutor(int threads, int queueCapacity, String threadNamePrefix) {
        ThreadPoolTaskExecutor exec = new ThreadPoolTaskExecutor();
        exec.setCorePoolSize(threads);
        exec.setMaxPoolSize(threads);
        exec.setQueueCapacity(queueCapacity);
        exec.setThreadNamePrefix(threadNamePrefix);


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
        exec.getThreadPoolExecutor().prestartAllCoreThreads();
        return exec;
    }
}
