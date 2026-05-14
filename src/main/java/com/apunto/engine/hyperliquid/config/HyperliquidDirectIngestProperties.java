package com.apunto.engine.hyperliquid.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "hyperliquid.direct-ingest")
public class HyperliquidDirectIngestProperties {

    private boolean enabled = true;
    private int queueCapacity = 20000;
    private int workerThreads = 4;
    private boolean rejectWhenDisabled = true;
    private boolean dedupeEnabled = false;
    private long dedupeTtlSeconds = 30;
    private long logIntervalMs = 10000;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public int getQueueCapacity() {
        return queueCapacity;
    }

    public void setQueueCapacity(int queueCapacity) {
        this.queueCapacity = queueCapacity;
    }

    public int getWorkerThreads() {
        return workerThreads;
    }

    public void setWorkerThreads(int workerThreads) {
        this.workerThreads = workerThreads;
    }

    public boolean isRejectWhenDisabled() {
        return rejectWhenDisabled;
    }

    public void setRejectWhenDisabled(boolean rejectWhenDisabled) {
        this.rejectWhenDisabled = rejectWhenDisabled;
    }

    public boolean isDedupeEnabled() {
        return dedupeEnabled;
    }

    public void setDedupeEnabled(boolean dedupeEnabled) {
        this.dedupeEnabled = dedupeEnabled;
    }

    public long getDedupeTtlSeconds() {
        return dedupeTtlSeconds;
    }

    public void setDedupeTtlSeconds(long dedupeTtlSeconds) {
        this.dedupeTtlSeconds = dedupeTtlSeconds;
    }

    public long getLogIntervalMs() {
        return logIntervalMs;
    }

    public void setLogIntervalMs(long logIntervalMs) {
        this.logIntervalMs = logIntervalMs;
    }
}
