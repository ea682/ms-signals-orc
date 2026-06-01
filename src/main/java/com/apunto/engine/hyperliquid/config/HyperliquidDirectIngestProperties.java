package com.apunto.engine.hyperliquid.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "hyperliquid.direct-ingest")
public class HyperliquidDirectIngestProperties {

    private boolean enabled = true;
    private int queueCapacity = 20000;
    private int workerThreads = 4;
    private boolean rejectWhenDisabled = true;
    private boolean dedupeEnabled = true;
    private long dedupeTtlSeconds = 30;
    /**
     * DB-backed idempotency guard used when running more than one signals replica.
     * This prevents duplicate copy orders before dispatch, not only after ledger persistence.
     */
    private boolean distributedDedupeEnabled = true;
    private long dedupeLeaseTtlMs = 60000;
    private boolean failOpenOnDedupeError = false;
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

    public boolean isDistributedDedupeEnabled() {
        return distributedDedupeEnabled;
    }

    public void setDistributedDedupeEnabled(boolean distributedDedupeEnabled) {
        this.distributedDedupeEnabled = distributedDedupeEnabled;
    }

    public long getDedupeLeaseTtlMs() {
        return dedupeLeaseTtlMs;
    }

    public void setDedupeLeaseTtlMs(long dedupeLeaseTtlMs) {
        this.dedupeLeaseTtlMs = dedupeLeaseTtlMs;
    }

    public boolean isFailOpenOnDedupeError() {
        return failOpenOnDedupeError;
    }

    public void setFailOpenOnDedupeError(boolean failOpenOnDedupeError) {
        this.failOpenOnDedupeError = failOpenOnDedupeError;
    }

    public long getLogIntervalMs() {
        return logIntervalMs;
    }

    public void setLogIntervalMs(long logIntervalMs) {
        this.logIntervalMs = logIntervalMs;
    }
}
