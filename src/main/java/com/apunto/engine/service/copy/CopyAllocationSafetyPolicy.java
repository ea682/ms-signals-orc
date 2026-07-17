package com.apunto.engine.service.copy;

import com.apunto.engine.service.metric.MetricWalletReadModeResolver;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Objects;

@Component
@Slf4j
public class CopyAllocationSafetyPolicy {

    private final MetricWalletReadModeResolver readModeResolver;
    private final boolean configuredFilterByWalletAllocation;
    private final boolean configuredFallbackAllUsers;

    public CopyAllocationSafetyPolicy(
            MetricWalletReadModeResolver readModeResolver,
            @Value("${operation.job.ingest.filter-by-wallet-allocation:${copy.job.ingest.filter-by-wallet-allocation:true}}")
            boolean configuredFilterByWalletAllocation,
            @Value("${operation.job.ingest.fallback-all-users-on-empty-allocation:${copy.job.ingest.fallback-all-users-on-empty-allocation:false}}")
            boolean configuredFallbackAllUsers
    ) {
        this.readModeResolver = Objects.requireNonNull(readModeResolver, "readModeResolver");
        this.configuredFilterByWalletAllocation = configuredFilterByWalletAllocation;
        this.configuredFallbackAllUsers = configuredFallbackAllUsers;
    }

    @PostConstruct
    void logEffectivePolicy() {
        log.info("event=copy_allocation_safety.config metricReadMode={} configuredFilterByWalletAllocation={} configuredFallbackAllUsers={} effectiveRequiresWalletAllocation={} effectiveAllowsAllUsersFallback={}",
                readModeResolver.effectiveMode(),
                configuredFilterByWalletAllocation,
                configuredFallbackAllUsers,
                requiresWalletAllocation(),
                allowsAllUsersFallback());
    }

    public boolean requiresWalletAllocation() {
        return readModeResolver.readsV2() || configuredFilterByWalletAllocation;
    }

    public boolean allowsAllUsersFallback() {
        return !readModeResolver.readsV2() && configuredFallbackAllUsers;
    }
}
