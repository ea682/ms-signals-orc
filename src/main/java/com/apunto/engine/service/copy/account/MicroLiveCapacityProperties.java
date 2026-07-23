package com.apunto.engine.service.copy.account;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

@Data
@Component
@ConfigurationProperties(prefix = "copy.micro-live.capacity")
public class MicroLiveCapacityProperties {

    private BigDecimal budgetPerAllocationUsdc = new BigDecimal("100");
    private BigDecimal safetyBufferUsdc = BigDecimal.ZERO;
    /** Zero means that equity is the only capacity ceiling. */
    private int maxConcurrentAllocations = 0;
    private int reservedRecertificationSlots = 0;
    private MicroLivePreemptionPolicy preemptionPolicy = MicroLivePreemptionPolicy.IDLE_ONLY;
    private long snapshotTtlSeconds = 300;
}
