package com.apunto.engine.service.copy.allocation;

@FunctionalInterface
public interface LiveAllocationPercentageResolver {

    LiveAllocationPercentageResolution resolve(LiveAllocationPercentageRequest request);
}
