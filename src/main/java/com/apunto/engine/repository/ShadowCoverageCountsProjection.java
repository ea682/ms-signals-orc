package com.apunto.engine.repository;

import java.time.Instant;

public interface ShadowCoverageCountsProjection {

    Long getShadowAllocationId();

    Long getSimulatedEvents();

    Long getRecordedEvents();

    Long getSkippedEvents();

    Long getErrorEvents();

    Instant getOldestEventTime();

    Instant getNewestEventTime();
}
