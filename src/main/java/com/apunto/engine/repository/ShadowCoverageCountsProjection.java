package com.apunto.engine.repository;

import java.time.OffsetDateTime;

public interface ShadowCoverageCountsProjection {

    Long getShadowAllocationId();

    Long getSimulatedEvents();

    Long getRecordedEvents();

    Long getSkippedEvents();

    Long getErrorEvents();

    OffsetDateTime getOldestEventTime();

    OffsetDateTime getNewestEventTime();
}
