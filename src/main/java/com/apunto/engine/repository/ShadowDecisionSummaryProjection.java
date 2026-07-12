package com.apunto.engine.repository;

public interface ShadowDecisionSummaryProjection {

    Long getSimulatedEvents();

    Long getRecordedEvents();

    Long getSkippedEvents();

    Long getDuplicateEvents();

    Long getErrorEvents();
}
