package com.apunto.engine.service.copy.coverage;

import java.time.OffsetDateTime;
import java.util.List;

@FunctionalInterface
public interface ShadowCoverageQueryService {

    ShadowCoverageBatch load(List<Long> shadowAllocationIds, OffsetDateTime windowEnd);
}
