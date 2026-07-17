package com.apunto.engine.service.copy.certification;

import java.util.List;

@FunctionalInterface
public interface LiveCertificationReadStore {
    List<LiveCertificationAuthorizationRecord> findCandidates(LiveEntryAuthorizationRequest request);
}
