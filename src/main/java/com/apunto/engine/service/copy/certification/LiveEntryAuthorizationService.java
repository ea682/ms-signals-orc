package com.apunto.engine.service.copy.certification;

import java.time.OffsetDateTime;
import java.util.List;

public class LiveEntryAuthorizationService {

    private final LiveCertificationReadStore readStore;

    public LiveEntryAuthorizationService(LiveCertificationReadStore readStore) {
        this.readStore = readStore;
    }

    public LiveEntryAuthorizationDecision evaluate(LiveEntryAuthorizationRequest request, OffsetDateTime now) {
        if (request == null || now == null) {
            return LiveEntryAuthorizationDecision.blocked("LIVE_CERTIFICATION_RUNTIME_CONTEXT_INCOMPLETE");
        }
        List<LiveCertificationAuthorizationRecord> candidates = readStore.findCandidates(request);
        if (candidates == null || candidates.isEmpty()) {
            return LiveEntryAuthorizationDecision.blocked("LIVE_CERTIFICATION_MISSING");
        }
        List<LiveCertificationAuthorizationRecord> exact = candidates.stream()
                .filter(record -> record != null && record.identity() != null && record.identity().matches(request))
                .toList();
        if (exact.isEmpty()) {
            return LiveEntryAuthorizationDecision.blocked("LIVE_CERTIFICATION_IDENTITY_MISMATCH");
        }
        if (exact.size() > 1) {
            return LiveEntryAuthorizationDecision.blocked("LIVE_CERTIFICATION_AMBIGUOUS");
        }
        LiveCertificationAuthorizationRecord record = exact.getFirst();
        if (record.certificationStatus() != LiveCertificationStatus.LIVE_APPROVED) {
            return LiveEntryAuthorizationDecision.blocked("LIVE_CERTIFICATION_NOT_APPROVED");
        }
        if (record.adoptionId() == null) {
            return LiveEntryAuthorizationDecision.blocked("LIVE_ADOPTION_MISSING");
        }
        if (!"VALID".equalsIgnoreCase(record.adoptionStatus()) || !record.everyAdoptionCheckValid()) {
            return LiveEntryAuthorizationDecision.blocked("LIVE_ADOPTION_REJECTED");
        }
        if (record.validatedAt() == null || record.validatedAt().isAfter(now)) {
            return LiveEntryAuthorizationDecision.blocked("LIVE_ADOPTION_OBSERVED_AT_IN_FUTURE");
        }
        if (record.expiresAt() == null || !record.expiresAt().isAfter(now)) {
            return LiveEntryAuthorizationDecision.blocked("LIVE_ADOPTION_EXPIRED");
        }
        return LiveEntryAuthorizationDecision.allowed(
                "LIVE_CERTIFICATION_AND_ADOPTION_VALID", record.certificationId());
    }
}
