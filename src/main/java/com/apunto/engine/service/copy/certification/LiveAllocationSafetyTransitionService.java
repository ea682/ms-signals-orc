package com.apunto.engine.service.copy.certification;

import com.apunto.engine.repository.UserCopyAllocationRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class LiveAllocationSafetyTransitionService {

    private final UserCopyAllocationRepository repository;

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public int markExitOnly(UUID certificationId, String reasonCode, OffsetDateTime now) {
        return repository.markCertificationAllocationsExitOnly(certificationId, reasonCode, now);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public int markPendingRevalidation(UUID certificationId, String reasonCode, OffsetDateTime now) {
        return repository.markCertificationAllocationsPendingRevalidation(certificationId, reasonCode, now);
    }
}
