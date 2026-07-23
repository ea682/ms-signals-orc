package com.apunto.engine.service.copy.certification;

import java.util.UUID;

@FunctionalInterface
public interface LiveCertificationStatePropagation {
    void propagate(UUID certificationId, LiveCertificationStatus nextStatus, String reasonCode);
}

