package com.apunto.engine.service.copy.certification;

import java.util.UUID;

public record LiveCertificationSnapshot(UUID id, LiveCertificationStatus status, long version) {
}
